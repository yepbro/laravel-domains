// Юнит-тесты сверки контента (T-056/T-147): ранний Put воркера кладёт
// каждую скачанную страницу немедленно (тела в памяти не удерживаются),
// ContentSaver.Save распределяет ссылки ранних Put — оригиналы наследуют
// свою ссылку, алиасы теряют content_hash, свёрнутые варианты и лишние
// скачивания отпускают ссылку Delete; ErrStoreFull/ErrTooLarge при
// раннем Put — результат живой с nil-хэшем и признаком «контент не
// сохранён» (Warn воркера), обрезанное тело не сохраняется вовсе;
// сбой раннего Put, отмена ctx и ошибки Delete роняют задачу/Save
// (повторная доставка); пустое тело (T-138) — валидный контент с живым
// хэшем, в том числе с кэша итоговых URL. Идентификаторы с префиксом
// store* против коллизий с соседними файлами пакета.

package httpworker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"crawler/internal/content"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- фейковое хранилище --------------------------------------------------------

// storeFake — фейковый content.ContentStore с живой семантикой T-041/T-042:
// те же байты — Created=false и refcount+1 без записи новых блоков; отказ
// newErr действует только на НОВЫЕ блоки (как в PackStore.Put: дедуп-UPDATE
// идёт раньше порога диска, T-046 — ссылки на живой контент продолжают
// работать при переполнении). badHash — нарушение контракта HashOf (T-041)
// для проверки защиты воркера; delErr — сбой Delete (сверка T-147);
// delMissing — Delete несуществующей ссылки (ErrNotFound).
type storeFake struct {
	mu         sync.Mutex
	blocks     map[content.Hash][]byte
	refs       map[content.Hash]int
	puts       int // все вызовы Put
	writes     int // вызовы, записавшие новые байты
	deletes    int // все вызовы Delete
	newErr     func(data []byte) error
	badHash    bool
	delErr     error
	delMissing bool
}

func newStoreFake() *storeFake {
	return &storeFake{blocks: make(map[content.Hash][]byte), refs: make(map[content.Hash]int)}
}

func (f *storeFake) Put(ctx context.Context, data []byte) (content.PutResult, error) {
	if err := ctx.Err(); err != nil {
		return content.PutResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	h := content.HashOf(data)
	if f.refs[h] > 0 {
		f.refs[h]++
		return content.PutResult{Hash: h}, nil
	}
	if f.newErr != nil {
		if err := f.newErr(data); err != nil {
			return content.PutResult{}, err
		}
	}
	f.blocks[h] = append([]byte(nil), data...)
	f.refs[h] = 1
	f.writes++
	if f.badHash {
		return content.PutResult{Hash: content.HashOf([]byte("чужой хэш"))}, nil
	}
	return content.PutResult{Hash: h, Created: true}, nil
}

func (f *storeFake) Get(_ context.Context, h content.Hash) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refs[h] <= 0 {
		return nil, content.ErrNotFound
	}
	return append([]byte(nil), f.blocks[h]...), nil
}

func (f *storeFake) Delete(ctx context.Context, h content.Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes++
	if f.delErr != nil {
		return f.delErr
	}
	if f.delMissing || f.refs[h] <= 0 {
		return content.ErrNotFound
	}
	f.refs[h]--
	return nil
}

func (f *storeFake) snapshot() (puts, writes, deletes int, refs map[content.Hash]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(map[content.Hash]int, len(f.refs))
	for h, n := range f.refs {
		cp[h] = n
	}
	return f.puts, f.writes, f.deletes, cp
}

// storeSaver — сверка над хранилищем с перехватом лога.
func storeSaver(t *testing.T, st content.ContentStore, logBuf *bytes.Buffer) *ContentSaver {
	t.Helper()
	s, err := NewContentSaver(st, slog.New(slog.NewTextHandler(logBuf, nil)))
	if err != nil {
		t.Fatalf("NewContentSaver: %v", err)
	}
	return s
}

// storeWorker — воркер над сценарием загрузки (см. workerFakeFetch) и
// хранилищем ранних Put (T-147); PerDomain=1 — детерминированный порядок
// проверок (кэш-попадания стабильны, как в worker-тестах), logBuf
// перехватывает Warn воркера.
func storeWorker(t *testing.T, script map[string]*workerPageScript, st content.ContentStore, logBuf *bytes.Buffer) *Worker {
	t.Helper()
	fetch, _ := workerFakeFetch(script)
	return workerWorker(t, fetch, func(o *Options) {
		o.Store = st
		o.PerDomain = 1
		if logBuf != nil {
			o.Log = slog.New(slog.NewTextHandler(logBuf, nil))
		}
	})
}

// storePageBody — различимые тела страниц для сценариев.
func storePageBody(label string) string {
	return "<html><body>" + strings.Repeat("текст страницы "+label+" для сохранения в хранилище контента ", 24) + "</body></html>"
}

// storeRun — задача одного домена с субдометками «метка» по порядку id 0..N.
func storeRun(t *testing.T, w *Worker, name string, labels ...string) *DomainResult {
	t.Helper()
	subs := make([]queue.HTTPSubdomain, len(labels))
	for i, l := range labels {
		subs[i] = workerSubMsg(int16(i), l)
	}
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 1, Name: name, Subdomains: subs,
	})
	if err != nil {
		t.Fatalf("ProcessDomain(%s): %v", name, err)
	}
	return res
}

// --- распределение ссылок ранних Put (ТЗ 5.5 п. 6 «контент хранится один раз») --

// TestStoreOriginalsClaimEarlyPuts — сценарий TestWorkerProcessDomain: обе
// проверки bare скачивают страницу home (два скачивания одного контента —
// разные стартовые URL), www-https берёт её из кэша итоговых URL; класс
// bare+www хранится ОДНИМ блоком: оригинал www v0 наследует свою ссылку
// раннего Put, лишняя ссылка второго скачивания снимается Delete;
// независимые записи (вариант 1 www, api) владеют своими. Дедупликация
// алиасов идёт по хэшам — тела записям не нужны (T-147).
func TestStoreOriginalsClaimEarlyPuts(t *testing.T) {
	home, old := storePageBody("home"), storePageBody("old")
	api := storePageBody("api")
	script := map[string]*workerPageScript{
		"example.com": { // обе проверки bare: редирект на www
			status: 301, body: home, final: "https://www.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "https://example.com/", Status: 301}},
		},
		"http://www.example.com/": {
			status: 200, body: old, final: "http://old.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "http://www.example.com/", Status: 301}},
		},
		"https://api.example.com/": {err: workerTimeoutErr{}},
		"api.example.com":          {status: 200, body: api},
	}
	st := newStoreFake()
	// PerDomain=1: детерминированный порядок проверок (как в worker-тестах).
	w := storeWorker(t, script, st, nil)
	res := storeRun(t, w, "example.com", "", "www", "api")
	bare, www, apiSub := &res.Subdomains[0], &res.Subdomains[1], &res.Subdomains[2]
	// До Save хэш алиаса ещё на месте (материал вариант-логики T-053).
	if bare.Records[0].ContentHash == nil {
		t.Fatal("до Save хэш алиаса отсутствует (сломан сценарий)")
	}
	if err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Ранние Put (T-147): home скачана дважды (https и http проверки bare),
	// old и api — по разу; новых блоков три.
	puts, writes, deletes, refs := st.snapshot()
	if puts != 4 || writes != 3 {
		t.Fatalf("Put: вызовов %d, новых блоков %d — ожидалось 4 и 3 (home скачана дважды)", puts, writes)
	}
	// Сверка сняла лишнюю ссылку home: по одной ссылке на запись.
	homeHash := content.HashOf([]byte(home))
	if refs[homeHash] != 1 {
		t.Errorf("refcount home: %d, ожидалась 1 ссылка (лишнее скачивание снято Delete)", refs[homeHash])
	}
	if refs[content.HashOf([]byte(old))] != 1 || refs[content.HashOf([]byte(api))] != 1 {
		t.Error("независимые записи не владеют своими ссылками")
	}
	if deletes != 1 {
		t.Errorf("Delete: вызовов %d, ожидался 1 (второе скачивание home)", deletes)
	}

	// Алиас: content_hash снят (NULL в БД), признак «не сохранён» НЕ
	// ставится — контент класса хранится записью-оригиналом.
	bareRec := &bare.Records[0]
	if bareRec.ContentHash != nil || bareRec.ContentNotStored {
		t.Errorf("алиас: hash=%v not_stored=%v (ожидалось nil/false — контент у оригинала)",
			bareRec.ContentHash, bareRec.ContentNotStored)
	}
	// Оригиналы: хэш = HashOf(тела), контент доступен из хранилища.
	for _, c := range []struct {
		name string
		rec  *Record
		body string
	}{
		{"www v0", &www.Records[0], home},
		{"www v1", &www.Records[1], old},
		{"api", &apiSub.Records[0], api},
	} {
		if c.rec.ContentHash == nil || c.rec.ContentNotStored {
			t.Fatalf("%s: hash=%v not_stored=%v", c.name, c.rec.ContentHash, c.rec.ContentNotStored)
		}
		if *c.rec.ContentHash != content.HashOf([]byte(c.body)) {
			t.Errorf("%s: хэш записи ≠ HashOf(тела)", c.name)
		}
		got, err := st.Get(context.Background(), *c.rec.ContentHash)
		if err != nil || string(got) != c.body {
			t.Errorf("%s: Get по хэшу записи: %v (байты совпадают: %v)", c.name, err, string(got) == c.body)
		}
	}
}

// TestStoreDedupAcrossDomains — уровень хранилища (§8): одна и та же
// парковочная страница двух доменов — второй ранний Put дедуплицируется
// (Created=false, новых байтов нет), каждый домен после свертки владеет
// одной ссылкой; обе записи держат живой хэш. Уровень задачи (один блок
// на класс) — TestStoreOriginalsClaimEarlyPuts.
func TestStoreDedupAcrossDomains(t *testing.T) {
	parking := storePageBody("парковка")
	script := func(host string) map[string]*workerPageScript {
		return map[string]*workerPageScript{
			host:                   {status: 200, body: parking},
			"http://" + host + "/": {status: 200, body: parking},
		}
	}
	st := newStoreFake()
	w1 := storeWorker(t, script("park1.test"), st, nil)
	w2 := storeWorker(t, script("park2.test"), st, nil)

	res1 := storeRun(t, w1, "park1.test", "")
	res2 := storeRun(t, w2, "park2.test", "")
	saver := storeSaver(t, st, &bytes.Buffer{})
	for _, res := range []*DomainResult{res1, res2} {
		if err := saver.Save(context.Background(), res); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	// Каждый домен скачивает парковку дважды (https и http — один сайт,
	// записи сворачиваются в одну): 4 ранних Put, 1 новый блок, сверка
	// сняла по одной лишней ссылке — осталось по ссылке на домен.
	puts, writes, deletes, refs := st.snapshot()
	h := content.HashOf([]byte(parking))
	if puts != 4 || writes != 1 {
		t.Fatalf("Put: вызовов %d, новых блоков %d — ожидалось 4 и 1 (дедуп между доменами)", puts, writes)
	}
	if deletes != 2 {
		t.Fatalf("Delete: вызовов %d, ожидалось 2 (лишние скачивания своих протоколов)", deletes)
	}
	if refs[h] != 2 {
		t.Errorf("refcount парковки: %d, ожидался 2 (по ссылке на домен)", refs[h])
	}
	if res1.Subdomains[0].Records[0].ContentHash == nil || *res1.Subdomains[0].Records[0].ContentHash != h {
		t.Error("первая запись не держит хэш парковки")
	}
	if res2.Subdomains[0].Records[0].ContentHash == nil || *res2.Subdomains[0].Records[0].ContentHash != h {
		t.Error("вторая запись не держит хэш парковки")
	}
}

// --- переполнение и лимиты (ТЗ 8, T-046) ---------------------------------------

// TestStoreFullResultAlive — ErrStoreFull при раннем Put: контент не
// сохраняется, но результат домена живёт (Save = nil): запись с
// content_hash = nil и признаком ContentNotStored, Warn в логе воркера.
// Дедуп-ссылка на УЖЕ живой контент проходит и при переполнении
// (PackStore делает UPDATE раньше порога диска, T-046): запись с
// существующим хэшем сохраняется обычным путём.
func TestStoreFullResultAlive(t *testing.T) {
	warm, fresh := storePageBody("тёплая"), storePageBody("свежая")
	page := func(host, body string) map[string]*workerPageScript {
		return map[string]*workerPageScript{
			host:                   {status: 200, body: body},
			"http://" + host + "/": {status: 200, body: body},
		}
	}
	st := newStoreFake()
	full := false
	// Обёрнутый сентинел — реальные ошибки Put завёрнуты в контекст,
	// воркер различает ветки errors.Is, а не текстом (T-041).
	st.newErr = func([]byte) error {
		if full {
			return errors.Join(content.ErrStoreFull, errors.New("свободно 0 байт"))
		}
		return nil
	}
	saver := storeSaver(t, st, &bytes.Buffer{})

	// 1) до переполнения контент warm записан ранним Put.
	resWarm := storeRun(t, storeWorker(t, page("warm.test", warm), st, nil), "warm.test", "")
	if err := saver.Save(context.Background(), resWarm); err != nil {
		t.Fatalf("Save(warm): %v", err)
	}

	// 2) диск полон: тот же контент другого домена — дедуп-ссылка
	// раннего Put проходит (без новых байтов), хэш записи жив.
	full = true
	resWarm2 := storeRun(t, storeWorker(t, page("warm2.test", warm), st, nil), "warm2.test", "")
	if err := saver.Save(context.Background(), resWarm2); err != nil {
		t.Fatalf("Save(warm2): %v (дедуп-ссылка обязана проходить при полном диске)", err)
	}
	rec := &resWarm2.Subdomains[0].Records[0]
	if rec.ContentHash == nil || rec.ContentNotStored {
		t.Errorf("дедуп-ссылка при полном диске: hash=%v not_stored=%v", rec.ContentHash, rec.ContentNotStored)
	}

	// 3) новый контент при полном диске — пропуск раннего Put: Warn
	// воркера, результат жив, записи без контента.
	var wlog bytes.Buffer
	resFresh := storeRun(t, storeWorker(t, page("fresh.test", fresh), st, &wlog), "fresh.test", "")
	flog := &bytes.Buffer{}
	if err := storeSaver(t, st, flog).Save(context.Background(), resFresh); err != nil {
		t.Fatalf("Save(fresh): %v — переполнение не роняет результат (ТЗ 8)", err)
	}
	rec = &resFresh.Subdomains[0].Records[0]
	if rec.ContentHash != nil || !rec.ContentNotStored {
		t.Errorf("запись при полном диске: hash=%v not_stored=%v (ожидался nil/true)",
			rec.ContentHash, rec.ContentNotStored)
	}
	if _, _, _, refs := st.snapshot(); refs[content.HashOf([]byte(fresh))] != 0 {
		t.Error("контент fresh попал в хранилище при переполнении")
	}
	if msg := wlog.String(); !strings.Contains(msg, "не сохранён") || !strings.Contains(msg, "переполнено") {
		t.Errorf("Warn воркера о переполнении не записан: %q", msg)
	}
	if msg := flog.String(); !strings.Contains(msg, "не сохранён") {
		t.Errorf("Warn сверки о записях без контента не записан: %q", msg)
	}
}

// TestStoreTooLarge — ErrTooLarge при раннем Put (тело сверх
// max_store_bytes, ТЗ 5.5): контент отвергается хранилищем целиком —
// запись жива, content_hash = nil, ContentNotStored, Warn воркера;
// Save не падает.
func TestStoreTooLarge(t *testing.T) {
	small := storePageBody("малая")
	big := storePageBody("большая страница")
	script := map[string]*workerPageScript{
		"big.test":         {status: 200, body: big},
		"http://big.test/": {status: 200, body: big},
	}
	st := newStoreFake()
	st.newErr = func(data []byte) error {
		if len(data) > len(small) {
			return errors.Join(content.ErrTooLarge, errors.New("300 байт при лимите 100"))
		}
		return nil
	}
	// Обе проверки big.test сходятся на одном URL → 1 запись https; для
	// сверхлимитного тела хватит одной записи. PerDomain=1 — порядок
	// проверок детерминирован.
	var wlog bytes.Buffer
	w := workerWorker(t, mustFetch(script), func(o *Options) {
		o.Store = st
		o.PerDomain = 1
		o.Log = slog.New(slog.NewTextHandler(&wlog, nil))
	})
	res := storeRun(t, w, "big.test", "")

	if err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v — сверхлимитный контент не роняет результат", err)
	}
	rec := &res.Subdomains[0].Records[0]
	if rec.ContentHash != nil || !rec.ContentNotStored {
		t.Errorf("запись: hash=%v not_stored=%v (ожидался nil/true)", rec.ContentHash, rec.ContentNotStored)
	}
	if _, writes, _, _ := st.snapshot(); writes != 0 {
		t.Errorf("сверхлимитный контент записан: %d блоков", writes)
	}
	if msg := wlog.String(); !strings.Contains(msg, "не сохранён") {
		t.Errorf("Warn воркера о лимите не записан: %q", msg)
	}
}

// TestStoreTruncatedNotStored — обрезанное потолком max_body_bytes тело
// (T-049) раннего Put не получает вовсе: хэш усечённых байт врал бы при
// дедупликации; запись живёт с nil-хэшем и признаком, соседняя нормальная
// запись сохраняется.
func TestStoreTruncatedNotStored(t *testing.T) {
	huge := strings.Repeat("a", int(httpfetch.DefaultLimitBodyBytes)+128)
	normal := storePageBody("обычная")
	script := map[string]*workerPageScript{
		"huge.test":            {status: 200, body: huge},
		"http://huge.test/":    {status: 200, body: huge},
		"ok.huge.test":         {status: 200, body: normal},
		"http://ok.huge.test/": {status: 200, body: normal},
	}
	st := newStoreFake()
	res := storeRun(t, storeWorker(t, script, st, nil), "huge.test", "", "ok")
	hugeRec := &res.Subdomains[0].Records[0]
	if !hugeRec.Truncated || hugeRec.ContentHash == nil {
		t.Fatalf("сценарий сломан: truncated=%v hash=%v", hugeRec.Truncated, hugeRec.ContentHash)
	}
	if hugeRec.ContentStored {
		t.Fatal("обрезанная страница помечена сохранённой")
	}

	if err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if hugeRec.ContentHash != nil || !hugeRec.ContentNotStored {
		t.Errorf("обрезанная запись: hash=%v not_stored=%v (ожидался nil/true)",
			hugeRec.ContentHash, hugeRec.ContentNotStored)
	}
	// ok скачан дважды (оба протокола, один сайт → 1 запись): 2 ранних
	// Put, сверка сняла лишний — ровно один блок и одна ссылка.
	_, writes, deletes, refs := st.snapshot()
	if writes != 1 || refs[content.HashOf([]byte(normal))] != 1 {
		t.Errorf("сохранена только нормальная запись: блоков %d, ссылок normal %d", writes, refs[content.HashOf([]byte(normal))])
	}
	if deletes != 1 {
		t.Errorf("Delete: вызовов %d, ожидался 1 (лишнее скачивание normal)", deletes)
	}
}

// --- ошибки раннего Put и сверки: повторная доставка (ТЗ 3.4) -------------------

// TestStoreWorkerPutErrors — сторонняя ошибка раннего Put и нарушение
// контракта HashOf роняют задачу в ProcessDomain (T-147): контент
// ссылки не получил, молчание оставило бы его несохранённым до
// перепроверки T-030; повторная доставка переиграет эпизод.
func TestStoreWorkerPutErrors(t *testing.T) {
	script := func(host string) map[string]*workerPageScript {
		return map[string]*workerPageScript{
			host:                   {status: 200, body: storePageBody(host)},
			"http://" + host + "/": {status: 200, body: storePageBody(host)},
		}
	}
	t.Run("сторонняя ошибка Put", func(t *testing.T) {
		st := newStoreFake()
		st.newErr = func([]byte) error { return errors.New("БД недоступна") }
		_, err := storeRunErr(t, storeWorker(t, script("boom.test"), st, nil), "boom.test")
		if err == nil || !strings.Contains(err.Error(), "сохранение контента") {
			t.Fatalf("ProcessDomain при сбое хранилища: %v", err)
		}
	})
	t.Run("чужой хэш Put", func(t *testing.T) {
		st := newStoreFake()
		st.badHash = true
		_, err := storeRunErr(t, storeWorker(t, script("hash.test"), st, nil), "hash.test")
		if err == nil || !strings.Contains(err.Error(), "HashOf") {
			t.Fatalf("ProcessDomain при чужом хэше: %v", err)
		}
	})
	t.Run("отмена ctx", func(t *testing.T) {
		st := newStoreFake()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		w := storeWorker(t, script("cancel.test"), st, nil)
		_, err := w.ProcessDomain(ctx, queue.HTTPTask{
			ZoneID: 1, DomainID: 1, Name: "cancel.test",
			Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
		})
		if err == nil {
			t.Fatal("ProcessDomain при отмене ctx прошёл")
		}
	})
}

// storeRunErr — storeRun с ошибкой (громоздить t.Fatalf в негативных
// тестах нельзя — ошибка и есть ожидаемый исход).
func storeRunErr(t *testing.T, w *Worker, name string) (*DomainResult, error) {
	t.Helper()
	return w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 1, Name: name,
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})
}

// mustFetch — сценарий без журнала (для сборок в одну строку).
func mustFetch(script map[string]*workerPageScript) FetchFunc {
	f, _ := workerFakeFetch(script)
	return f
}

// storeManualResult — минимальный результат с одной сохраняемой записью
// (ручная сборка: проверка Save не требует сети). stored — был ли ранний
// Put (реестр puts имитирует его успешный исход).
func storeManualResult(name string, body string, stored bool) (*DomainResult, content.Hash) {
	h := content.HashOf([]byte(body))
	res := &DomainResult{
		ZoneID: 1, DomainID: 1, Name: name,
		Subdomains: []SubdomainResult{{
			SubdomainID: 0, Label: "", Hostname: name,
			Records: []Record{{
				Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
				SubdomainID: 0, Label: "", FinalURL: "https://" + name + "/",
				ContentHash: &h, ContentStored: stored, StoreContent: true,
			}},
		}},
	}
	if stored {
		res.puts = []workerPut{{hash: h}}
	}
	return res, h
}

// TestStoreSaveErrors — ошибки сверки: сбой Delete и отмена ctx роняют
// Save (доставка без XACK вернёт задачу), ссылка без раннего Put —
// рассинхронизация воркера и хранилища (громкая ошибка, T-044),
// снятие уже несуществующей ссылки — Warn без ошибки (гонка повторной
// доставки); nil-хранилище отвергается конструктором.
func TestStoreSaveErrors(t *testing.T) {
	t.Run("сбой Delete", func(t *testing.T) {
		// Сценарий со срезом: обе проверки домена качают один контент,
		// свёрнутая запись требует один Delete — он и падает.
		body := storePageBody("del")
		script := map[string]*workerPageScript{
			"del.test":         {status: 200, body: body},
			"http://del.test/": {status: 200, body: body},
		}
		st := newStoreFake()
		st.delErr = errors.New("БД недоступна")
		res := storeRun(t, storeWorker(t, script, st, nil), "del.test", "")
		err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res)
		if err == nil || !strings.Contains(err.Error(), "снятие ссылки") {
			t.Fatalf("Save при сбое Delete: %v", err)
		}
	})
	t.Run("отмена ctx", func(t *testing.T) {
		res, _ := storeManualResult("cancel-save.test", "тело", false)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := storeSaver(t, newStoreFake(), &bytes.Buffer{}).Save(ctx, res)
		if err != nil {
			t.Fatalf("Save без обращений к хранилищу при отмене: %v", err)
		}
	})
	t.Run("ссылки уже нет", func(t *testing.T) {
		// Гонка повторной доставки: ссылку лишнего раннего Put снял
		// конкурент — Warn, Save не роняется (счётчик цел у победившей
		// доставки).
		res, h := storeManualResult("gone.test", "тело", true)
		res.puts = append(res.puts, workerPut{hash: h}) // лишний Put без записи
		st := &storeFake{blocks: map[content.Hash][]byte{}, refs: map[content.Hash]int{}, delMissing: true}
		var logBuf bytes.Buffer
		if err := storeSaver(t, st, &logBuf).Save(context.Background(), res); err != nil {
			t.Fatalf("Save при снятой ссылке: %v", err)
		}
		if !strings.Contains(logBuf.String(), "ссылки уже нет") {
			t.Errorf("Warn о гонке не записан: %q", logBuf.String())
		}
	})
	t.Run("запись без раннего Put", func(t *testing.T) {
		// Рассинхронизация: запись требует ссылку, которой не клал —
		// ссылка без счётчика уронила бы учёт при свопе (T-044).
		res, _ := storeManualResult("desync.test", "тело", true)
		res.puts = nil
		err := storeSaver(t, newStoreFake(), &bytes.Buffer{}).Save(context.Background(), res)
		if err == nil || !strings.Contains(err.Error(), "без раннего Put") {
			t.Fatalf("Save при рассинхронизации: %v", err)
		}
	})
	t.Run("хранилище не задано", func(t *testing.T) {
		if _, err := NewContentSaver(nil, nil); err == nil {
			t.Fatal("NewContentSaver(nil) прошёл")
		}
	})
	t.Run("nil-результат", func(t *testing.T) {
		if err := storeSaver(t, newStoreFake(), &bytes.Buffer{}).Save(context.Background(), nil); err != nil {
			t.Fatalf("Save(nil): %v", err)
		}
	})
}

// --- интеграция OnResult → store в цикле Run ------------------------------------

// TestStoreOnResultIntegration — цикл целиком (как в сборке сервиса,
// cmd/http-worker): доставка q:http → ProcessDomain (ранние Put) →
// OnResult (Save до XACK) → Ack; контент в хранилище, хэш записи жив,
// лишние ссылки сняты сверкой.
func TestStoreOnResultIntegration(t *testing.T) {
	page := storePageBody("интеграция")
	script := map[string]*workerPageScript{
		"ack.test":         {status: 200, body: page},
		"http://ack.test/": {status: 200, body: page},
	}
	st := newStoreFake()
	w := storeWorker(t, script, st, nil)
	q := &workerFakeQueue{deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		TaskID: "task-1", ZoneID: 5, DomainID: 42, Name: "ack.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}}
	saver := storeSaver(t, st, &bytes.Buffer{})

	var stored *DomainResult
	opts := Options{
		Store: st, // тот же фейк, что у воркера (ранние Put — в него)
		OnResult: func(c context.Context, res *DomainResult) error {
			// Порядок сборки сервиса: сверка контента до будущей записи
			// T-058 (обе — до XACK).
			if err := saver.Save(c, res); err != nil {
				return err
			}
			stored = res
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	deadline := time.After(5 * time.Second)
	for q.ackedCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("доставка не подтверждена за отведённое время")
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}

	if stored == nil || stored.RecordCount() != 1 {
		t.Fatalf("OnResult: результат %v", stored)
	}
	rec := &stored.Subdomains[0].Records[0]
	h := content.HashOf([]byte(page))
	if rec.ContentHash == nil || *rec.ContentHash != h || rec.ContentNotStored {
		t.Fatalf("запись после Save: hash=%v not_stored=%v", rec.ContentHash, rec.ContentNotStored)
	}
	// Оба скачивания страницы (https и http) прошли ранними Put, сверка
	// оставила одну ссылку записи.
	_, writes, deletes, refs := st.snapshot()
	if writes != 1 || refs[h] != 1 || deletes != 1 {
		t.Fatalf("хранилище: блоков %d, ссылок %d, Delete %d — ожидались 1/1/1", writes, refs[h], deletes)
	}
	if got, err := st.Get(context.Background(), h); err != nil || string(got) != page {
		t.Errorf("Get сохранённого контента: %v", err)
	}
	if q.ackedCount() != 1 {
		t.Errorf("подтверждений %d, ожидалось 1", q.ackedCount())
	}
}

// --- пустое тело — валидный контент (T-138) -----------------------------------

// TestStoreEmptyBodyKeepsHash — пустое тело ответа (Content-Length: 0) —
// валидный контент с валидным хэшем: пустые 200 и 400/404/500 дают записи
// с живым HashOf пустых байтов, ранний Put сохраняет пустые байты как
// любые другие, все пустые страницы разных доменов — один блок
// дедупликации (§8). До T-138 ToUTF8 отдавал nil, предохранитель «тело
// записи недоступно» гасил content_hash у ~4% задач — парковок и пустых
// страниц ошибок.
func TestStoreEmptyBodyKeepsHash(t *testing.T) {
	fetchScript := map[string]*workerPageScript{
		"empty-200.test": {status: 200},
		"empty-400.test": {status: 400},
		"empty-404.test": {status: 404},
		"empty-500.test": {status: 500},
	}
	st := newStoreFake()
	w := storeWorker(t, fetchScript, st, nil)
	domains := []string{"empty-200.test", "empty-400.test", "empty-404.test", "empty-500.test"}

	// Хэш пустых байтов один у всех пустых страниц — HashOf(nil) ==
	// HashOf([]byte{}) (контракт T-041), классы эквивалентности пустых
	// страниц строятся по нему же.
	emptyHash := content.HashOf(nil)

	var logBuf bytes.Buffer
	saver := storeSaver(t, st, &logBuf)
	for i, name := range domains {
		res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
			ZoneID: 1, DomainID: int64(i + 1), Name: name,
			Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
		})
		if err != nil {
			t.Fatalf("ProcessDomain(%s): %v", name, err)
		}
		if len(res.Subdomains[0].Records) != 1 {
			t.Fatalf("%s: записей %d, ожидалась 1 (оба протокола — один пустой сайт)", name, len(res.Subdomains[0].Records))
		}
		rec := &res.Subdomains[0].Records[0]
		if rec.HTTPStatus < 200 || rec.ErrClass != ErrNone {
			t.Fatalf("%s: сломан сценарий — статус %d, ошибка %v", name, rec.HTTPStatus, rec.ErrClass)
		}
		if rec.ContentHash == nil || *rec.ContentHash != emptyHash {
			t.Fatalf("%s: хэш записи %v, ожидался хэш пустого тела %s", name, rec.ContentHash, emptyHash)
		}
		if !rec.ContentStored {
			t.Fatalf("%s: пустое тело не сохранено ранним Put — пустое тело валидно (T-138)", name)
		}
		if err := saver.Save(context.Background(), res); err != nil {
			t.Fatalf("Save(%s): %v", name, err)
		}
	}
	if strings.Contains(logBuf.String(), "недоступно") {
		t.Errorf("предохранитель сработал на пустом теле:\n%s", logBuf.String())
	}

	// Каждый домен скачал пустую страницу дважды (оба протокола), запись
	// одна — сверка сняла лишние ссылки; блок один на все пустые страницы.
	puts, writes, deletes, refs := st.snapshot()
	if puts != 2*len(domains) {
		t.Fatalf("Put: вызовов %d, ожидалось %d (по два скачивания на домен)", puts, 2*len(domains))
	}
	if writes != 1 {
		t.Fatalf("новых блоков %d, ожидался 1 (пустые страницы — один блок)", writes)
	}
	if deletes != len(domains) {
		t.Fatalf("Delete: вызовов %d, ожидалось %d (лишнее скачивание каждого домена)", deletes, len(domains))
	}
	if refs[emptyHash] != len(domains) {
		t.Errorf("refcount пустого блока: %d, ожидался %d", refs[emptyHash], len(domains))
	}
	if got, err := st.Get(context.Background(), emptyHash); err != nil || len(got) != 0 {
		t.Errorf("Get пустого блока: %v, байтов %d (ожидалось 0)", err, len(got))
	}
}

// TestStoreEmptyBodyCachePath — кэш итоговых URL (§5.5 п. 6) с пустой
// страницей: кэш-итог несёт сохранённый пустой контент как валидный
// (T-138), запись кэш-попадания становится оригиналом класса (T-055) и
// наследует ссылку раннего Put — content_hash не гасится ни у кэш-записи,
// ни у алиаса-соседа.
func TestStoreEmptyBodyCachePath(t *testing.T) {
	old := storePageBody("old")
	script := map[string]*workerPageScript{
		"example.com": { // обе проверки bare: редирект на пустую www
			status: 301, final: "https://www.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "https://example.com/", Status: 301}},
		},
		"https://www.example.com/": {status: 200}, // пустая страница
		"http://www.example.com/": { // другой сайт — вариант 1
			status: 200, body: old, final: "http://old.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "http://www.example.com/", Status: 301}},
		},
	}
	fetch, log := workerFakeFetch(script)
	st := newStoreFake()
	// PerDomain=1 — детерминированный порядок проверок (кэш-попадание
	// www-https стабильно).
	w := workerWorker(t, fetch, func(o *Options) { o.Store = st; o.PerDomain = 1 })
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 7, Name: "example.com",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(1, "www")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}

	// Кэш-попадание: пустая страница www-https скачана один раз
	// (проверкой bare), сети от www-https нет.
	if n := log.countHost("www.example.com"); n != 1 {
		t.Fatalf("скачиваний www: %d, ожидалось 1 (https — из кэша итоговых URL)", n)
	}
	bare, www := &res.Subdomains[0], &res.Subdomains[1]
	if len(bare.Records) != 1 || len(www.Records) != 2 {
		t.Fatalf("записей: bare %d, www %d — ожидались 1 и 2", len(bare.Records), len(www.Records))
	}
	// Оригинал класса (www v0, кэш-запись) наследует сохранённый пустой
	// контент (T-138): ранний Put прошёл, ссылка ждёт запись.
	wwwHome := &www.Records[0]
	if wwwHome.ContentHash == nil || *wwwHome.ContentHash != content.HashOf(nil) {
		t.Fatalf("кэш-запись: hash=%v, ожидался хэш пустого тела", wwwHome.ContentHash)
	}
	if !wwwHome.ContentStored {
		t.Fatal("кэш-запись: пустой контент не сохранён ранним Put (T-138)")
	}

	var logBuf bytes.Buffer
	if err := storeSaver(t, st, &logBuf).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if strings.Contains(logBuf.String(), "недоступно") {
		t.Errorf("предохранитель сработал на пустом теле кэш-записи:\n%s", logBuf.String())
	}

	// Ранние Put: пустая страница скачана дважды (обе проверки bare), old
	// — один раз; сверка сняла лишнюю пустую ссылку. Два блока — пустая
	// страница (оригинал класса bare+www) и старый сайт; хэш пустого тела
	// жив у оригинала, алиас ссылается через alias_subdomain_id.
	puts, writes, deletes, refs := st.snapshot()
	emptyHash, oldHash := content.HashOf(nil), content.HashOf([]byte(old))
	if puts != 3 || writes != 2 {
		t.Fatalf("Put: вызовов %d, новых блоков %d — ожидалось 3 и 2", puts, writes)
	}
	if deletes != 1 {
		t.Fatalf("Delete: вызовов %d, ожидался 1 (второе скачивание пустой страницы)", deletes)
	}
	if refs[emptyHash] != 1 || refs[oldHash] != 1 {
		t.Errorf("refcount: пустой %d, старый %d — ожидались 1 и 1", refs[emptyHash], refs[oldHash])
	}
	if wwwHome.ContentHash == nil || *wwwHome.ContentHash != emptyHash || wwwHome.ContentNotStored {
		t.Errorf("оригинал после Save: hash=%v not_stored=%v", wwwHome.ContentHash, wwwHome.ContentNotStored)
	}
	if bare.Records[0].ContentHash != nil || bare.Records[0].ContentNotStored {
		t.Errorf("алиас: hash=%v not_stored=%v — контент класса у оригинала",
			bare.Records[0].ContentHash, bare.Records[0].ContentNotStored)
	}
	if got, err := st.Get(context.Background(), emptyHash); err != nil || len(got) != 0 {
		t.Errorf("Get пустого блока: %v, байтов %d (ожидалось 0)", err, len(got))
	}
}

// --- память слота: структурный инвариант (T-147) -------------------------------

// TestStoreNoBodyFieldsInResult — тела страниц не удерживаются памятью
// слота: у SchemeCheck и Record (всё, что живёт в задаче до конца
// домена) нет полей-байтов тел — контент представлен хэшем и признаком
// сохранения, байты лежат в хранилище. Регрессионный инвариант: новое
// поле []byte в этих структурах — механический откат T-147.
func TestStoreNoBodyFieldsInResult(t *testing.T) {
	for _, tp := range []reflect.Type{
		reflect.TypeOf(SchemeCheck{}),
		reflect.TypeOf(Record{}),
		reflect.TypeOf(workerPage{}),
	} {
		for i := 0; i < tp.NumField(); i++ {
			f := tp.Field(i)
			if f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Uint8 {
				t.Errorf("%s.%s: поле []byte в структуре результата (тела удерживаются памятью слота — T-147)",
					tp.Name(), f.Name)
			}
		}
	}
}

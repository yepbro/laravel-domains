// Юнит-тесты сохранения контента (T-056): ContentSaver над фейковым
// ContentStore — сохранение только оригиналов (алиасы не сохраняются и
// теряют content_hash), дедупликация хранилища между доменами (Created =
// false, refcount), ErrStoreFull/ErrTooLarge — результат живой с nil-хэшем
// и признаком «контент не сохранён», обрезанное тело не сохраняется вовсе,
// отмена ctx и прочие ошибки — ошибка Save (повторная доставка),
// интеграция OnResult → store в цикле Run; пустое тело (T-138) — валидный
// контент с живым хэшем, в том числе с кэша итоговых URL. Идентификаторы с
// префиксом store* против коллизий с соседними файлами пакета.

package httpworker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
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
// для проверки защиты воркера.
type storeFake struct {
	mu      sync.Mutex
	blocks  map[content.Hash][]byte
	refs    map[content.Hash]int
	puts    int // все вызовы Put
	writes  int // вызовы, записавшие новые байты
	newErr  func(data []byte) error
	badHash bool
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

func (f *storeFake) Delete(_ context.Context, h content.Hash) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refs[h] <= 0 {
		return content.ErrNotFound
	}
	f.refs[h]--
	return nil
}

func (f *storeFake) snapshot() (puts, writes int, refs map[content.Hash]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(map[content.Hash]int, len(f.refs))
	for h, n := range f.refs {
		cp[h] = n
	}
	return f.puts, f.writes, cp
}

// storeSaver — сохранатель над хранилищем с перехватом лога.
func storeSaver(t *testing.T, st content.ContentStore, logBuf *bytes.Buffer) *ContentSaver {
	t.Helper()
	s, err := NewContentSaver(st, slog.New(slog.NewTextHandler(logBuf, nil)))
	if err != nil {
		t.Fatalf("NewContentSaver: %v", err)
	}
	return s
}

// storeWorker — воркер над сценарием загрузки (см. workerFakeFetch).
func storeWorker(t *testing.T, script map[string]*workerPageScript) *Worker {
	t.Helper()
	fetch, _ := workerFakeFetch(script)
	return workerWorker(t, fetch, nil)
}

// storePageBody — различимые тела страниц для сценариев.
func storePageBody(label string) string {
	return "<html><body>" + strings.Repeat("текст страницы "+label+" для сохранения в хранилище контента ", 24) + "</body></html>"
}

// --- сохранение только оригиналов (ТЗ 5.5 п. 6 «контент хранится один раз») ---

// TestStoreSavesOriginalsOnly — сценарий TestWorkerProcessDomain: класс
// bare+www (один итоговый URL) хранится одним Put записи-оригинала www;
// алиас bare теряет content_hash (контент класса — у оригинала, отдельной
// ссылки нет), независимые записи (вариант 1 www, api) сохраняются своими
// телами. Тела всех записей отпускаются после Save.
func TestStoreSavesOriginalsOnly(t *testing.T) {
	home, old := storePageBody("home"), storePageBody("old")
	api := storePageBody("api")
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
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
	})
	w := workerWorker(t, fetch, func(o *Options) { o.PerDomain = 1 })
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 7, Name: "example.com",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(1, "www"), workerSubMsg(2, "api")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}
	bare, www, apiSub := &res.Subdomains[0], &res.Subdomains[1], &res.Subdomains[2]
	// До Save хэш алиаса ещё на месте (материал вариант-логики T-053).
	if bare.Records[0].ContentHash == nil {
		t.Fatal("до Save хэш алиаса отсутствует (сломан сценарий)")
	}

	st := newStoreFake()
	if err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Ровно три Put: home (оригинал класса bare+www), old (вариант 1 www),
	// api; алиас bare не сохраняется — контент класса уже у www.
	puts, writes, refs := st.snapshot()
	if puts != 3 || writes != 3 {
		t.Fatalf("Put: вызовов %d, новых блоков %d — ожидалось 3 и 3 (алиас не сохраняется)", puts, writes)
	}
	homeHash := content.HashOf([]byte(home))
	if refs[homeHash] != 1 {
		t.Errorf("refcount home: %d, ожидалась 1 ссылка (класс — один Put)", refs[homeHash])
	}
	if refs[content.HashOf([]byte(old))] != 1 || refs[content.HashOf([]byte(api))] != 1 {
		t.Error("независимые записи не сохранили по собственной ссылке")
	}

	// Алиас: content_hash снят (NULL в БД), признак «не сохранён» НЕ
	// ставится — контент класса хранится записью-оригинала.
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
	// Тела отпущены (байты не удерживаются после сохранения).
	for _, sub := range []*SubdomainResult{bare, www, apiSub} {
		for j := range sub.Records {
			if sub.Records[j].Body != nil {
				t.Errorf("запись %s/%d удерживает тело после Save", sub.Hostname, j)
			}
		}
	}
}

// TestStoreDedupAcrossDomains — уровень хранилища (§8): одна и та же
// парковочная страница двух доменов — второй Put дедуплицируется
// (Created=false, новых байтов нет), refcount растёт до 2; обе записи
// держат живой хэш. Уровень задачи (один Put на класс) — предыдущий тест.
func TestStoreDedupAcrossDomains(t *testing.T) {
	parking := storePageBody("парковка")
	script := func(host string) map[string]*workerPageScript {
		return map[string]*workerPageScript{
			host:                   {status: 200, body: parking},
			"http://" + host + "/": {status: 200, body: parking},
		}
	}
	fetch1, _ := workerFakeFetch(script("park1.test"))
	fetch2, _ := workerFakeFetch(script("park2.test"))
	w1 := workerWorker(t, fetch1, nil)
	w2 := workerWorker(t, fetch2, nil)

	res1, err := w1.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 1, Name: "park1.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain(1): %v", err)
	}
	res2, err := w2.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 2, Name: "park2.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain(2): %v", err)
	}

	st := newStoreFake()
	saver := storeSaver(t, st, &bytes.Buffer{})
	for _, res := range []*DomainResult{res1, res2} {
		if err := saver.Save(context.Background(), res); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	puts, writes, refs := st.snapshot()
	h := content.HashOf([]byte(parking))
	if puts != 2 || writes != 1 {
		t.Fatalf("Put: вызовов %d, новых блоков %d — ожидалось 2 и 1 (дедуп между доменами)", puts, writes)
	}
	if refs[h] != 2 {
		t.Errorf("refcount парковки: %d, ожидался 2 (по ссылке на запись)", refs[h])
	}
	if res1.Subdomains[0].Records[0].ContentHash == nil || *res1.Subdomains[0].Records[0].ContentHash != h {
		t.Error("первая запись не держит хэш парковки")
	}
	if res2.Subdomains[0].Records[0].ContentHash == nil || *res2.Subdomains[0].Records[0].ContentHash != h {
		t.Error("вторая запись не держит хэш парковки")
	}
}

// --- переполнение и лимиты (ТЗ 8, T-046) ---------------------------------------

// TestStoreFullResultAlive — ErrStoreFull: контент не сохраняется, но
// результат домена живёт (Save = nil): запись с content_hash = nil и
// признаком ContentNotStored, Warn в логе. Дедуп-ссылка на УЖЕ живой
// контент проходит и при переполнении (PackStore делает UPDATE раньше
// порога диска, T-046): запись с существующим хэшем сохраняется обычным
// путём.
func TestStoreFullResultAlive(t *testing.T) {
	warm, fresh := storePageBody("тёплая"), storePageBody("свежая")
	page := func(host, body string) map[string]*workerPageScript {
		return map[string]*workerPageScript{
			host:                   {status: 200, body: body},
			"http://" + host + "/": {status: 200, body: body},
		}
	}
	process := func(t *testing.T, host, body string) *DomainResult {
		t.Helper()
		w := storeWorker(t, page(host, body))
		res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
			ZoneID: 1, DomainID: 1, Name: host,
			Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
		})
		if err != nil {
			t.Fatalf("ProcessDomain(%s): %v", host, err)
		}
		return res
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

	// 1) до переполнения контент warm записан.
	resWarm := process(t, "warm.test", warm)
	if err := saver.Save(context.Background(), resWarm); err != nil {
		t.Fatalf("Save(warm): %v", err)
	}

	// 2) диск полон: тот же контент другого домена — дедуп-ссылка
	// проходит, хэш записи жив.
	full = true
	resWarm2 := process(t, "warm2.test", warm)
	if err := saver.Save(context.Background(), resWarm2); err != nil {
		t.Fatalf("Save(warm2): %v (дедуп-ссылка обязана проходить при полном диске)", err)
	}
	rec := &resWarm2.Subdomains[0].Records[0]
	if rec.ContentHash == nil || rec.ContentNotStored {
		t.Errorf("дедуп-ссылка при полном диске: hash=%v not_stored=%v", rec.ContentHash, rec.ContentNotStored)
	}

	// 3) новый контент при полном диске — пропуск: результат жив.
	resFresh := process(t, "fresh.test", fresh)
	logBuf := &bytes.Buffer{}
	if err := storeSaver(t, st, logBuf).Save(context.Background(), resFresh); err != nil {
		t.Fatalf("Save(fresh): %v — переполнение не роняет результат (ТЗ 8)", err)
	}
	rec = &resFresh.Subdomains[0].Records[0]
	if rec.ContentHash != nil || !rec.ContentNotStored {
		t.Errorf("запись при полном диске: hash=%v not_stored=%v (ожидался nil/true)",
			rec.ContentHash, rec.ContentNotStored)
	}
	if rec.Body != nil {
		t.Error("тело не отпущено после пропуска сохранения")
	}
	if _, _, refs := st.snapshot(); refs[content.HashOf([]byte(fresh))] != 0 {
		t.Error("контент fresh попал в хранилище при переполнении")
	}
	if msg := logBuf.String(); !strings.Contains(msg, "переполнено") || !strings.Contains(msg, "ErrStoreFull") {
		t.Errorf("Warn о переполнении не записан: %q", msg)
	}
}

// TestStoreTooLarge — ErrTooLarge (тело сверх max_store_bytes, ТЗ 5.5):
// контент отвергается хранилищем целиком — запись жива, content_hash =
// nil, ContentNotStored, Warn; Save не падает.
func TestStoreTooLarge(t *testing.T) {
	small := storePageBody("малая")
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"big.test":         {status: 200, body: storePageBody("большая страница")},
		"http://big.test/": {status: 200, body: small},
	})
	// Обе проверки big.test сходятся на одном URL → 1 запись https; для
	// сверхлимитного тела хватит одной записи.
	w := workerWorker(t, fetch, func(o *Options) { o.PerDomain = 1 })
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 3, Name: "big.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}

	st := newStoreFake()
	st.newErr = func(data []byte) error {
		if len(data) > len(small) {
			return errors.Join(content.ErrTooLarge, errors.New("300 байт при лимите 100"))
		}
		return nil
	}
	logBuf := &bytes.Buffer{}
	if err := storeSaver(t, st, logBuf).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v — сверхлимитный контент не роняет результат", err)
	}
	rec := &res.Subdomains[0].Records[0]
	if rec.ContentHash != nil || !rec.ContentNotStored {
		t.Errorf("запись: hash=%v not_stored=%v (ожидался nil/true)", rec.ContentHash, rec.ContentNotStored)
	}
	if _, writes, _ := st.snapshot(); writes != 0 {
		t.Errorf("сверхлимитный контент записан: %d блоков", writes)
	}
	if msg := logBuf.String(); !strings.Contains(msg, "max_store_bytes") {
		t.Errorf("Warn о лимите не записан: %q", msg)
	}
}

// TestStoreTruncatedNotStored — обрезанное потолком max_body_bytes тело
// (T-049) не сохраняется и не хэшируется для хранения: хэш усечённых
// байт врал бы при дедупликации; Put не вызывается вовсе, запись живёт
// с nil-хэшем и признаком, соседняя нормальная запись сохраняется.
func TestStoreTruncatedNotStored(t *testing.T) {
	huge := strings.Repeat("a", int(httpfetch.DefaultLimitBodyBytes)+128)
	normal := storePageBody("обычная")
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"huge.test":            {status: 200, body: huge},
		"http://huge.test/":    {status: 200, body: huge},
		"ok.huge.test":         {status: 200, body: normal},
		"http://ok.huge.test/": {status: 200, body: normal},
	})
	w := workerWorker(t, fetch, nil)
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 2, DomainID: 4, Name: "huge.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(5, "ok")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}
	hugeRec := &res.Subdomains[0].Records[0]
	if !hugeRec.Truncated || hugeRec.ContentHash == nil {
		t.Fatalf("сценарий сломан: truncated=%v hash=%v", hugeRec.Truncated, hugeRec.ContentHash)
	}

	st := newStoreFake()
	if err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if hugeRec.ContentHash != nil || !hugeRec.ContentNotStored {
		t.Errorf("обрезанная запись: hash=%v not_stored=%v (ожидался nil/true)",
			hugeRec.ContentHash, hugeRec.ContentNotStored)
	}
	if _, writes, refs := st.snapshot(); writes != 1 || refs[content.HashOf([]byte(normal))] != 1 {
		t.Errorf("сохранена только нормальная запись: блоков %d, ссылок normal %d", writes, refs[content.HashOf([]byte(normal))])
	}
}

// --- ошибки Save: повторная доставка (ТЗ 3.4) -----------------------------------

// storeManualResult — минимальный результат с одной сохраняемой записью
// (ручная сборка: проверка Save не требует сети).
func storeManualResult(name string, body string) (*DomainResult, content.Hash) {
	h := content.HashOf([]byte(body))
	return &DomainResult{
		ZoneID: 1, DomainID: 1, Name: name,
		Subdomains: []SubdomainResult{{
			SubdomainID: 0, Label: "", Hostname: name,
			Records: []Record{{
				Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
				SubdomainID: 0, Label: "", FinalURL: "https://" + name + "/",
				ContentHash: &h, Body: []byte(body), StoreContent: true,
			}},
		}},
	}, h
}

// TestStoreSaveErrors — отмена ctx, сторонние ошибки Put и нарушение
// контракта HashOf — ошибка Save (доставка без XACK вернёт задачу);
// nil-хранилище отвергается конструктором.
func TestStoreSaveErrors(t *testing.T) {
	t.Run("отмена ctx", func(t *testing.T) {
		res, _ := storeManualResult("cancel.test", "тело")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := storeSaver(t, newStoreFake(), &bytes.Buffer{}).Save(ctx, res)
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("Save при отмене: %v", err)
		}
	})
	t.Run("сторонняя ошибка Put", func(t *testing.T) {
		res, _ := storeManualResult("boom.test", "тело")
		st := newStoreFake()
		st.newErr = func([]byte) error { return errors.New("БД недоступна") }
		err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res)
		if err == nil || !strings.Contains(err.Error(), "сохранение контента") {
			t.Fatalf("Save при сбое хранилища: %v", err)
		}
	})
	t.Run("чужой хэш Put", func(t *testing.T) {
		res, _ := storeManualResult("hash.test", "тело")
		st := newStoreFake()
		st.badHash = true
		err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res)
		if err == nil || !strings.Contains(err.Error(), "HashOf") {
			t.Fatalf("Save при чужом хэше: %v", err)
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

// --- групповая фиксация доставки (T-102) ----------------------------------------

// batchStoreFake — storeFake с групповой фиксацией: PutBatch пишет
// элементы пачкой (та же семантика дедупа/refcount) и помнит вызовы;
// batchErr — сбой пачки целиком, batchErrs — по-элементные отказы
// (индекс → ошибка), badBatchHash — нарушение контракта HashOf.
type batchStoreFake struct {
	*storeFake
	batches     [][][]byte
	batchErr    error
	batchErrs   map[int]error
	badBatch    bool
	badBatchMsg string // подстановка чужого хэша в результат
}

func (f *batchStoreFake) PutBatch(ctx context.Context, items [][]byte) ([]content.PutResult, []error, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, items)
	if f.batchErr != nil {
		return nil, nil, f.batchErr
	}
	results := make([]content.PutResult, len(items))
	errs := make([]error, len(items))
	for i, data := range items {
		if err := f.batchErrs[i]; err != nil {
			errs[i] = err
			continue
		}
		h := content.HashOf(data)
		if f.refs[h] > 0 {
			f.refs[h]++
			results[i] = content.PutResult{Hash: h}
			continue
		}
		f.blocks[h] = append([]byte(nil), data...)
		f.refs[h] = 1
		f.writes++
		results[i] = content.PutResult{Hash: h, Created: true}
	}
	if f.badBatch {
		results[0].Hash = content.HashOf([]byte(f.badBatchMsg))
	}
	return results, errs, nil
}

// storeBatchResult — результат с сохраняемыми записями (тела bodies).
func storeBatchResult(name string, bodies ...string) (*DomainResult, []content.Hash) {
	res := &DomainResult{ZoneID: 1, DomainID: 1, Name: name,
		Subdomains: []SubdomainResult{{SubdomainID: 0, Label: "", Hostname: name}}}
	hashes := make([]content.Hash, len(bodies))
	for i, b := range bodies {
		h := content.HashOf([]byte(b))
		hashes[i] = h
		res.Subdomains[0].Records = append(res.Subdomains[0].Records, Record{
			Variant: model.VariantMain, Scheme: model.SchemeHTTPS,
			FinalURL:    fmt.Sprintf("https://%s/%d", name, i),
			ContentHash: &h, Body: []byte(b), StoreContent: true,
		})
	}
	return res, hashes
}

// TestStoreSaveBatch — доставка с несколькими сохраняемыми записями
// фиксируется одной пачкой (T-102): PutBatch вызван один раз со всеми
// телами в порядке записей, построчный Put не используется, записи
// получают хэши и отпускают тела.
func TestStoreSaveBatch(t *testing.T) {
	res, hashes := storeBatchResult("batch.test", "тело раз", "тело два", "тело три")
	st := &batchStoreFake{storeFake: newStoreFake()}
	if err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if len(st.batches) != 1 || len(st.batches[0]) != 3 {
		t.Fatalf("PutBatch: вызовов %d, элементов %d — want 1 пачка из 3", len(st.batches), len(st.batches[0]))
	}
	for i, b := range []string{"тело раз", "тело два", "тело три"} {
		if string(st.batches[0][i]) != b {
			t.Errorf("элемент пачки %d: тело не совпало (пачка в порядке записей)", i)
		}
	}
	if puts, _, _ := st.snapshot(); puts != 0 {
		t.Errorf("построчных Put: %d, want 0 (доставка фиксируется пачкой)", puts)
	}
	_, writes, refs := st.snapshot()
	if writes != 3 {
		t.Errorf("новых блоков: %d, want 3", writes)
	}
	for i, rec := range res.Subdomains[0].Records {
		if rec.ContentHash == nil || rec.ContentNotStored {
			t.Fatalf("запись %d: hash=%v not_stored=%v", i, rec.ContentHash, rec.ContentNotStored)
		}
		if *rec.ContentHash != hashes[i] {
			t.Errorf("запись %d: хэш записи ≠ HashOf(тела)", i)
		}
		if rec.Body != nil {
			t.Errorf("запись %d удерживает тело после Save", i)
		}
		if refs[hashes[i]] != 1 {
			t.Errorf("запись %d: ссылок %d, want 1", i, refs[hashes[i]])
		}
	}
}

// TestStoreSaveBatchPerItemErrors — по-элементные отказы пачки:
// ErrTooLarge и ErrStoreFull роняют только свою запись (content_hash =
// NULL, ContentNotStored), остальные сохранены, Save не ошибается,
// Warn про переполнение — в логе.
func TestStoreSaveBatchPerItemErrors(t *testing.T) {
	res, hashes := storeBatchResult("mixed.test", "нормальное тело", "сверхлимитное тело", "тело в полный диск")
	st := &batchStoreFake{storeFake: newStoreFake()}
	st.batchErrs = map[int]error{
		1: fmt.Errorf("%w: 100500 байт", content.ErrTooLarge),
		2: content.ErrStoreFull,
	}
	var logBuf bytes.Buffer
	if err := storeSaver(t, st, &logBuf).Save(context.Background(), res); err != nil {
		t.Fatalf("Save с по-элементными отказами: %v", err)
	}
	rec := res.Subdomains[0].Records
	if rec[0].ContentHash == nil || rec[0].ContentNotStored || rec[0].Body != nil {
		t.Errorf("здоровая запись: hash=%v not_stored=%v body=%v", rec[0].ContentHash, rec[0].ContentNotStored, rec[0].Body)
	}
	for i := 1; i <= 2; i++ {
		if rec[i].ContentHash != nil || !rec[i].ContentNotStored {
			t.Errorf("запись %d: hash=%v not_stored=%v — want nil/true", i, rec[i].ContentHash, rec[i].ContentNotStored)
		}
	}
	if _, writes, refs := st.snapshot(); writes != 1 || refs[hashes[0]] != 1 {
		t.Errorf("сохранена только здоровая запись: блоков %d, ссылок %d", writes, refs[hashes[0]])
	}
	if !strings.Contains(logBuf.String(), "переполнено") {
		t.Errorf("Warn про переполнение отсутствует в логе: %q", logBuf.String())
	}
}

// TestStoreSaveBatchErrors — сбой пачки целиком, чужой хэш результата и
// отмена ctx — ошибка Save (доставка без XACK вернётся повторной
// доставкой, ТЗ 3.4).
func TestStoreSaveBatchErrors(t *testing.T) {
	t.Run("сбой пачки", func(t *testing.T) {
		res, _ := storeBatchResult("boom-batch.test", "тело")
		st := &batchStoreFake{storeFake: newStoreFake()}
		st.batchErr = errors.New("БД недоступна")
		err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res)
		if err == nil || !strings.Contains(err.Error(), "сохранение контента") {
			t.Fatalf("Save при сбое пачки: %v", err)
		}
	})
	t.Run("чужой хэш пачки", func(t *testing.T) {
		res, _ := storeBatchResult("hash-batch.test", "тело")
		st := &batchStoreFake{storeFake: newStoreFake()}
		st.badBatch, st.badBatchMsg = true, "чужой хэш пачки"
		err := storeSaver(t, st, &bytes.Buffer{}).Save(context.Background(), res)
		if err == nil || !strings.Contains(err.Error(), "HashOf") {
			t.Fatalf("Save при чужом хэше пачки: %v", err)
		}
	})
	t.Run("отмена ctx", func(t *testing.T) {
		res, _ := storeBatchResult("cancel-batch.test", "тело")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := storeSaver(t, &batchStoreFake{storeFake: newStoreFake()}, &bytes.Buffer{}).Save(ctx, res)
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("Save при отмене: %v", err)
		}
	})
}

// --- интеграция OnResult → store в цикле Run ------------------------------------

// TestStoreOnResultIntegration — цикл целиком (как в сборке сервиса,
// cmd/http-worker): доставка q:http → ProcessDomain → OnResult (Save до
// XACK) → Ack; контент в хранилище, хэш записи жив, тело отпущено.
func TestStoreOnResultIntegration(t *testing.T) {
	page := storePageBody("интеграция")
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"ack.test":         {status: 200, body: page},
		"http://ack.test/": {status: 200, body: page},
	})
	w := workerWorker(t, fetch, nil)
	q := &workerFakeQueue{deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		TaskID: "task-1", ZoneID: 5, DomainID: 42, Name: "ack.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}}
	st := newStoreFake()
	saver := storeSaver(t, st, &bytes.Buffer{})

	var stored *DomainResult
	opts := Options{OnResult: func(c context.Context, res *DomainResult) error {
		// Порядок сборки сервиса: сохранение контента до будущей записи
		// T-058 (обе — до XACK).
		if err := saver.Save(c, res); err != nil {
			return err
		}
		stored = res
		return nil
	}}

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
	if rec.Body != nil {
		t.Error("тело удерживается после сохранения")
	}
	if _, writes, _ := st.snapshot(); writes != 1 {
		t.Errorf("блоков в хранилище: %d, ожидался 1", writes)
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
// с живым HashOf пустых байтов, тела (пустые, не nil) доходят до
// сохранения, все пустые страницы разных доменов — один блок дедупликации
// (§8). Оба пути ContentSaver: построчный Put и пачка PutBatch
// (продакшн-хранилище — content.BatchStore). До T-138 ToUTF8 отдавал nil,
// предохранитель «тело записи недоступно» гасил content_hash у ~4% задач —
// парковок и пустых страниц ошибок.
func TestStoreEmptyBodyKeepsHash(t *testing.T) {
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"empty-200.test": {status: 200},
		"empty-400.test": {status: 400},
		"empty-404.test": {status: 404},
		"empty-500.test": {status: 500},
	})
	w := workerWorker(t, fetch, nil)
	domains := []string{"empty-200.test", "empty-400.test", "empty-404.test", "empty-500.test"}

	// Хэш пустых байтов один у всех пустых страниц — HashOf(nil) ==
	// HashOf([]byte{}) (контракт T-041), классы эквивалентности пустых
	// страниц строятся по нему же.
	emptyHash := content.HashOf(nil)

	// process — свежие результаты доменов: Save мутирует записи (тела
	// отпускаются), каждая ветка хранилища прогоняет свой проход.
	process := func(t *testing.T) []*DomainResult {
		t.Helper()
		results := make([]*DomainResult, len(domains))
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
			if rec.Body == nil {
				t.Fatalf("%s: тело записи nil — пустое тело обязано быть валидным (T-138)", name)
			}
			results[i] = res
		}
		return results
	}

	for _, tc := range []struct {
		name string
		boot func() (content.ContentStore, *storeFake, *batchStoreFake)
	}{
		{"построчный Put", func() (content.ContentStore, *storeFake, *batchStoreFake) {
			st := newStoreFake()
			return st, st, nil
		}},
		{"пачка PutBatch", func() (content.ContentStore, *storeFake, *batchStoreFake) {
			st := newStoreFake()
			b := &batchStoreFake{storeFake: st}
			return b, st, b
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, st, batch := tc.boot()
			results := process(t)
			var logBuf bytes.Buffer
			saver := storeSaver(t, store, &logBuf)
			for _, res := range results {
				if err := saver.Save(context.Background(), res); err != nil {
					t.Fatalf("Save(%s): %v", res.Name, err)
				}
			}
			if strings.Contains(logBuf.String(), "тело записи недоступно") {
				t.Errorf("предохранитель погасил пустое тело:\n%s", logBuf.String())
			}

			// Пустое тело доходит до хранилища каждого домена (Put или
			// элемент пачки), блок один на все пустые страницы.
			puts, writes, refs := st.snapshot()
			if batch == nil {
				if puts != len(results) {
					t.Fatalf("Put: вызовов %d, ожидалось %d", puts, len(results))
				}
			} else {
				if len(batch.batches) != len(results) {
					t.Fatalf("пачек %d, ожидалось %d (по одной на доставку)", len(batch.batches), len(results))
				}
				for i, items := range batch.batches {
					if len(items) != 1 || len(items[0]) != 0 {
						t.Fatalf("пачка %d: %d элементов, длина первого %d — ожидалось одно пустое тело", i, len(items), len(items[0]))
					}
				}
			}
			if writes != 1 {
				t.Fatalf("новых блоков %d, ожидался 1 (пустые страницы — один блок)", writes)
			}
			if refs[emptyHash] != len(results) {
				t.Errorf("refcount пустого блока: %d, ожидался %d", refs[emptyHash], len(results))
			}
			for _, res := range results {
				rec := &res.Subdomains[0].Records[0]
				if rec.ContentHash == nil || rec.ContentNotStored {
					t.Errorf("%s: после Save hash=%v not_stored=%v — ссылка на контент потеряна", res.Name, rec.ContentHash, rec.ContentNotStored)
				}
				if rec.Body != nil {
					t.Errorf("%s: тело удерживается после Save", res.Name)
				}
			}
			if got, err := st.Get(context.Background(), emptyHash); err != nil || len(got) != 0 {
				t.Errorf("Get пустого блока: %v, байтов %d (ожидалось 0)", err, len(got))
			}
		})
	}
}

// TestStoreEmptyBodyCachePath — кэш итоговых URL (§5.5 п. 6) с пустой
// страницей: кэш-итог несёт пустое тело как валидное (не nil, T-138),
// запись кэш-попадания становится оригиналом класса (T-055) и сохраняется
// Put-ом — content_hash не гасится ни у кэш-записи, ни у алиаса-соседа.
func TestStoreEmptyBodyCachePath(t *testing.T) {
	old := storePageBody("old")
	fetch, log := workerFakeFetch(map[string]*workerPageScript{
		"example.com": { // обе проверки bare: редирект на пустую www
			status: 301, final: "https://www.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "https://example.com/", Status: 301}},
		},
		"https://www.example.com/": {status: 200}, // пустая страница
		"http://www.example.com/": { // другой сайт — вариант 1
			status: 200, body: old, final: "http://old.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "http://www.example.com/", Status: 301}},
		},
	})
	w := workerWorker(t, fetch, func(o *Options) { o.PerDomain = 1 })
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
	// Оригинал класса (www v0, кэш-запись) несёт пустое тело для Save.
	wwwHome := &www.Records[0]
	if wwwHome.ContentHash == nil || *wwwHome.ContentHash != content.HashOf(nil) {
		t.Fatalf("кэш-запись: hash=%v, ожидался хэш пустого тела", wwwHome.ContentHash)
	}
	if wwwHome.Body == nil {
		t.Fatal("кэш-запись: тело nil — кэш-итог обязан нести пустое тело (T-138)")
	}

	st := newStoreFake()
	var logBuf bytes.Buffer
	if err := storeSaver(t, st, &logBuf).Save(context.Background(), res); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if strings.Contains(logBuf.String(), "тело записи недоступно") {
		t.Errorf("предохранитель погасил пустое тело кэш-записи:\n%s", logBuf.String())
	}

	// Два блока — пустая страница (оригинал класса bare+www) и старый
	// сайт; хэш пустого тела жив у оригинала, алиас ссылается через
	// alias_subdomain_id.
	puts, writes, refs := st.snapshot()
	emptyHash, oldHash := content.HashOf(nil), content.HashOf([]byte(old))
	if puts != 2 || writes != 2 {
		t.Fatalf("Put: вызовов %d, новых блоков %d — ожидалось 2 и 2", puts, writes)
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

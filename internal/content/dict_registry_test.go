// Тесты реестра прежних словарей zstd (T-103): читаемость старых паков
// после переобучения (ленивая до-регистрация dict-<id>.zstd по dictID
// кадра), memoизация, ясная ошибка отсутствующего словаря, работа
// читателя без активного файла словаря.

package content

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// dictCorpusAlt — второй детерминированный корпус иного шаблона
// («заглушка хостинга»): другой набор повторяющихся сегментов — другой
// ID словаря (FNV содержимого), что и нужно сценариям переобучения.
func dictCorpusAlt(n, pageSize int) [][]byte {
	head := `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">` +
		`<meta name="viewport" content="width=device-width, initial-scale=1">` +
		`<title>Suspended domain</title><link rel="stylesheet" href="/hosting.css">` +
		`<script src="/vendor/jquery.min.js"></script></head><body>` +
		`<div id="wrap"><header class="hdr"><nav class="crumbs">` +
		`<a href="/en">Home</a><a href="/faq">FAQ</a><a href="/abuse">Abuse</a>` +
		`</nav></header><main><section class="notice"><h1>Account suspended</h1>`
	foot := `</section></main><aside class="side"><p>Hosting Inc.</p>` +
		`<p>contact support@example.invalid</p></aside></div>` +
		`<footer class="ftr"><p>&copy; Hosting Inc. All rights reserved.</p>` +
		`</footer></body></html>`
	rng := rand.New(rand.NewSource(7))
	midSize := pageSize - len(head) - len(foot)
	if midSize < 1 {
		midSize = 1
	}
	pages := make([][]byte, n)
	for i := 0; i < n; i++ {
		m := make([]byte, midSize)
		rng.Read(m)
		pages[i] = append(append([]byte(head), m...), foot...)
	}
	return pages
}

// trainTwoDicts — пара словарей разных шаблонов (старый/новый): гарантия
// разных ID — предусловие сценариев переобучения.
func trainTwoDicts(t *testing.T) (oldD, newD *Dict) {
	t.Helper()
	var err error
	if oldD, err = TrainDict(dictCorpus(12, 2048), TrainOptions{DictSize: 16 << 10}); err != nil {
		t.Fatalf("TrainDict (старый): %v", err)
	}
	if newD, err = TrainDict(dictCorpusAlt(12, 2048), TrainOptions{DictSize: 16 << 10}); err != nil {
		t.Fatalf("TrainDict (новый): %v", err)
	}
	if oldD.ID() == newD.ID() {
		t.Fatalf("корпуса разных шаблонов дали одинаковый ID %08x — тест не проверяет реестр", oldD.ID())
	}
	return oldD, newD
}

// newDictRegistryStore — PackStore с фейковым пулом в общем каталоге
// со словарями (как в бою: content.dir содержит и паки, и dict.zst,
// и файлы реестра).
func newDictRegistryStore(t *testing.T) (*PackStore, *packFakePool, string) {
	t.Helper()
	dir := t.TempDir()
	pool := newPackFakePool()
	st, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	t.Cleanup(func() { _, _ = SetDictFile("", nil) }) // сброс активного словаря и реестра
	return st, pool, dir
}

// TestFrameDictID — разбор dictID из заголовка кадра: кадр со словарём
// называет его ID, кадр без словаря и мусор — 0.
func TestFrameDictID(t *testing.T) {
	d, err := TrainDict(dictCorpus(12, 2048), TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}
	page := dictCorpus(1, 2048)[0]
	if got := frameDictID(d.Compress(page)); got != d.ID() {
		t.Errorf("frameDictID(кадр со словарём) = %08x, хочу %08x", got, d.ID())
	}
	if got := frameDictID(Compress(page)); got != 0 {
		t.Errorf("frameDictID(кадр без словаря) = %08x, хочу 0", got)
	}
	frame := d.Compress(page)
	for _, bad := range [][]byte{nil, []byte("мусор"), frame[:6]} {
		if got := frameDictID(bad); got != 0 {
			t.Errorf("frameDictID(%d байт) = %08x, хочу 0", len(bad), got)
		}
	}
}

// TestDictRegistryOldPacksReadable — сценарий переобучения целиком:
// контент, сжатый старым словарём, читается через реестр после замены
// активного; новый пишется новым; чтение memoизируется; работает и без
// активного файла словаря (реестр подключён путём конфигурации).
func TestDictRegistryOldPacksReadable(t *testing.T) {
	oldD, newD := trainTwoDicts(t)
	st, pool, dir := newDictRegistryStore(t)
	ctx := context.Background()
	dictPath := filepath.Join(dir, "dict.zst")

	// Эпизод 1: активен старый словарь, контент записан им.
	if err := oldD.Save(dictPath); err != nil {
		t.Fatalf("Save (старый): %v", err)
	}
	if ok, err := SetDictFile(dictPath, nil); err != nil || !ok {
		t.Fatalf("SetDictFile (старый): ok=%v err=%v", ok, err)
	}
	page1 := dictCorpus(1, 2048)[0]
	put1, err := st.Put(ctx, page1)
	if err != nil {
		t.Fatalf("Put (старым словарём): %v", err)
	}
	// Блок действительно сжат старым словарём — иначе реестр не при чём.
	if row, ok := pool.row(put1.Hash); ok {
		frame, err := readPackBlock(dir, row.packID, row.off, row.clen)
		if err != nil {
			t.Fatalf("чтение блока: %v", err)
		}
		if id := frameDictID(frame); id != oldD.ID() {
			t.Fatalf("блок сжат словарём %08x, хочу старый %08x", id, oldD.ID())
		}
	} else {
		t.Fatal("строка метаданных не найдена")
	}

	// Переобучение: прежний словарь — в реестр (RetireDict, как делает
	// crawlerctl dict train), новый — активный.
	if err := RetireDict(oldD, dir); err != nil {
		t.Fatalf("RetireDict: %v", err)
	}
	if err := newD.Save(dictPath); err != nil {
		t.Fatalf("Save (новый): %v", err)
	}
	if ok, err := SetDictFile(dictPath, nil); err != nil || !ok {
		t.Fatalf("SetDictFile (новый): ok=%v err=%v", ok, err)
	}

	// Новый контент пишется новым словарём; старый читается через реестр.
	page2 := dictCorpusAlt(1, 2048)[0]
	put2, err := st.Put(ctx, page2)
	if err != nil {
		t.Fatalf("Put (новым словарём): %v", err)
	}
	for _, tc := range []struct {
		name string
		h    Hash
		want []byte
	}{
		{"старый контент через реестр", put1.Hash, page1},
		{"новый контент активным", put2.Hash, page2},
	} {
		got, err := st.Get(ctx, tc.h)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Errorf("%s: байты изменились", tc.name)
		}
	}

	// Memoизация: после первого чтения словарь в процессе — удаление
	// файла реестра не ломает чтение.
	if err := os.Remove(DictRegistryPath(dir, oldD.ID())); err != nil {
		t.Fatal(err)
	}
	if got, err := st.Get(ctx, put1.Hash); err != nil || !bytes.Equal(got, page1) {
		t.Errorf("чтение после удаления файла реестра: %v (memoизация не сработала)", err)
	}
	// Файл возвращается: следующий сценарий переустанавливает словарь
	// (SetDictFile сбрасывает memoизацию — как рестарт процесса).
	if err := RetireDict(oldD, dir); err != nil {
		t.Fatal(err)
	}

	// Активного файла нет (не обучен/удалён), путь конфигурации на месте:
	// реестр подключён, старые кадры читаются; кадры незарегистрированного
	// нового словаря — ясная ошибка реестра.
	if err := os.Remove(dictPath); err != nil {
		t.Fatal(err)
	}
	if ok, err := SetDictFile(dictPath, nil); err != nil || ok {
		t.Fatalf("SetDictFile без файла: ok=%v err=%v, хочу (false, nil)", ok, err)
	}
	if got, err := st.Get(ctx, put1.Hash); err != nil || !bytes.Equal(got, page1) {
		t.Errorf("чтение старого контента без активного словаря: %v", err)
	}
	_, err = st.Get(ctx, put2.Hash)
	if !errors.Is(err, zstd.ErrUnknownDictionary) {
		t.Errorf("новый словарь не в реестре: err = %v, хочу zstd.ErrUnknownDictionary", err)
	}
}

// TestDictRegistryMissingDict — прежний словарь не архивирован (файл
// реестра отсутствует): ошибка чтения называет реестр и сохраняет
// прежние сентинелы (zstd.ErrUnknownDictionary внутри ErrCorrupted).
func TestDictRegistryMissingDict(t *testing.T) {
	oldD, newD := trainTwoDicts(t)
	st, _, dir := newDictRegistryStore(t)
	ctx := context.Background()
	dictPath := filepath.Join(dir, "dict.zst")

	if err := oldD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}
	page := dictCorpus(1, 2048)[0]
	put, err := st.Put(ctx, page)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Переобучение «вручную»: новый словарь активен, старый НЕ архивирован.
	if err := newD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}

	_, err = st.Get(ctx, put.Hash)
	if err == nil {
		t.Fatal("чтение без словаря в реестре: ожидалась ошибка")
	}
	if !errors.Is(err, zstd.ErrUnknownDictionary) {
		t.Errorf("err = %v, хочу обёртку zstd.ErrUnknownDictionary", err)
	}
	if !errors.Is(err, ErrCorrupted) {
		t.Errorf("err = %v, прежняя обёртка ErrCorrupted потеряна", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "реестр") || !strings.Contains(msg, DictRegistryPath(dir, oldD.ID())) {
		t.Errorf("ошибка не называет реестр: %s", msg)
	}

	// Файл, положенный в реестр на живом процессе, подхватывается без
	// рестарта (отрицательный результат не memoизируется).
	if err := RetireDict(oldD, dir); err != nil {
		t.Fatal(err)
	}
	if got, err := st.Get(ctx, put.Hash); err != nil || !bytes.Equal(got, page) {
		t.Errorf("чтение после возврата файла в реестр: %v", err)
	}
}

// TestDictRegistryNameMismatch — файл реестра с чужим содержимым (имя
// dict-<id>.zstd не соответствует dictID внутри): ясная ошибка, а не
// сухая «unknown dictionary» от декодера.
func TestDictRegistryNameMismatch(t *testing.T) {
	oldD, newD := trainTwoDicts(t)
	st, _, dir := newDictRegistryStore(t)
	ctx := context.Background()
	dictPath := filepath.Join(dir, "dict.zst")

	if err := oldD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}
	page := dictCorpus(1, 2048)[0]
	put, err := st.Put(ctx, page)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Архив под чужим именем: файл словаря newD назван ID oldD.
	if err := os.WriteFile(DictRegistryPath(dir, oldD.ID()), newD.Raw(), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := newD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}

	_, err = st.Get(ctx, put.Hash)
	if !errors.Is(err, zstd.ErrUnknownDictionary) {
		t.Fatalf("err = %v, хочу zstd.ErrUnknownDictionary", err)
	}
	if !strings.Contains(err.Error(), "имя файла не соответствует") {
		t.Errorf("ошибка не объясняет несоответствие файла реестра: %v", err)
	}
}

// TestDictRegistryLazyLoadOnce — ленивая загрузка один раз: файл реестра
// подменяется мусором после первого чтения — memoизация держит словарь.
func TestDictRegistryLazyLoadOnce(t *testing.T) {
	oldD, newD := trainTwoDicts(t)
	st, _, dir := newDictRegistryStore(t)
	ctx := context.Background()
	dictPath := filepath.Join(dir, "dict.zst")

	if err := oldD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}
	page := dictCorpus(1, 2048)[0]
	put, err := st.Put(ctx, page)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := RetireDict(oldD, dir); err != nil {
		t.Fatal(err)
	}
	if err := newD.Save(dictPath); err != nil {
		t.Fatal(err)
	}
	if _, err := SetDictFile(dictPath, nil); err != nil {
		t.Fatal(err)
	}

	if got, err := st.Get(ctx, put.Hash); err != nil || !bytes.Equal(got, page) {
		t.Fatalf("первое чтение через реестр: %v", err)
	}
	// Порча файла реестра не видна процессу: словарь уже загружен.
	if err := os.WriteFile(DictRegistryPath(dir, oldD.ID()), []byte("мусор"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, err := st.Get(ctx, put.Hash); err != nil || !bytes.Equal(got, page) {
		t.Errorf("повторное чтение при испорченном файле реестра: %v (загрузка не memoизирована?)", err)
	}
}

// dictCraftHeaderPack — пак из одного заголовка с заданным dictID
// (диагностика WarnDictMismatch).
func dictCraftHeaderPack(t *testing.T, dir string, id int32, dictID uint32) {
	t.Helper()
	hdr := packHeader{id: id, maxSize: 1 << 20, dictID: dictID}
	if err := os.WriteFile(filepath.Join(dir, packFileName(id)), hdr.bytes(), 0o644); err != nil {
		t.Fatalf("сборка пака %d: %v", id, err)
	}
}

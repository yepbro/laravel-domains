// Групповая фиксация записей контента (T-102; отложенная находка C-15).
//
// Put платит за каждый блок отдельный fsync и отдельный запрос к БД под
// flock — цена инвариантов C-1. Доставка воркера сохраняет несколько
// страниц сразу (записи-оригиналы одного домена, T-056; до 126 записей),
// поэтому PutBatch дописывает блоки пачки и фиксирует метаданные всех
// строк одной групповой фиксацией: один fsync и один пакетный INSERT на
// пачку вместо по-блочных. Инвариант C-1 не ослабляется:
//
//   - байты раньше строк: единственный fsync пачки выполняется ДО
//     пакетного INSERT — строка не может зафиксироваться раньше своих
//     байтов на диске (паки, закрытые ротацией внутри пачки,
//     синхронизируются при ротации — closeActiveLocked, как у пачек
//     компактификации C-11);
//   - пак не удаляется раньше фиксации его живых строк: пакетный INSERT
//     выполняется под flock, взятым до первого дописывания блока пачки.
//     Компактификация удаляет паки под той же блокировкой (T-045) —
//     окно «ротация → unlink → INSERT» закрыто взаимным исключением,
//     а не временем: пометки «пак в записи» и grace-периоды в
//     компактификации не вводятся (межпроцессная пометка требовала бы
//     файла с собственной crash-семантикой; ошибка там — тот же класс
//     потери, что C-1, а выигрыш против одного INSERT на пачку минимален).
//
// Сбой пачки безопасен и идемпотентен при повторе (ТЗ 3.4): пакетный
// INSERT атомарен — строк либо нет всех, либо все на месте; крах до
// INSERT оставляет блоки мусором без строк (невидимы, вычистит
// компактификация T-045); дедуп-ссылки, добавленные до сбоя, остаются —
// утечка в безопасную сторону (тот же выбор, что у Put и SwapRef).
package content

import (
	"context"
	"errors"
	"fmt"
)

// SQL групповой фиксации. Точные тексты закреплены тестами (batch_test.go,
// стиль pack_test.go).
const (
	// Дедуп-пачка: живой контент пачки — только ссылки, байты не пишутся
	// (пачковый аналог sqlMetaAddRef: одна команда на пачку вместо
	// построчных UPDATE). Мёртвые строки (refcount = 0) не воскресаются:
	// их блоки могли быть скомпактифицированы, новые Put пишут свежие байты.
	sqlMetaAddRefBatch = "UPDATE contents SET refcount = refcount + 1 " +
		"WHERE hash = ANY($1::bytea[]) AND refcount > 0 RETURNING hash"

	// Пакетная фиксация новых блоков пачки (пачковый аналог sqlMetaPut):
	// unnest-массивы; хэши в пачке уникальны (дубликаты одного хэша
	// сведены в одну строку со счётчиком n — ON CONFLICT не может
	// затронуть одну строку дважды, а refcount = n сохраняет семантику
	// «каждый Put добавляет ссылку»). Конфликт — конкурент успел раньше
	// (блоки пачки — мусор) или воскрешение мёртвой строки: размещение
	// переводится на блоки пачки (EXCLUDED), как у построчного upsert.
	// RETURNING hash + (xmax = 0): порядок строк RETURNING не определён,
	// созданность различается по хэшу.
	sqlMetaPutBatch = "INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) " +
		"SELECT hash, pack_id, off, len, raw_len, n " +
		"FROM unnest($1::bytea[], $2::int4[], $3::int8[], $4::int4[], $5::int4[], $6::int4[]) " +
		"AS v(hash, pack_id, off, len, raw_len, n) " +
		"ON CONFLICT (hash) DO UPDATE SET refcount = contents.refcount + EXCLUDED.refcount, " +
		"pack_id = EXCLUDED.pack_id, off = EXCLUDED.off, " +
		"len = EXCLUDED.len, raw_len = EXCLUDED.raw_len " +
		"RETURNING hash, (xmax = 0) AS created"
)

// BatchStore — необязательное расширение ContentStore групповой фиксацией
// (T-102): потребитель, сохраняющий несколько контентов сразу (доставка
// воркера), фиксирует их пачкой — один fsync и один пакетный INSERT
// метаданных на пачку вместо по-блочных (см. комментарий файла).
// Реализация — (*PackStore).PutBatch; потребители обнаруживают её
// утверждением типа (ContentSaver httpworker, T-056) и обязаны корректно
// работать с хранилищами без пачки (построчный Put).
type BatchStore interface {
	ContentStore

	// PutBatch сохраняет элементы items одной групповой фиксацией и
	// возвращает результаты по элементам в порядке items. Контракт:
	//
	//   - errs[i] != nil ⟺ элемент i не сохранён индивидуально:
	//     ErrTooLarge (сверх max_store_bytes, ТЗ 5.5) или ErrStoreFull
	//     (пауза записи контента, ТЗ 8: байты элемента не писались);
	//     остальные элементы пачки при этом сохранены — дедуп-ссылки на
	//     живой контент проходят и при переполнении;
	//   - err != nil ⟺ сбой пачки (ошибка БД/ФС или отмена ctx): новые
	//     строки не зафиксированы, results/errs не определены; повторная
	//     доставка переигрывает пачку идемпотентно по хэшу (дедуп и
	//     upsert, ТЗ 3.4);
	//   - при err == nil len(results) = len(errs) = len(items), и
	//     results[i].Hash = HashOf(items[i]) — контракт T-041;
	//     PutResult.Created — «записаны новые байты», false у
	//     дедупликаций и повторов хэша внутри пачки.
	//
	// Дубликаты одного контента внутри пачки допустимы: байты пишутся
	// один раз, каждая копия добавляет ссылку (как последовательные Put).
	// Пустая пачка и пачка из одних дедупликаций не занимает блокировок
	// и не делает fsync.
	PutBatch(ctx context.Context, items [][]byte) (results []PutResult, errs []error, err error)
}

// compile-time: PackStore реализует групповую фиксацию (T-102).
var _ BatchStore = (*PackStore)(nil)

// batchItem — элемент пачки после фильтров: хэш, индексы элементов
// оригинального items с этим хэшем (первый — носитель байтов), счётчик
// Put-ов этого хэша в пачке (refcount вставляемой строки) и размер
// несжатых байтов (contents.raw_len).
type batchItem struct {
	hash   Hash
	idxs   []int
	count  int32
	rawLen int32
}

// PutBatch — групповая фиксация, контракт — в BatchStore. Шаги:
// дедуп-пачка (одно UPDATE) → пауза диска → сжатие → дописывание всех
// блоков, один fsync и один пакетный INSERT под одним захватом мьютекса
// и flock (инварианты C-1, комментарий файла).
func (s *PackStore) PutBatch(ctx context.Context, items [][]byte) ([]PutResult, []error, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if s.opts.ReadOnly {
		return nil, nil, errReadOnly
	}
	results := make([]PutResult, len(items))
	errs := make([]error, len(items))

	// 0) хэши, лимиты, внутри-пачковые дубликаты: один хэш — одна строка
	// со счётчиком ссылок (байты пишутся один раз, каждый Put элемента
	// добавляет ссылку — семантика последовательных Put).
	all := make([]*batchItem, 0, len(items))
	byHash := make(map[Hash]*batchItem, len(items))
	for i, data := range items {
		if s.opts.MaxStoreBytes > 0 && int64(len(data)) > s.opts.MaxStoreBytes {
			errs[i] = fmt.Errorf("%w: %d байт при лимите %d",
				ErrTooLarge, len(data), s.opts.MaxStoreBytes)
			continue
		}
		h := HashOf(data)
		if it, ok := byHash[h]; ok {
			it.idxs = append(it.idxs, i)
			it.count++
			continue
		}
		it := &batchItem{hash: h, idxs: []int{i}, count: 1, rawLen: int32(len(data))}
		byHash[h] = it
		all = append(all, it)
	}
	if len(all) == 0 {
		return results, errs, nil
	}

	// 1) дедуп-пачка: живой контент — только ссылки (одно обращение к БД
	// на пачку), байты не пишутся (sqlMetaAddRefBatch).
	live, err := s.addRefBatch(ctx, all)
	if err != nil {
		return nil, nil, err
	}
	remain := make([]*batchItem, 0, len(all))
	for _, it := range all {
		if live[it.hash] {
			for _, i := range it.idxs {
				results[i] = PutResult{Hash: it.hash} // Created=false: дедуп
			}
			continue
		}
		remain = append(remain, it)
	}
	if len(remain) == 0 {
		return results, errs, nil
	}

	// 2) пауза записи контента (ТЗ 8, T-046): байты пачки не пишутся,
	// элементы — по-элементный ErrStoreFull (дедупы выше уже вернулись:
	// ссылки на живой контент проходят и при переполнении).
	if err := s.pauseIfDiskFull(); err != nil {
		for _, it := range remain {
			for _, i := range it.idxs {
				errs[i] = err
			}
		}
		return results, errs, nil
	}

	// 3) сжатие до захвата блокировок: CPU-работа не удлиняет критическую
	// секцию (flock держится от дописывания до фиксации строк).
	blocks := make([][]byte, len(remain))
	for i, it := range remain {
		blocks[i] = CompressWithDict(items[it.idxs[0]])
	}

	// 4–6) дописывание всех блоков, один fsync и пакетный INSERT — под
	// одним захватом мьютекса и flock (putBatchCommit, инварианты C-1).
	created, err := s.putBatchCommit(ctx, remain, blocks)
	if err != nil {
		// ENOSPC при физической записи — событие алерта (ТЗ 8), как у Put.
		if errors.Is(err, ErrStoreFull) {
			s.fireStoreFull(s.enospcEvent())
		}
		return nil, nil, err
	}
	for _, it := range remain {
		results[it.idxs[0]] = PutResult{Hash: it.hash, Created: created[it.hash]}
		for _, i := range it.idxs[1:] {
			results[i] = PutResult{Hash: it.hash} // повтор хэша в пачке: байты уже есть
		}
	}
	return results, errs, nil
}

// addRefBatch — дедуп-пачка (шаг 1): живому контенту добавляются ссылки
// без записи байтов, одна команда на пачку. Возвращает множество хэшей,
// получивших ссылку.
func (s *PackStore) addRefBatch(ctx context.Context, items []*batchItem) (map[Hash]bool, error) {
	hashes := make([][]byte, 0, len(items))
	for _, it := range items {
		hashes = append(hashes, it.hash[:])
	}
	rows, err := s.pool.Query(ctx, sqlMetaAddRefBatch, hashes)
	if err != nil {
		return nil, fmt.Errorf("content: метаданные пачки (%d хэшей): %w", len(hashes), err)
	}
	defer rows.Close()
	live := make(map[Hash]bool, len(items))
	for rows.Next() {
		h, err := scanHashRow(rows)
		if err != nil {
			return nil, fmt.Errorf("content: метаданные пачки: %w", err)
		}
		live[h] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("content: метаданные пачки: %w", err)
	}
	return live, nil
}

// scanHashRow — первая колонка строки как Hash (RETURNING hash пакетных
// операторов): контроль длины против повреждённых данных.
func scanHashRow(rows rowScanner) (Hash, error) {
	var hb []byte
	if err := rows.Scan(&hb); err != nil {
		return Hash{}, err
	}
	if len(hb) != HashLen {
		return Hash{}, fmt.Errorf("хэш %x: %d октетов, ожидается %d", hb, len(hb), HashLen)
	}
	var h Hash
	copy(h[:], hb)
	return h, nil
}

// rowScanner — минимальный интерфейс чтения строки (pgx.Rows и его
// подстановки в тестах).
type rowScanner interface {
	Scan(dest ...any) error
}

// putBatchCommit — шаги 4–6 групповой фиксации: дописывает блоки в
// новейший пак, делает единственный fsync пачки и фиксирует все строки
// одним пакетным INSERT — под одним захватом мьютекса и flock от
// дописывания до фиксации (инвариант «пак не удаляется раньше фиксации
// его живых строк»; fsync до INSERT — «байты раньше строк»). Возвращает
// отображение «хэш → создан ли» (RETURNING xmax = 0).
func (s *PackStore) putBatchCommit(ctx context.Context, items []*batchItem, blocks [][]byte) (map[Hash]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errPackClosed
	}
	if err := flockDir(s.lock); err != nil {
		return nil, err
	}
	defer func() { unlockDir(s.lock) }()

	type placed struct {
		it     *batchItem
		packID int32
		off    int64
		clen   int32
	}
	written := make([]placed, 0, len(items))
	for i, it := range items {
		packID, off, err := s.appendBlockLocked(blocks[i])
		if err != nil {
			return nil, err
		}
		written = append(written, placed{it: it, packID: packID, off: off, clen: int32(len(blocks[i]))})
	}
	// Единственный fsync пачки: байты всех блоков — на диске до фиксации
	// любой строки пачки (C-1); паки, закрытые ротацией внутри пачки,
	// синхронизированы closeActiveLocked при ротации.
	if err := s.syncActiveLocked(); err != nil {
		return nil, err
	}

	hashes := make([][]byte, 0, len(written))
	packs := make([]int32, 0, len(written))
	offs := make([]int64, 0, len(written))
	clens := make([]int32, 0, len(written))
	rawLens := make([]int32, 0, len(written))
	counts := make([]int32, 0, len(written))
	for _, w := range written {
		hashes = append(hashes, w.it.hash[:])
		packs = append(packs, w.packID)
		offs = append(offs, w.off)
		clens = append(clens, w.clen)
		rawLens = append(rawLens, w.it.rawLen)
		counts = append(counts, w.it.count)
	}
	rows, err := s.pool.Query(ctx, sqlMetaPutBatch, hashes, packs, offs, clens, rawLens, counts)
	if err != nil {
		return nil, fmt.Errorf("content: метаданные пачки (%d строк): %w", len(written), err)
	}
	defer rows.Close()
	created := make(map[Hash]bool, len(written))
	for rows.Next() {
		var hb []byte
		var cr bool
		if err := rows.Scan(&hb, &cr); err != nil {
			return nil, fmt.Errorf("content: метаданные пачки: %w", err)
		}
		if len(hb) != HashLen {
			return nil, fmt.Errorf("content: метаданные пачки: хэш %x: %d октетов, ожидается %d",
				hb, len(hb), HashLen)
		}
		var h Hash
		copy(h[:], hb)
		created[h] = cr
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("content: метаданные пачки: %w", err)
	}
	if len(created) != len(written) {
		return nil, fmt.Errorf("content: метаданные пачки: RETURNING дал %d строк, ожидалось %d",
			len(created), len(written))
	}
	return created, nil
}

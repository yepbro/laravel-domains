// Сверка контента результата домена с хранилищем (T-056/T-147; ТЗ 5.5
// п. 8, §5.7, §8, §11.4 п. 3). Тела страниц сохраняются РАННИМ Put —
// воркер кладёт каждую скачанную страницу в ContentStore сразу по
// получении (worker.go checkScheme, T-147) и не удерживает байты в
// памяти слота; дедупликация алиасов (T-055) идёт по хэшам. ContentSaver
// вызывается в Options.OnResult (сборка cmd/http-worker) до записи
// результата (Recorder, T-058) и XACK (ТЗ 4.2) и решает судьбу ссылок
// ранних Put:
//
//   - запись-оригинал класса (StoreContent, T-055) с несокращённым
//     телом и прошедшим Put наследует ссылку: content_hash записи —
//     это ссылка из Put, строка subdomain_results вступает во владение
//     (refcount не меняется; своп при перезаписи снимет, T-044/T-058);
//   - ссылки, не ставшие чьей-либо строкой, отпускаются Delete:
//     свёрнутые вариант-логикой проверки (оба протокола — один сайт),
//     проигравшие классы алиасов (контент хранится один раз, ТЗ 5.5
//     п. 6), повторные скачивания одного контента. Без свертки
//     refcount систематически рос бы — ранний Put не знает, кем
//     станут его страницы;
//   - алиасы собственной ссылки не получают: content_hash
//     нормализуется в nil (контент класса доступен через
//     alias_subdomain_id оригинала, T-055);
//   - обрезанное тело (Truncated, T-049) раннего Put не получало —
//     хэш снимается, ставится ContentNotStored;
//   - страницы, чей Put не прошёл (ErrStoreFull — пауза записи
//     контента, ТЗ 8/T-046; ErrTooLarge — лимит max_store_bytes),
//     помечены воркером ContentStored=false: запись живёт с
//     content_hash = nil и ContentNotStored — результат домена НЕ
//     роняется (§8: пауза записи контента, не результатов; повторная
//     проверка домена recheck T-030 скачает и сохранит). Warn пишет
//     воркер при получении ответа.
//
// Дедупликация остаётся двухуровневой (§8): уровень задачи — кэш
// итоговых URL и классы DetectAliases (сравнение по хэшам), уровень
// хранилища — Put/PutBatch по хэшу (T-041/T-042): одинаковые страницы
// разных доменов лежат одним блоком, refcount растёт, новые байты не
// пишутся (Created = false, метрика доли дедупликации ТЗ 11.4 п. 3 —
// статистику created Save берёт из реестра ранних Put).
//
// Хэш — один и тот же источник: воркер вычисляет content.HashOf над
// UTF-8-нормализованным телом (T-051), ранний Put получает те же байты,
// и воркер сверяет HashOf(data) с возвратом Put (контракт T-041) до
// отбрасывания тела — рассинхронизация роняет задачу сразу (T-147).
//
// Цена раннего Put против пачки T-102: по одному fsync и INSERT на
// скачанную страницу вместо пачки на доставку. Принята сознательно
// (T-147): буферизация тел до конца задачи ради групповой фиксации и
// держала ≈3.34 МиБ/слот (замер T-092 §11) — память слота дороже
// лишних fsync (SSD держит тысячи в секунду; замер после деплоя —
// docs/load-measurements.md). PutBatch остаётся контрактом BatchStore
// (T-102) для потребителей, сохраняющих несколько контентов сразу.
//
// At-least-once (ТЗ 3.4): повторная доставка задачи переигрывает и
// скачивания, и ранние Put (дедуп: +1 без новых байтов), и сверку.
// Ссылки попытки, не дошедшей до Save (крах процесса, сбой Put, отмена
// ctx), остаются без строки — утечка места в безопасную сторону (тот
// же выбор, что у SwapRef T-044): блок остаётся живым, данные не
// теряются.

package httpworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"crawler/internal/content"
)

// ContentSaver — сверка контента записей результата домена с хранилищем
// (T-056/T-147). Подключается сборкой сервиса в Options.OnResult (хук
// вызывается в цикле Run до XACK, T-053) первым звеном цепочки — перед
// записью результата (Recorder, T-058): к моменту строки ссылки ранних
// Put уже распределены (владение — у строк, лишние — сняты):
//
//	saver, err := httpworker.NewContentSaver(store, logger)
//	…
//	opts.OnResult = func(ctx context.Context, res *httpworker.DomainResult) error {
//		if err := saver.Save(ctx, res); err != nil {
//			return err
//		}
//		_, err := recorder.RecordAndPublish(ctx, res) // T-058
//		return err
//	}
//
// Ошибка Save (кроме пропусков выше) — ошибка OnResult: доставка не
// подтверждается и вернётся повторной доставкой (ТЗ 3.4) — так же
// поведёт себя запись результата (Recorder, T-058) при сбое БД.
//
// Потокобезопасен: состояние неизменно после New; Save вызывается для
// результата одной задачи, конкурентных Save одних и тех же записей нет.
type ContentSaver struct {
	store content.ContentStore
	log   *slog.Logger
}

// NewContentSaver строит сверку контента поверх хранилища (продакшн —
// *content.PackStore, T-042; nil — ошибка: тихое «сохранение в никуда»
// теряло бы контент незаметно).
func NewContentSaver(store content.ContentStore, log *slog.Logger) (*ContentSaver, error) {
	if store == nil {
		return nil, errors.New("httpworker: сохранение контента: хранилище не задано")
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &ContentSaver{store: store, log: log}, nil
}

// Save распределяет ссылки ранних Put задачи (T-147) по записям
// результата домена:
//
//   - записи-алиасы контент не хранят — собственной ссылки в contents у
//     них нет, ContentHash нормализуется в nil (контент класса доступен
//     через alias_subdomain_id записи-оригинала, T-055);
//   - обрезанное тело (Truncated) и тело без прошедшего Put — контент
//     не сохранён, хэш снимается, ставится ContentNotStored (см.
//     комментарий файла); Warn о причине написал воркер при получении;
//   - оригинал с несокращённым сохранённым телом остаётся с хэшем —
//     ссылка его раннего Put становится ссылкой строки;
//   - ссылки ранних Put, на которые не вышло ни одной записи, снимаются
//     Delete (сверка реестра DomainResult.puts);
//   - записи, ссылающиеся на хэш без раннего Put, — рассинхронизация
//     воркера и хранилища: ссылка без счётчика ломала бы учёт
//     (SwapRefTx T-044, ErrRefIntegrity) — громкая ошибка, доставка без
//     XACK (ТЗ 3.4);
//   - отмена ctx и прочие ошибки Delete — ошибка Save: эпизод не
//     завершён, повторная доставка переиграет его целиком (ТЗ 3.4).
//
// Идемпотентность вызова не требуется (одна доставка — один вызов);
// повторная доставка задачи просто повторит скачивания, Put-ы и сверку.
func (s *ContentSaver) Save(ctx context.Context, res *DomainResult) error {
	if res == nil {
		return nil // не бывает (processDelivery); защита прямых вызовов
	}
	var stats saveStats
	claims := make(map[content.Hash]int, len(res.puts))

	// Проход 1 — нормализация записей: кому из них достанется ссылка
	// раннего Put (учёт требований — claims по хэшам).
	for i := range res.Subdomains {
		for j := range res.Subdomains[i].Records {
			r := &res.Subdomains[i].Records[j]
			if r.ContentHash == nil {
				continue // контента нет — нечего ни хранить, ни снимать
			}
			if !r.StoreContent {
				// Алиас класса (T-055): контент хранится один раз у
				// оригинала; собственная ссылка не создавалась —
				// content_hash записи NULL.
				r.ContentHash = nil
				continue
			}
			if r.Truncated {
				// Раннего Put не было (T-049): хэш усечённых байт
				// врал бы при дедупликации — не хранится.
				stats.truncated++
				r.ContentHash, r.ContentNotStored = nil, true
				continue
			}
			if !r.ContentStored {
				// Ранний Put не прошёл: ErrStoreFull (пауза записи
				// контента, ТЗ 8/T-046) или ErrTooLarge (лимит
				// max_store_bytes, ТЗ 5.5) — Warn у воркера; ссылку
				// записи не давать.
				stats.unstored++
				r.ContentHash, r.ContentNotStored = nil, true
				continue
			}
			claims[*r.ContentHash]++
		}
	}

	// Проход 2 — сверка реестра ранних Put (в порядке скачиваний):
	// заявленная записью ссылка достаётся ей, остальные снимаются.
	for i := range res.puts {
		p := &res.puts[i]
		if claims[p.hash] > 0 {
			claims[p.hash]--
			stats.stored++
			if p.created {
				stats.created++
			}
			continue
		}
		stats.released++
		if err := s.store.Delete(ctx, p.hash); err != nil {
			if errors.Is(err, content.ErrNotFound) {
				// Ссылку уже снял кто-то другой (гонка повторной
				// доставки той же задачи, T-025): счётчик целостности
				// не ломаем, Warn для разбора — блок мог остаться жив
				// другими ссылками.
				s.log.Warn("снятие ссылки раннего Put: ссылки уже нет (гонка повторной доставки?)",
					"domain", res.Name, "hash", p.hash.String())
				continue
			}
			return fmt.Errorf("httpworker: домен %s: снятие ссылки несохранённого контента %s: %w",
				res.Name, p.hash, err)
		}
	}

	// Остались незакрытые требования — записи ссылаются на хэш без
	// раннего Put: построение результата рассинхронизировано с
	// хранилищем, ссылка без счётчика уронила бы учёт при первом же
	// свопе (T-044). Лучше громкий сбой доставки, чем молчаливая порча.
	for h, n := range claims {
		if n > 0 {
			return fmt.Errorf("httpworker: домен %s: записи требуют %s ×%d без раннего Put (рассинхронизация воркера и хранилища)",
				res.Name, h, n)
		}
	}

	if stats.unstored > 0 {
		s.log.Warn("контент части записей не сохранён (пауза записи контента/лимит; Warn о причине — при получении страниц)",
			"domain", res.Name, "records", stats.unstored)
	}
	if stats.stored+stats.released+stats.unstored+stats.truncated > 0 {
		s.log.Debug("контент результата домена",
			"domain", res.Name, "stored", stats.stored, "created", stats.created,
			"released", stats.released, "skipped_unstored", stats.unstored, "skipped_truncated", stats.truncated)
	}
	return nil
}

// saveStats — счётчики исходов сверки одной доставки: stored — ссылки,
// ставшие записями; created — из них записавшие новые байты (доля
// дедупликации, ТЗ 11.4 п. 3); released — снятые лишние; unstored и
// truncated — записи без контента (пауза/лимит/обрезка).
type saveStats struct {
	stored, created, released, unstored, truncated int
}

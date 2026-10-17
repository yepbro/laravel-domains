// Сохранение контента результата домена в ContentStore (T-056; ТЗ 5.5
// п. 8, §5.7, §8, §11.4 п. 3). Новый файл — правки worker.go точечные
// (Record.Body у оригиналов); точка вызова — Options.OnResult в сборке
// сервиса (cmd/http-worker): сохранение идёт внутри обработки задачи,
// до XACK и до записи результата (Recorder, T-058; ТЗ 4.2).
//
// Дедупликация парковок и заглушек — двухуровневая (§8 «естественная
// дедупликация по хэшу»):
//
//   - уровень задачи домена (T-053/T-055): общий кэш «итоговый URL →
//     результат» не скачивает одну страницу дважды, а DetectAliases
//     собирает записи с одинаковым итоговым URL или одинаковым хэшем
//     контента в класс эквивалентности — контент класса сохраняется
//     один раз, у записи-оригинала (Record.StoreContent = true);
//   - уровень хранилища (T-041/T-042): ContentStore.Put дедуплицирует
//     по хэшу — одинаковые страницы разных доменов (парковки) лежат
//     одним блоком, refcount растёт, новые байты не пишутся
//     (Created = false, метрика доли дедупликации ТЗ 11.4 п. 3).
//
// Хэш — один и тот же источник: воркер вычисляет content.HashOf над
// UTF-8-нормализованным телом (T-051), Record.Body несёт те же байты,
// Put обязан вернуть HashOf(data) (контракт T-041) — совпадение
// проверяется после каждого Put.
//
// Поведение при переполнении диска (ErrStoreFull, ТЗ 8 «сервисы
// приостанавливают запись контента», T-046) и при превышении лимита
// max_store_bytes (ErrTooLarge, ТЗ 5.5): контент НЕ сохраняется, но
// результат домена НЕ роняется — запись живёт с content_hash = NULL и
// признаком Record.ContentNotStored (§8: пауза записи контента, не
// результатов). content_hash в subdomain_results — ссылка на contents
// (§6.1): без блока в хранилище строки нет, а ссылка без счётчика
// ломала бы учёт (SwapRef T-044, фоновая очистка T-020) — хэш
// сохраняется в записи только вместе с контентом. Повторная проверка
// домена (recheck, T-030) скачает страницу снова и сохранит контент.
//
// Обрезанное потолком max_body_bytes тело (Record.Truncated, T-049)
// не сохраняется вовсе и не даёт хэша для хранения: хэш усечённых
// байт врал бы при дедупликации (парковка-«префикс» склеилась бы с
// целой страницей другого домена); частичность данных уже отмечена
// Truncated/ErrBody (T-049/T-053).
//
// At-least-once (ТЗ 3.4): повторная доставка задачи переиграет и
// сохранение — повторный Put тех же байтов добавит ссылку (Created =
// false, новых байтов нет). Лишняя ссылка без записи результата —
// утечка места в безопасную сторону (тот же выбор, что у SwapRef
// T-044): блок остаётся живым, данные не теряются.

package httpworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"crawler/internal/content"
)

// ContentSaver — сохранение контента записей результата домена в
// ContentStore (T-056). Подключается сборкой сервиса в Options.OnResult
// (хук вызывается в цикле Run до XACK, T-053): первым звеном цепочки —
// перед записью результата (Recorder, T-058), чтобы новые content_hash
// уже имели +1 refcount от Put к свопу ссылок:
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
// Ошибка Save (кроме пропусков ниже) — ошибка OnResult: доставка не
// подтверждается и вернётся повторной доставкой (ТЗ 3.4) — так же
// поведёт себя запись результата (Recorder, T-058) при сбое БД.
//
// Потокобезопасен: состояние неизменно после New; Save вызывается для
// результата одной задачи, конкурентных Save одних и тех же записей нет.
type ContentSaver struct {
	store content.ContentStore
	log   *slog.Logger
}

// NewContentSaver строит сохранатель поверх хранилища контента
// (продакшн — *content.PackStore, T-042; nil — ошибка: тихое
// «сохранение в никуда» теряло бы контент незаметно).
func NewContentSaver(store content.ContentStore, log *slog.Logger) (*ContentSaver, error) {
	if store == nil {
		return nil, errors.New("httpworker: сохранение контента: хранилище не задано")
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &ContentSaver{store: store, log: log}, nil
}

// Save обходит записи результата домена и сохраняет контент оригиналов
// (StoreContent = true, ТЗ 5.5 п. 6 «контент хранится один раз»):
//
//   - записи-алиасы контент не хранят — собственной ссылки в contents у
//     них нет, ContentHash нормализуется в nil (контент класса доступен
//     через alias_subdomain_id записи-оригинала, T-055);
//   - обрезанное тело (Truncated) — контент не сохраняется, хэш снимается,
//     ставится ContentNotStored (см. комментарий файла);
//   - ErrStoreFull/ErrTooLarge от Put — контент не сохраняется, запись
//     живёт с content_hash = nil и ContentNotStored, Warn в лог; результат
//     домена не роняется (ТЗ 8);
//   - успех — ContentHash записи остаётся ссылкой на сохранённый блок
//     (сверка с PutResult.Hash), тело отбрасывается (Body = nil);
//   - прочие ошибки Put и отмена ctx — возврат ошибки: эпизод не
//     завершён, повторная доставка переиграет его целиком (ТЗ 3.4).
//
// Идемпотентность вызова не требуется (одна доставка — один вызов);
// повторная доставка задачи просто повторит сохранение (см. комментарий
// файла про лишние ссылки).
func (s *ContentSaver) Save(ctx context.Context, res *DomainResult) error {
	if res == nil {
		return nil // не бывает (processDelivery); защита прямых вызовов
	}
	var stored, created, full, tooLarge, truncated int
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
				// Хэш усечённых байт врал бы при дедупликации — не
				// сохраняется и не хэшируется для хранения.
				truncated++
				r.ContentHash, r.ContentNotStored = nil, true
				continue
			}
			if r.Body == nil {
				// Невозможно по построению ProcessDomain (StoreContent,
				// хэш и необрезанное тело ⇒ Body); защита от
				// рассинхронизации слоёв — запись не роняем.
				s.log.Warn("контент не сохранён: тело записи недоступно",
					"domain", res.Name, "subdomain", r.Label,
					"subdomain_id", r.SubdomainID, "variant", r.Variant)
				r.ContentHash, r.ContentNotStored = nil, true
				continue
			}

			pr, err := s.store.Put(ctx, r.Body)
			switch {
			case err == nil:
				// Контракт T-041: Put возвращает HashOf(data) — те же
				// байты, от которых воркер вычислил ContentHash.
				if pr.Hash != *r.ContentHash {
					return fmt.Errorf("httpworker: домен %s: хранилище вернуло хэш %s, воркер вычислил %s (контракт HashOf, T-041)",
						res.Name, pr.Hash, *r.ContentHash)
				}
				stored++
				if pr.Created {
					created++
				}
				r.Body = nil // байты больше не нужны: хэш записи ссылается на блок
			case errors.Is(err, content.ErrStoreFull):
				// Пауза записи контента (ТЗ 8, T-046): результат живёт
				// без контента, повторная проверка сохранит.
				full++
				r.ContentHash, r.ContentNotStored, r.Body = nil, true, nil
			case errors.Is(err, content.ErrTooLarge):
				// Тело сверх лимита max_store_bytes (ТЗ 5.5) — отвергнуто
				// хранилищем целиком.
				tooLarge++
				r.ContentHash, r.ContentNotStored, r.Body = nil, true, nil
			default:
				// Отмена ctx или сбой хранилища/БД: эпизод не завершён,
				// без XACK — повторная доставка (ТЗ 3.4).
				return fmt.Errorf("httpworker: домен %s: сохранение контента: %w", res.Name, err)
			}
		}
	}

	if full > 0 {
		s.log.Warn("контент не сохранён: хранилище переполнено (ErrStoreFull) — запись контента на паузе; повторная проверка домена сохранит страницы",
			"domain", res.Name, "records", full)
	}
	if tooLarge > 0 {
		s.log.Warn("контент не сохранён: тело сверх лимита max_store_bytes",
			"domain", res.Name, "records", tooLarge)
	}
	if stored+created+full+tooLarge+truncated > 0 {
		s.log.Debug("контент результата домена",
			"domain", res.Name, "stored", stored, "created", created,
			"skipped_full", full, "skipped_large", tooLarge, "skipped_truncated", truncated)
	}
	return nil
}

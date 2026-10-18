// Package analyzer — анализ сохранённого контента по правилам Laravel
// (ТЗ 5.8): пассивный, по уже скачанным страницам, cookies и заголовкам.
// Потребление q:analyze и запись laravel_findings — T-071 (worker.go),
// переанализ — T-074 (reanalyze.go).
//
// Этот файл — агрегация на уровне домена (T-072; ТЗ 5.8: «на уровне
// домена — агрегат (максимальный score, объединённый диапазон, маска
// субдоменов)», таблица laravel_domains — §6.1). Пересчёт выполняется в
// транзакции воркера сразу после записи findings домена: каждое
// зафиксированное состояние согласовано — findings и агрегат не
// расходятся (ТЗ 3.4).
//
// Состав агрегата — по уверенным срабатываниям домена (laravel_findings
// с is_laravel; §13.1: порог попадания в laravel_domains «score ≥ 0.6»
// и есть определение уверенности, слабые срабатывания остаются только в
// findings):
//
//   - score — максимум: порога мог достичь только максимум, поэтому
//     значение совпадает с максимумом по всем findings домена;
//   - диапазон версий — объединение диапазонов уверенных срабатываний:
//     нижняя граница — min(ver_min), верхняя — max(ver_max) (ТЗ 5.8
//     «объединённый диапазон» — не пересечение: пересечение считает сам
//     findings по правилам одной страницы). Сравнение — числовыми
//     компонентами (rules.CompareVersions): текстовое MIN/MAX в SQL
//     упорядочило бы «10» раньше «5.3». Граница NULL у любого срабатывания
//     (нет границы или конфликт T-070) распространяется на домен:
//     объединение с этой стороны неограничено; инверсия границ невозможна
//     — у каждого срабатывания ver_min ≤ ver_max (T-070);
//   - found_on_mask — биты субдоменов с is_laravel-срабатыванием (любого
//     варианта — варианты одного субдомена схлопываются в бит), семантика
//     model.DNSMask: бит № = subdomains.id, биты 0..62;
//   - rules_version — версия набора, которым выполнен пересчёт: в
//     переходный период переанализа (T-074) findings домена смешанных
//     версий, строка перезаписывается каждой доставкой — после последнего
//     обновления findings домена версия свежая;
//   - updated_at — now() сервера БД: время транзакции (стиль record.go).
//
// Домен без is_laravel-строк не попадает в laravel_domains (порог §13.1):
// пересчёт удаляет существующую строку — прежние признаки в текущем
// эпизоде не подтвердились (§3.5: хранится только последний результат).
// Переходы наличия строки пишутся в append-only журнал laravel_history
// (T-118, appeared/lost) — только по свежим эпизодам HTTP
// (queue.Result.Fresh): переанализ (T-074) пересчитывает тот же
// скачанный контент по новым правилам и фактом о сайте не является;
// детали семантики — в комментарии aggregateDomain.
//
// Согласование параллельных доставок домена: воркер обрабатывает до
// Concurrency доставок одновременно, инстансов может быть несколько
// (ТЗ 4.2), а HTTP-этап публикует q:analyze пачкой по строкам домена.
// Два пересчёта, читающие findings до фиксации друг друга, породили бы
// агрегат без вклада соседней доставки (поздняя запись затёрла бы раннюю
// — до следующей доставки домена). Поэтому пересчёт сначала берёт
// pg_advisory_xact_lock по domain_id (глобально уникален — sequence
// domain_id_seq, T-003): второй пересчёт домена ждёт фиксации первого и
// читает уже полные findings. Advisory, а не блокировка строки domains:
// воркер к этому моменту держит блокировку строки subdomain_results
// (штамп T-071), и цепочка «штамп → domains» замкнулась бы в цикл с
// транзакцией перепроверки T-058 (UPDATE domains → DELETE
// subdomain_results); advisory-ключ этого класса берут только пересчёты
// анализатора — циклов ожидания нет.
package analyzer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"crawler/internal/model"
	"crawler/internal/rules"
)

// SQL агрегации. Ключи — литералами типов (стиль worker.go); now() —
// время транзакции сервера БД (record.go).
var (
	// aggregateLockSQL — сериализация пересчётов одного домена до конца
	// транзакции (см. док-комментарий пакета). Транзакционная —
	// освобождается фиксацией/откатом, отдельного unlock нет.
	aggregateLockSQL = "SELECT pg_advisory_xact_lock($1::int8)"

	// aggregateSelectSQL — вход пересчёта: уверенные срабатывания домена.
	// Читается в транзакции воркера — видит собственную запись findings
	// этой доставки. Порядок — по PK (сортировка ключей листа партиции),
	// детерминирован: равные в разной записи границы («9.19» и «9.19.0»)
	// дают в агрегате первую.
	aggregateSelectSQL = "SELECT subdomain_id, score, ver_min, ver_max FROM laravel_findings " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 AND is_laravel"

	// aggregateUpsertSQL — запись агрегата: upsert по PK laravel_domains
	// (§3.5 перезапись, §6.1): повторные доставки и переанализ
	// перезаписывают строку начисто. ver_min/ver_max NULL — объединение
	// неограничено с этой стороны (включая конфликт T-070).
	aggregateUpsertSQL = "INSERT INTO laravel_domains (" +
		"zone_id, domain_id, score, ver_min, ver_max, found_on_mask, rules_version, updated_at) " +
		"VALUES ($1::int2, $2::int8, $3::float4, $4::text, $5::text, $6::int8, $7::int4, now()) " +
		"ON CONFLICT (zone_id, domain_id) DO UPDATE SET " +
		"score = $3::float4, ver_min = $4::text, ver_max = $5::text, " +
		"found_on_mask = $6::int8, rules_version = $7::int4, updated_at = now()"

	// aggregateDeleteSQL — уверенных срабатываний нет: строки домена не
	// существует (порог §13.1), существующая удаляется (§3.5). Отсутствие
	// строки — не ошибка (DELETE 0).
	aggregateDeleteSQL = "DELETE FROM laravel_domains " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8"

	// aggregateExistsSQL — была ли строка домена до пересчёта: переход
	// appeared/lost — сравнение «было» и «стало» (T-118). По PK —
	// индексный lookup; выполняется под advisory-блокировкой домена,
	// параллельный пересчёт не вставит между проверкой и записью.
	aggregateExistsSQL = "SELECT EXISTS (" +
		"SELECT 1 FROM laravel_domains WHERE zone_id = $1::int2 AND domain_id = $2::int8)"

	// aggregateHistorySQL — журнал переходов appeared/lost (T-118,
	// аналог laravel_histories larasites): append-only, changed_at —
	// время транзакции (now() сервера БД). Пишется только при fresh —
	// свежем эпизоде HTTP (queue.Result.Fresh): переанализ того же
	// контента по новым правилам — не факт о сайте (см. док-комментарий
	// пакета).
	aggregateHistorySQL = "INSERT INTO laravel_history (zone_id, domain_id, appeared) " +
		"VALUES ($1::int2, $2::int8, $3)"
)

// aggregateDomain пересчитывает строку laravel_domains домена по его
// findings. Вызывается из транзакции воркера (T-071) после записи
// findings: SELECT видит записи этой транзакции, сбой любого шага
// откатывает findings и агрегат вместе. version — версия набора доставки
// (rules.Set.Version): ей штампуется пересчёт.
//
// fresh — доставка свежего эпизода HTTP (queue.Result.Fresh: публикация
// HTTP/рендер-этапа; переанализ T-074 — false). При смене наличия строки
// laravel_domains и fresh в laravel_history пишется переход (T-118):
// появление — appeared=true, исчезновение — false. Без fresh история не
// пишется: пересчёт по новому набору правил и флап borderline-скоров
// вокруг порога (T-116) — не события о сайте. Семантика привязана к
// строке, а не к сайту: «ушёл при переанализе, вернулся свежим
// эпизодом» оставит один appeared (появление) без парного lost —
// плата за защиту от шума правил, тренд по множеству доменов верен.
func aggregateDomain(ctx context.Context, tx pgx.Tx, zoneID int16, domainID int64, version int32, fresh bool) error {
	// Сначала блокировка: пересчёты одного домена выстраиваются в очередь,
	// второй читает findings после фиксации первого.
	if _, err := tx.Exec(ctx, aggregateLockSQL, domainID); err != nil {
		return fmt.Errorf("блокировка домена: %w", err)
	}

	rows, err := tx.Query(ctx, aggregateSelectSQL, zoneID, domainID)
	if err != nil {
		return fmt.Errorf("чтение срабатываний: %w", err)
	}
	agg, err := scanAggregate(rows)
	rows.Close()
	if err != nil {
		return fmt.Errorf("чтение срабатываний: %w", err)
	}

	// Наличие строки до пересчёта — для перехода appeared/lost (T-118);
	// под advisory-блокировкой конкурентный пересчёт не изменит его
	// между проверкой и записью.
	var existed bool
	if err := tx.QueryRow(ctx, aggregateExistsSQL, zoneID, domainID).Scan(&existed); err != nil {
		return fmt.Errorf("чтение строки домена: %w", err)
	}

	if agg.findings == 0 {
		tag, err := tx.Exec(ctx, aggregateDeleteSQL, zoneID, domainID)
		if err != nil {
			return fmt.Errorf("удаление строки домена: %w", err)
		}
		if tag.RowsAffected() > 0 && fresh && existed {
			// Строка была и исчезла — lost (только свежий эпизод, T-118).
			if _, err := tx.Exec(ctx, aggregateHistorySQL, zoneID, domainID, false); err != nil {
				return fmt.Errorf("запись истории (lost): %w", err)
			}
		}
		return nil
	}
	tag, err := tx.Exec(ctx, aggregateUpsertSQL,
		zoneID, domainID, agg.score, agg.verMin, agg.verMax,
		int64(agg.foundOnMask), version)
	if err != nil {
		return fmt.Errorf("запись строки домена: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("запись строки домена: затронуто %d строк", tag.RowsAffected())
	}
	if !existed && fresh {
		// Строки не было и появилась — appeared (только свежий эпизод).
		if _, err := tx.Exec(ctx, aggregateHistorySQL, zoneID, domainID, true); err != nil {
			return fmt.Errorf("запись истории (appeared): %w", err)
		}
	}
	return nil
}

// aggRow — уверенное срабатывание домена, вход пересчёта.
type aggRow struct {
	subdomainID int16
	score       float32
	verMin      *string
	verMax      *string
}

// domainAggregate — агрегат признаков Laravel по домену (ТЗ 5.8): поля
// строки laravel_domains; findings — число учтённых срабатываний
// (0 — строки домена нет).
type domainAggregate struct {
	score       float32
	verMin      *string
	verMax      *string
	foundOnMask model.DNSMask
	findings    int
}

// scanAggregate сводит уверенные срабатывания домена в агрегат:
// максимальный score, объединённый диапазон версий и маска субдоменов
// (решения — в док-комментарии пакета). Некорректный номер субдомена —
// ошибка: биты маски 0..62 (model.DNSMask), молчаливо потерянный субдомен
// исказил бы found_on_mask.
func scanAggregate(rows pgx.Rows) (domainAggregate, error) {
	var (
		agg                   domainAggregate
		minStr, maxStr        string
		unboundedMin, haveMin bool
		unboundedMax, haveMax bool
	)
	for rows.Next() {
		var r aggRow
		if err := rows.Scan(&r.subdomainID, &r.score, &r.verMin, &r.verMax); err != nil {
			return domainAggregate{}, err
		}
		if agg.findings == 0 || r.score > agg.score {
			agg.score = r.score
		}
		agg.findings++
		if err := agg.foundOnMask.Set(int(r.subdomainID)); err != nil {
			return domainAggregate{}, fmt.Errorf("found_on_mask: %w", err)
		}
		// Границы: NULL (нет границы/конфликт) запоминается флагом —
		// объединение неограничено с этой стороны; из известных границ
		// берётся экстремум (min для нижней, max для верхней), равные в
		// разной записи оставляют первую (порядок PK детерминирован).
		if r.verMin == nil {
			unboundedMin = true
		} else if !haveMin || rules.CompareVersions(*r.verMin, minStr) < 0 {
			minStr, haveMin = *r.verMin, true
		}
		if r.verMax == nil {
			unboundedMax = true
		} else if !haveMax || rules.CompareVersions(*r.verMax, maxStr) > 0 {
			maxStr, haveMax = *r.verMax, true
		}
	}
	if err := rows.Err(); err != nil {
		return domainAggregate{}, err
	}
	if agg.findings == 0 {
		return agg, nil
	}
	if haveMin && !unboundedMin {
		agg.verMin = &minStr
	}
	if haveMax && !unboundedMax {
		agg.verMax = &maxStr
	}
	return agg, nil
}

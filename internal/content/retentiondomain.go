// Точечный дренаж контента одного домена (T-187) — закрытие остаточного
// класса потери за курсором (ревью T-185, P1/P3-R3; docs/content.md).
//
// Класс: снятие Laravel-находки переанализом — доставкой без fresh
// (повторные публикации q:analyze кампаний crawlerctl reanalyze и цикла
// T-074, redeliveries тех же сообщений) — удаляет строку laravel_domains
// (aggregateDeleteSQL), не переписывая subdomain_results: старые записи
// домена возвращаются в множество кандидатов retention уже позади курсора
// дренажа по времени (T-185: строка посещается один раз за жизнь) —
// часовой проход их больше не видит, контент и jsonb держатся до редких
// catch-up-кругов либо перепроверки (~6 мес). Свежие потери находки
// строк не теряют (перезапись DELETE+INSERT пишет fresh-отметку впереди
// курсора, T-058), удалённые домены вычищает T-020.
//
// Событие известно в месте перехода — aggregateDomain (analyzer,
// RowsAffected > 0 ∧ ¬fresh ∧ existed, T-187 возвращает его сигналом) —
// и там же выполняется дренаж: сервисы analyzer/http-worker разные, сигнал
// через БД (счётчик) знает «сколько», но не «какие домены», без миграций
// ключи не передать, круг по счётчу — возврат ежечасных полных проходов,
// отвергнутых T-185; Redis-стрим сигнала — новая очередь ради P3-хвоста.
// Прямой вызов переиспользует purge-механику T-166 целиком (выборка
// фильтров отбора + retentionPurge) и не требует изменений цикла.
//
// Отличия от прохода цикла (сознательные, интерлоки/снимки T-166):
//
//   - интерлок глубины q:analyze и снимок непотреблённых ключей
//     (retentionpending.go) не консультируются: они живут в http-worker,
//     а дренаж выполняет сам потребитель q:analyze — по факту события
//     уверенные срабатывания домена уже разобраны (их больше нет).
//     Остаточное окно — непотреблённое сообщение СЛАБОЙ строки того же
//     домена при застое стрима: поздняя обработка по пустому входе
//     (контент срезан) даст «признаков нет» — потеря возможна только если
//     новые правила одновременно сняли последнюю уверенную строку и
//     подняли вес слабой (смешанное изменение одного набора), лечится
//     перепроверкой; охрана analyzeMarkSQL (content_hash IS NOT DISTINCT
//     FROM, NULL легален T-056) сходится и на очищенной строке;
//   - render_pending/rendered, deleted-домены и повторная находка
//     (NOT EXISTS laravel_domains) фильтруются той же выборкой: записи
//     под рендер-флагом остаются за дренажем — их догонит catch-up-круг
//     по обнулению счётчика долга (T-185), удалённые чистит T-020,
//     перевfinding конкурентной доставкой защищает контент заново.
//
// Выполняется отдельной короткой транзакцией ПОСЛЕ фиксации доставки
// (ошибки дренажа не роняют анализ — poison pill), идемпотентен:
// повторный вызов не найдёт кандидатов (строки уже очищены), охраны
// purge закрывают гонки перезаписи (T-166).
package content

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DrainDomain — точечный дренаж контента не-находки одного домена
// (T-187): кандидаты очистки по префиксу PK (zone_id, domain_id) —
// фильтры отбора retentionSelectSQL без курсора — чистятся штатной
// пачкой retentionPurge (срезание ссылки и jsonb → decref → удаление
// contents, ТЗ 3.4). retention — то же окно content.retention, что у
// цикла: граница старения одна для всех чистильщиков. Пустой итог
// (домен — находка/под рендер-флагом/всё чисто) — не ошибка, транзакция
// не открывается.
func DrainDomain(ctx context.Context, pool RetentionDB, retention time.Duration, zoneID int16, domainID int64) (RetentionStats, error) {
	if pool == nil {
		return RetentionStats{}, errors.New("content: retention: дренаж домена: пул не задан")
	}
	if retention <= 0 {
		// retention = 0 — контент хранится всегда (RetentionLoop не
		// поднимается): точечная чистка противоречила бы режиму.
		return RetentionStats{}, errors.New("content: retention: дренаж домена: окно retention не задано (retention выключен?)")
	}
	o := RetentionOptions{Retention: retention}.withDefaults()
	batch, err := retentionDomainSelect(ctx, pool, o, zoneID, domainID)
	if err != nil {
		return RetentionStats{}, err
	}
	if len(batch) == 0 {
		return RetentionStats{}, nil
	}
	st, err := retentionPurge(ctx, pool, o, batch)
	if err != nil {
		return st, fmt.Errorf("content: retention: дренаж домена %d/%d: %w", zoneID, domainID, err)
	}
	return st, nil
}

// retentionDomainSelect — кандидаты очистки одного домена
// (retentionDomainSQL): строки домена, остаревшие сверх окна и не
// защищённые (находка/рендер/удаление), в порядке PK.
func retentionDomainSelect(ctx context.Context, pool RetentionDB, o RetentionOptions, zoneID int16, domainID int64) ([]retentionCandidate, error) {
	rows, err := pool.Query(ctx, retentionDomainSQL, zoneID, domainID, o.Retention.String())
	if err != nil {
		return nil, fmt.Errorf("content: retention: дренаж домена: выборка: %w", err)
	}
	defer rows.Close()

	var out []retentionCandidate
	for rows.Next() {
		var (
			c       retentionCandidate
			variant int16
		)
		if err := rows.Scan(&c.key.zone, &c.key.domain, &c.key.sub, &variant, &c.hash, &c.checkedAt); err != nil {
			return nil, fmt.Errorf("content: retention: дренаж домена: выборка: чтение: %w", err)
		}
		c.key.variant = variant
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("content: retention: дренаж домена: выборка: %w", err)
	}
	return out, nil
}

// Точная защита retention от чистки записей, ожидающих очередей
// (T-166/T-167, фикс окна q:render). Флаг render_pending (бит 1
// domains.flags, T-066) защищает только записи, публикация q:render
// для которых НЕ состоялась — очередь была полна, флаг поставлен вместо
// публикации той же транзакцией (renderq.go). Записи, чьи Result-сообщения
// УЖЕ опубликованы в q:render/q:analyze (очередь была свободна), флага не
// имеют: при зависшем потребителе (инцидент 23.10: лаг q:render ≈99 тыс.,
// last-delivered-id заморожен с 08:59, рендер-воркер T-159 не задеплоен)
// сообщения стареют в стриме вместе с записями, а предикат отбора
// «r.rendered OR (d.flags & render_pending) = 0» их не видит — первый же
// проход срезал бы content_hash и headers/cookies/tls/redirects/addrs/
// cname записей, на которые указывают живые сообщения. Рендер читает
// контент SPA-записей (ТЗ 5.6), анализатор — тела и headers/cookies
// (ТЗ 5.8): потеря = деградация вердиктов (для cert-когорты, где рендер
// не состоится вовсе, сырой контент — единственный источник сигналов),
// требование T-166 «до rendered не удалять» нарушено. У q:analyze окно
// той же природы, но слабее: интерлок по общей глубине (RetentionDepth)
// не видит застарелых непотреблённых сообщений при глубине ниже порога —
// закрывается той же проверкой.
//
// Защита — точная, по самим сообщениям: перед проходом (и обновлением при
// длинном дренаже) вычитываются ключи непотреблённых сообщений обоих
// стримов — записи, доставленные группой, но не подтверждённые (pending),
// ждут потребителя минуты (клейм RecoverLoop → XACK или q:dead, T-025),
// недоставленный хвост (лаг) — часов при зависании; XRANGE покрывает
// главное окно и не трогает группу вовсе (XREADGROUP ">" здесь запрещён —
// сдвинул бы last-delivered-id и сломал реального потребителя). Ключ
// сообщения — Result T-024 (zone_id, domain_id, subdomain_id, variant;
// публикуют record.go и renderSPAResults) — совпадает с PK кандидатов
// (retentionSelectSQL); исключение — фильтром пачки в Go после выборки:
// SQL отбора не меняется (множество ключей на стороне БД не передать, а
// гонки с перезаписью всё равно закрывают охраны UPDATE).
//
// Гонки безопасны в обе стороны:
//   - публикация после снимка ссылается на fresh-запись (checked_at только
//     что записан, T-058) — не кандидат, защите не нужна; исключение —
//     переиздание прохода доработки T-066 (RequeueRenderPending публикует
//     q:render по остаревшим записям домена и лишь затем снимает флаг) —
//     его закрывает обновление снимка при длинном дренаже;
//   - потребление сообщения после снимка — запись останется исключённой
//     до конца прохода (перестраховка на минуты дренажа), следующим
//     проходом чистится штатно: опоздать защита не может, только
//     задержать.
//
// Снимок — раз в проход, а не на каждую пачку: чтение хвоста при застое —
// десятки тысяч записей, а свежие публикации защищены свежестью checked_at.
// Обновление — после каждых DefaultRetentionPendingRefresh выборок пачек:
// длинный дренаж (бэкфилл T-167 — ~114 пачек) длится минуты, за которые
// T-066-проход может переиздать q:render по остаревшим записям;
// перечитать снимок дешевле, чем допустить окно. Обновление среди дренажа
// не пересматривает пройденное (курсорная пагинация вперёд, стиль T-074) —
// только будущие пачки.
//
// Ошибка чтения стримов — консервативный пропуск прохода, как ошибка
// глубины у интерлока: чистить «наугад» дороже, чем отложить — записи
// очередей не тронуты, зафиксированные пачки остаются (пачки атомарны
// сами по себе), цикл повторит через Interval; итог уходит в OnPass
// (проход не ошибочен, а отложен) — Warn в логе и метрика
// crawler_content_retention_pending_errors_total (retentionmetrics.go).
package content

import (
	"context"
	"fmt"

	"crawler/internal/queue"
)

// DefaultRetentionPendingRefresh — сколько выборок пачек дренажа
// держат один снимок непотреблённых ключей. 25: типовой проход —
// ~13 пачек притока (замер §12) — обновляется раз в пару проходов,
// бэкфилл (~114 пачек, минуты) — ~5 обновлений за проход; окно
// устаревания (гонка с переизданием T-066) сужается с прохода до
// секунд-минут дренажа. Цена обновления — два XRANGE по стримам.
const DefaultRetentionPendingRefresh = 25

// retentionPendingStreams — стримы, чьи непотреблённые сообщения
// защищают записи от чистки: рендер (контент SPA-записей, ТЗ 5.6) и
// анализ (тела и headers/cookies, ТЗ 5.8). Имена — константы пакета
// очередей (единственный источник, как в httpworker).
var retentionPendingStreams = []string{queue.StreamRender, queue.StreamAnalyze}

// RetentionPendingKey — ключ строки subdomain_results в сообщении
// Result (T-024): тот же PK, что у кандидатов retention (ТЗ 6.1).
// Экспортирован для адаптера источника (cmd/http-worker), собирающего
// множество из записей стрима.
type RetentionPendingKey struct {
	Zone    int16
	Domain  int64
	Sub     int16
	Variant int16
}

// RetentionPending — источник непотреблённых ключей стримов: узкий
// интерфейс, пакет не зависит от Redis-специфики (стиль RetentionDepth
// для интерлока); сборка сервиса подключает адаптер над клиентом гейта
// (cmd/http-worker/main.go). Реализация возвращает множество ключей
// сообщений стрима stream, ещё не доставленных его группе потребителей
// (XRANGE от last-delivered-id группы эксклюзивно до '+'); стримы —
// константы q:render/q:analyze (retentionPendingStreams).
type RetentionPending interface {
	PendingKeys(ctx context.Context, stream string) (map[RetentionPendingKey]struct{}, error)
}

// retentionPendingSnapshot — снимок множеств обоих стримов: один проход
// работает против одного снимка (см. шапку файла). Ошибка любого стрима —
// ошибка снимка целиком: частичная защита хуже пропуска прохода
// (непокрытый q:render — предмет фикса).
func retentionPendingSnapshot(ctx context.Context, src RetentionPending) (*retentionPendingSets, error) {
	sets := make(map[string]map[RetentionPendingKey]struct{}, len(retentionPendingStreams))
	for _, s := range retentionPendingStreams {
		keys, err := src.PendingKeys(ctx, s)
		if err != nil {
			return nil, fmt.Errorf("content: retention: непотреблённые %s: %w", s, err)
		}
		sets[s] = keys
	}
	return &retentionPendingSets{sets: sets}, nil
}

// retentionPendingSets — снимок: множество ключей по стриму.
// Нулевое значение (nil-sets) — «ждущих нет».
type retentionPendingSets struct {
	sets map[string]map[RetentionPendingKey]struct{}
}

// key — внешний ключ снимка по внутреннему ключу кандидата.
func (p *retentionPendingSets) key(k retentionKey) RetentionPendingKey {
	return RetentionPendingKey{Zone: k.zone, Domain: k.domain, Sub: k.sub, Variant: k.variant}
}

// waiting — ждёт ли ключ потребителей хоть одного стрима снимка:
// единый критерий исключения — им пользуется и deferBatch (фильтрация
// пачки), и сбор списка ретраев защиты очередей (T-185,
// retentiondrain.go).
func (p *retentionPendingSets) waiting(k retentionKey) bool {
	if p == nil {
		return false // защита выключена — ждущих нет
	}
	key := p.key(k)
	for _, s := range retentionPendingStreams {
		if _, ok := p.sets[s][key]; ok {
			return true
		}
	}
	return false
}

// countDeferred — учёт исключённой записи по стримам-держателям (запись
// публикуется в q:render и q:analyze одновременно — держателей может
// быть два, наблюдаемость «кто держит» важнее единичности счёта).
func (p *retentionPendingSets) countDeferred(k retentionKey, st *RetentionStats) {
	key := p.key(k)
	for _, s := range retentionPendingStreams {
		if _, ok := p.sets[s][key]; !ok {
			continue
		}
		if st.Deferred == nil {
			st.Deferred = make(map[string]int64, len(retentionPendingStreams))
		}
		st.Deferred[s]++
	}
}

// deferBatch исключает из пачки кандидатов записи, чьи ключи есть в
// снимке (критерий — waiting): их сообщения ждут потребителей — контент
// и jsonb ещё нужны («до rendered не удалять», T-166); каждое исключение
// считается в stats.Deferred. Фильтрация на месте: пачка после вызова
// вызывающему не нужна, аллокация второй пачки в дренаже лишняя.
func (p *retentionPendingSets) deferBatch(batch []retentionCandidate, st *RetentionStats) []retentionCandidate {
	if p == nil {
		return batch // защита выключена — пачка без изменений
	}
	kept := batch[:0]
	for _, c := range batch {
		if p.waiting(c.key) {
			p.countDeferred(c.key, st)
			continue
		}
		kept = append(kept, c)
	}
	return kept
}

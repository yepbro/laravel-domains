// Package db: пакетный сдвиг счётчиков состояний зон (T-178, ТЗИ §6.1):
// дельты выполняются в транзакции перехода — вторым оператором (здесь)
// или CTE того же оператора (scheduler: батчевые переходы по RETURNING
// zone_id). Откат транзакции откатывает пару «переход + дельта» —
// отдельная согласованность не нужна (ТЗИ §3.2.3). Отрицательные
// значения не клампятся: отрицательный счётчик — видимый дрейф, который
// чинит реконсиляция, а не маскировка greatest(0, …) (ТЗИ §6.1.4).
package db

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgconn"
)

// Execer — исполнитель одного оператора: pgx.Tx транзакции перехода
// или пул (db.DB); инкрементальный сдвиг выполняется только внутри
// транзакции перехода — интерфейс объединяет оба.
type Execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// ZoneCounterShiftSQL — пакетный сдвиг колонок зон дельтами: параллельные
// массивы (zone_id + 4 дельты; 0 — колонка не меняется; domains_laravel
// сдвигает analyzer отдельным точечным оператором — у переходов
// состояний конвейера laravel не меняется). unnest — стиль пакетных
// UPDATE пакета (ТЗ 6: «UPDATE … FROM unnest(...)»).
const ZoneCounterShiftSQL = "UPDATE zones AS z " +
	"SET domains_total = z.domains_total + u.d_total, " +
	"domains_pending = z.domains_pending + u.d_pending, " +
	"domains_queued = z.domains_queued + u.d_queued, " +
	"domains_checked = z.domains_checked + u.d_checked " +
	"FROM unnest($1::int2[], $2::int8[], $3::int8[], $4::int8[], $5::int8[]) " +
	"AS u(id, d_total, d_pending, d_queued, d_checked) WHERE z.id = u.id"

// ZoneShift — накопитель дельт счётчиков зон перехода: собирается по
// RETURNING zone_id оператора перехода (GROUP BY в памяти — зон в пачке
// десятки), применяется одним оператором ZoneCounterShiftSQL в той же
// транзакции. Не потокобезопасен: один переход — один накопитель.
type ZoneShift struct {
	byZone map[int16][4]int64
}

// NewZoneShift — пустой накопитель дельт.
func NewZoneShift() *ZoneShift {
	return &ZoneShift{byZone: make(map[int16][4]int64)}
}

// Add фиксирует дельту колонок одной зоны перехода (значения — ±n;
// 0 — колонка не меняется).
func (s *ZoneShift) Add(zone int16, total, pending, queued, checked int64) {
	d := s.byZone[zone]
	d[0], d[1], d[2], d[3] = d[0]+total, d[1]+pending, d[2]+queued, d[3]+checked
	s.byZone[zone] = d
}

// Len — число зон с дельтами.
func (s *ZoneShift) Len() int { return len(s.byZone) }

// Exec применяет накопленные дельты в транзакции перехода. Пустой
// накопитель — no-op без обращения к БД. RowsAffected != числу зон —
// строка зоны отсутствует (возможна только при ручной правке
// справочника): молчаливый пропуск — источник дрейфа, ошибка громче
// (стиль renderBumpDebtCounter, T-175).
func (s *ZoneShift) Exec(ctx context.Context, ex Execer) error {
	if len(s.byZone) == 0 {
		return nil
	}
	ids := make([]int16, 0, len(s.byZone))
	for id := range s.byZone {
		ids = append(ids, id)
	}
	// Порядок детерминирован по возрастанию zone_id — протокол вызовов
	// воспроизводим в юнит-тестах (карта в Go итерируется случайно).
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	cols := make([][4]int64, len(ids))
	for i, id := range ids {
		cols[i] = s.byZone[id]
	}
	total := make([]int64, len(cols))
	pending := make([]int64, len(cols))
	queued := make([]int64, len(cols))
	checked := make([]int64, len(cols))
	for i, d := range cols {
		total[i], pending[i], queued[i], checked[i] = d[0], d[1], d[2], d[3]
	}
	tag, err := ex.Exec(ctx, ZoneCounterShiftSQL, ids, total, pending, queued, checked)
	if err != nil {
		return fmt.Errorf("db: сдвиг счётчиков зон (%d зон): %w", len(ids), err)
	}
	if tag.RowsAffected() != int64(len(ids)) {
		return fmt.Errorf("db: сдвиг счётчиков зон: затронуто %d строк из %d (строка зоны обязательна)",
			tag.RowsAffected(), len(ids))
	}
	return nil
}

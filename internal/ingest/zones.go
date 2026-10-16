// Справочник зон приёма (ТЗ 5.1, 5.2 пп. 6–7): EnsureZone — найти зону по
// punycode-имени или создать автоматически при первом файле (is_active =
// true); доступ к БД — через маленький интерфейс ZoneStore (юнит-тесты —
// на фейке), SQL-реализация SQLZoneStore — поверх internal/db.
package ingest

import (
	"context"
	"errors"
	"fmt"

	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/model"

	"github.com/jackc/pgx/v5"
)

// ErrZoneNotFound — зоны с указанным punycode-именем нет в таблице zones.
// Возвращается ZoneStore.ZoneByName; для EnsureZone это сигнал создать
// зону (ТЗ 5.1: автоматически при первом файле).
var ErrZoneNotFound = errors.New("ingest: зоны нет в таблице zones")

// ZoneStore — доступ к таблице zones, необходимый приёму файлов: найти
// зону по имени или создать. Интерфейс маленький намеренно: EnsureZone
// зависит только от него, поэтому юнит-тесты обходятся фейком, а хранилище
// подменяемо (SQLZoneStore — pgx; тестовый дубль — сколько угодно).
type ZoneStore interface {
	// ZoneByName возвращает строку zones по punycode-имени (zones.name,
	// ТЗ 5.2: внутри системы хранится ASCII-форма). Отсутствие зоны —
	// ошибка ErrZoneNotFound, прочее — ошибка хранилища.
	ZoneByName(ctx context.Context, name string) (model.Zone, error)

	// CreateZone создаёт зону с is_active = true (ТЗ 5.1) и возвращает её
	// строку. Имя уже занято — не ошибка: возвращается существующая строка
	// (конкурентная обработка двух файлов одной зоны — одна зона, без
	// дублей, ТЗ 11.4 п. 1).
	CreateZone(ctx context.Context, name string, nameUnicode *string) (model.Zone, error)
}

// EnsureZone возвращает зону с именем из каталога incoming (unicode или
// punycode — ТЗ 5.1): имя нормализуется в ASCII-форму (ТЗ 5.2 п. 6), по
// ней зона ищется в БД, а отсутствующая создаётся автоматически при первом
// файле с is_active = true (ТЗ 5.1, 11.4 п. 1). Так unicode- и
// punycode-каталог одной зоны сходятся в одну строку zones — оба имени
// нормализуются в один ключ поиска.
//
// name новой зоны — punycode (zones.name), name_unicode — unicode-форма
// только для отображения (ТЗ 5.2 п. 7); для ASCII-зоны она совпадает с
// name и не хранится (NULL): информации не несёт.
func EnsureZone(ctx context.Context, store ZoneStore, zone string) (model.Zone, error) {
	ascii, err := idn.NormalizeZone(zone)
	if err != nil {
		return model.Zone{}, fmt.Errorf("ingest: зона %q: %w", zone, err)
	}

	z, err := store.ZoneByName(ctx, ascii)
	if err == nil {
		return z, nil
	}
	if !errors.Is(err, ErrZoneNotFound) {
		return model.Zone{}, fmt.Errorf("ingest: поиск зоны %s: %w", ascii, err)
	}

	z, err = store.CreateZone(ctx, ascii, displayUnicode(ascii))
	if err != nil {
		return model.Zone{}, fmt.Errorf("ingest: создание зоны %s: %w", ascii, err)
	}
	return z, nil
}

// displayUnicode — значение zones.name_unicode для punycode-имени зоны:
// unicode-форма (idna.ToUnicode на лету, ТЗ 5.2 п. 7), если отличается от
// ASCII-формы; для ASCII-зоны — nil (NULL).
func displayUnicode(ascii string) *string {
	if u := idn.ToUnicode(ascii); u != ascii {
		return &u
	}
	return nil
}

// zoneColumns — колонки строки zones в порядке полей model.Zone; теги db
// модели соответствуют этому списку (pgx.RowToStructByName, T-008).
const zoneColumns = "id, name, name_unicode, is_active, domains_total, domains_checked, updated_at"

const (
	sqlZoneByName = "SELECT " + zoneColumns + " FROM zones WHERE name = $1"

	// is_active задаётся явно, а не DEFAULT схемы: «новая зона создаётся
	// автоматически при первом файле (is_active = true)» — ТЗ 5.1.
	// ON CONFLICT DO NOTHING: конкурентное создание той же зоны — не
	// ошибка; без RETURNING-строки существующая читается повторным
	// поиском (ниже в CreateZone).
	sqlZoneCreate = "INSERT INTO zones (name, name_unicode, is_active) " +
		"VALUES ($1, $2, true) ON CONFLICT (name) DO NOTHING RETURNING " + zoneColumns
)

// SQLZoneStore — ZoneStore поверх pgx-пула (internal/db). Реальное
// поведение против живой БД — интеграционные тесты (T-023); юнит-тесты
// проверяют текст SQL и параметры на фейковом пуле.
type SQLZoneStore struct {
	pool db.DB
}

// NewZoneStore — ZoneStore на pgx-пуле (*pgxpool.Pool и любые обёртки,
// удовлетворяющие db.DB).
func NewZoneStore(pool db.DB) *SQLZoneStore {
	return &SQLZoneStore{pool: pool}
}

// ZoneByName ищет зону по punycode-имени; отсутствующая — ErrZoneNotFound.
func (s *SQLZoneStore) ZoneByName(ctx context.Context, name string) (model.Zone, error) {
	z, err := s.queryZone(ctx, sqlZoneByName, "поиск зоны "+name, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Zone{}, ErrZoneNotFound
	}
	return z, err
}

// CreateZone вставляет зону (name — punycode, name_unicode — отображение,
// ТЗ 5.2 п. 7) и возвращает её строку. Если INSERT не вернул строку
// (ON CONFLICT DO NOTHING — имя занято конкурентным созданием), зона
// читается повторным поиском: два файла одной зоны дают одну строку zones.
func (s *SQLZoneStore) CreateZone(ctx context.Context, name string, nameUnicode *string) (model.Zone, error) {
	z, err := s.queryZone(ctx, sqlZoneCreate, "создание зоны "+name, name, nameUnicode)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.ZoneByName(ctx, name)
	}
	return z, err
}

// queryZone выполняет запрос, возвращающий не более одной строки zones,
// и разбирает её в model.Zone. pgx.ErrNoRows не оборачивается: вызывающие
// (ZoneByName/CreateZone) трактуют его сами.
func (s *SQLZoneStore) queryZone(ctx context.Context, sql, what string, args ...any) (model.Zone, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return model.Zone{}, fmt.Errorf("db: zones: %s: %w", what, err)
	}
	z, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[model.Zone])
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return model.Zone{}, fmt.Errorf("db: zones: %s: %w", what, err)
	}
	return z, err
}

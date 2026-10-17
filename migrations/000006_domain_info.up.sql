-- T-082: SQL-функция domain_info(text) — сводка домена по имени для
-- прямого запроса из psql (ТЗ 5.11: «SQL-функция domain_info(text) для
-- прямого запроса из БД»; CLI-обёртка crawlerctl domain — T-081).
--
-- Формат ответа — jsonb, не TABLE-набор колонок: результат составной
-- (зона + строка домена + массивы расшифровок dns_mask, результатов
-- по субдоменам/вариантам и признаков Laravel), переменной глубины;
-- jsonb читается из psql одним значением и даёт стабильные пути полей
-- для тестов. Домен или зона не найдены — NULL, не ошибка (ТЗ 5.11:
-- «понятное сообщение» формирует CLI, SQL-функция лишь отвечает фактом).
--
-- Нормализация входа — минимум средствами SQL (ТЗ 5.2 п. 1): нижний
-- регистр, снятие краевых пробелов и точек. Полный конвейер IDN
-- (пробелы внутри, punycode, проверки длин) — сторона Go (internal/idn):
-- в БД имена хранятся punycode, unicode-вход адресует CLI T-081,
-- нормализуя имя до обращения к функции. Зона отделяется сопоставлением
-- суффиксов со справочником zones (самый длинный совпавший суффикс):
-- список публичных суффиксов в SQL недоступен, а zones — фактический
-- набор зон каталога, покрывающий и составные зоны вроде co.uk;
-- поведение совпадает с idn.Normalize (зона по PSL, составные метки
-- домена остаются в label).
--
-- Поиск домена — по уникальному ключу (zone_id, label), индекс
-- domains_zone_id_label_key, без полного сканирования (ТЗ 5.11).
--
-- Безопасность: функция только читает — STABLE, PARALLEL SAFE,
-- SECURITY INVOKER (явно); тело обращается к таблицам по
-- неквалифицированным именам, поэтому search_path закрепляется за
-- функцией (ALTER после CREATE: имя схемы во время миграции неизвестно —
-- набор накатывается и в рабочую схему, и в изолированные схемы
-- интеграционных тестов через search_path DSN).
--
-- Состав ответа (ключи jsonb, змеиные имена как у колонок):
--   zone            {id, name, name_unicode}
--   domain          {id, label, name, status, status_name, check_state,
--                    check_state_name, is_idn, render_pending, dns_mask,
--                    temp_attempts, queued_at, last_checked_at}
--   dns_alive       [{id, label}] — расшифровка dns_mask по справочнику
--                    subdomains (ТЗ 5.11)
--   results         [{subdomain_id, subdomain, variant, scheme,
--                    scheme_name, http_status, final_url, alias_kind,
--                    alias_kind_name, alias, rendered, error,
--                    content_hash, checked_at}] — по (субдомен, вариант),
--                    как в ТЗ 5.11; контент не раскрывается — только
--                    ссылка content_hash (распаковка из хранилища — CLI)
--   laravel         {score, ver_min, ver_max, found_on_mask, found_on,
--                    rules_version, updated_at, findings} — агрегат
--                    laravel_domains (json null, когда признаков нет);
--                    findings — все срабатывания laravel_findings
--                    домена по убыванию score (включая слабые ниже
--                    порога с пометкой is_laravel, ТЗ 5.11), signals
--                    передаются как есть
-- Имена *_name дублируют коды значений из ТЗ 6.2 и internal/model
-- (Status/CheckState/Scheme/AliasKind) для чтения из psql без справочника.

CREATE OR REPLACE FUNCTION domain_info(p_name text)
RETURNS jsonb
LANGUAGE sql
STABLE
PARALLEL SAFE
SECURITY INVOKER
AS $fn$
WITH input AS (
    -- ТЗ 5.2 п. 1 SQL-минимумом: регистр, краевые пробелы и точки.
    SELECT NULLIF(lower(btrim(p_name, E' \t\n\r\f\v.')), '') AS name
),
labels AS (
    SELECT string_to_array(name, '.') AS arr
    FROM input
    WHERE name IS NOT NULL
),
cand AS (
    -- Зона — самый длинный суффикс имени, присутствующий в zones
    -- (co.uk предпочитается uk); label — оставшиеся метки целиком.
    -- k — индекс первой метки зоны; k = 2 оставляет метке хотя бы одну
    -- метку: имя из одной метки — зона, не домен.
    SELECT z.id                                        AS zone_id,
           z.name                                      AS zone_name,
           z.name_unicode                              AS zone_name_unicode,
           array_to_string(l.arr[1:k - 1], '.')        AS label
    FROM labels l
    CROSS JOIN LATERAL generate_series(2, array_length(l.arr, 1)) AS k
    JOIN zones z
      ON z.name = array_to_string(l.arr[k:array_length(l.arr, 1)], '.')
    ORDER BY k
    LIMIT 1
),
dom AS (
    -- Строка домена по уникальному ключу (ТЗ 5.11); dom пуста —
    -- функция вернёт NULL (нет строки результата).
    SELECT c.zone_id, c.zone_name, c.zone_name_unicode, c.label,
           d.id, d.status, d.check_state, d.flags, d.dns_mask,
           d.queued_at, d.last_checked_at, d.temp_attempts
    FROM cand c
    JOIN domains d
      ON d.zone_id = c.zone_id AND d.label = c.label
)
SELECT jsonb_build_object(
    'zone', jsonb_build_object(
        'id',           dom.zone_id,
        'name',         dom.zone_name,
        'name_unicode', dom.zone_name_unicode
    ),
    'domain', jsonb_build_object(
        'id',               dom.id,
        'label',            dom.label,
        'name',             dom.label || '.' || dom.zone_name,
        'status',           dom.status,
        'status_name',      CASE dom.status
                                WHEN 0 THEN 'new'
                                WHEN 1 THEN 'not_delegated'
                                WHEN 2 THEN 'delegated_no_address'
                                WHEN 3 THEN 'no_http'
                                WHEN 4 THEN 'alive'
                                WHEN 5 THEN 'temp_error'
                                WHEN 6 THEN 'deleted'
                            END,
        'check_state',      dom.check_state,
        'check_state_name', CASE dom.check_state
                                WHEN 0 THEN 'pending'
                                WHEN 1 THEN 'queued'
                                WHEN 2 THEN 'done'
                            END,
        'is_idn',           (dom.flags & 1) <> 0,
        'render_pending',   (dom.flags & 2) <> 0,
        'dns_mask',         dom.dns_mask,
        'temp_attempts',    dom.temp_attempts,
        'queued_at',        dom.queued_at,
        'last_checked_at',  dom.last_checked_at
    ),
    'dns_alive', COALESCE((
        SELECT jsonb_agg(jsonb_build_object('id', s.id, 'label', s.label)
                         ORDER BY s.id)
        FROM subdomains s
        WHERE (dom.dns_mask >> s.id) & 1 <> 0
    ), '[]'::jsonb),
    'results', COALESCE((
        SELECT jsonb_agg(
                   jsonb_build_object(
                       'subdomain_id',    r.subdomain_id,
                       'subdomain',       s.label,
                       'variant',         r.variant,
                       'scheme',          r.scheme,
                       'scheme_name',     CASE r.scheme
                                              WHEN 1 THEN 'http'
                                              WHEN 2 THEN 'https'
                                          END,
                       'http_status',     r.http_status,
                       'final_url',       r.final_url,
                       'alias_kind',      r.alias_kind,
                       'alias_kind_name', CASE r.alias_kind
                                              WHEN 0 THEN 'none'
                                              WHEN 1 THEN 'identical'
                                              WHEN 2 THEN 'redirect'
                                          END,
                       'alias',           s2.label,
                       'rendered',        r.rendered,
                       'error',           r.error,
                       'content_hash',    encode(r.content_hash, 'hex'),
                       'checked_at',      r.checked_at
                   )
                   ORDER BY r.subdomain_id, r.variant
               )
        FROM subdomain_results r
        JOIN subdomains s  ON s.id  = r.subdomain_id
        LEFT JOIN subdomains s2 ON s2.id = r.alias_subdomain_id
        WHERE r.zone_id = dom.zone_id AND r.domain_id = dom.id
    ), '[]'::jsonb),
    'laravel', (
        SELECT jsonb_build_object(
            'score',          ld.score,
            'ver_min',        ld.ver_min,
            'ver_max',        ld.ver_max,
            'found_on_mask',  ld.found_on_mask,
            'found_on', COALESCE((
                SELECT jsonb_agg(jsonb_build_object('id', s.id, 'label', s.label)
                                 ORDER BY s.id)
                FROM subdomains s
                WHERE (ld.found_on_mask >> s.id) & 1 <> 0
            ), '[]'::jsonb),
            'rules_version',  ld.rules_version,
            'updated_at',     ld.updated_at,
            'findings', COALESCE((
                SELECT jsonb_agg(
                           jsonb_build_object(
                               'subdomain_id', lf.subdomain_id,
                               'subdomain',    s.label,
                               'variant',      lf.variant,
                               'score',        lf.score,
                               'ver_min',      lf.ver_min,
                               'ver_max',      lf.ver_max,
                               'is_laravel',   lf.is_laravel,
                               'signals',      lf.signals
                           )
                           ORDER BY lf.score DESC, lf.subdomain_id, lf.variant
                       )
                FROM laravel_findings lf
                JOIN subdomains s ON s.id = lf.subdomain_id
                WHERE lf.zone_id = ld.zone_id AND lf.domain_id = ld.domain_id
            ), '[]'::jsonb)
        )
        FROM laravel_domains ld
        WHERE ld.zone_id = dom.zone_id AND ld.domain_id = dom.id
    )
)
FROM dom;
$fn$;

-- Фиксация search_path за функцией (см. комментарий о безопасности):
-- pg_catalog первым — имена функций в теле, — затем схема, по которой
-- накатана миграция (таблицы). %I экранирует имя при необходимости.
DO $fix$
BEGIN
    EXECUTE format(
        'ALTER FUNCTION domain_info(text) SET search_path = pg_catalog, %I',
        current_schema());
END
$fix$;

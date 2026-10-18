-- T-120: данные публичного лендинга (этап 13, перенос из larasites).
--
-- 1) zones.domains_laravel — кэш числа Laravel-доменов зоны: карточки и
--    облако/таблица зон лендинга читают суммы по zones без агрегатов по
--    laravel_domains (сотни миллионов строк на боевом корпусе).
--    Обновляется тем же проходом ZoneStatsLoop (T-078, раз в 30–60 с):
--    дополнительный агрегат SELECT zone_id, count(*) FROM laravel_domains
--    GROUP BY zone_id — строка таблицы уже прошла порог analyzer
--    (laravel_domains пишут только уверенные срабатывания, §13.1),
--    отдельный порог здесь не нужен. NOT NULL DEFAULT 0: зона без
--    находок — ноль, а не NULL (LEFT JOIN прохода даёт COALESCE).
--
-- 2) laravel_domains_updated_at_idx — «последние находки» лендинга
--    сортируются по updated_at (ORDER BY updated_at DESC LIMIT 5), PK
--    таблицы — (zone_id, domain_id): без индекса — сортировка всей
--    таблицы на каждый показ. Партиционированный (родитель
--    партиционирован по zone_id — индекс создаётся на родителе, листья
--    на партициях создаст PG; новые партиции зон наследуют индекс
--    автоматически, процедура 000003 трогается только своими таблицами).
--
-- 3) web_checks — лог ручных проверок из формы лендинга (аналог
--    form_histories larasites; пишет только cmd/web, T-123): кто, что
--    и когда проверял — антиспам-разбор и статистика формы.
--    domain — как введено оператором (до нормализации), punycode_domain
--    — после idn.Normalize: сырой вход нужен для разбора опечаток,
--    punycode — для сопоставления с данными конвейера. Ротации нет —
--    объём ручных проверок мал; рост контролируется рейт-лимитом
--    формы (T-123).

ALTER TABLE zones ADD COLUMN domains_laravel bigint NOT NULL DEFAULT 0;

CREATE INDEX laravel_domains_updated_at_idx ON laravel_domains (updated_at);

CREATE TABLE web_checks (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    domain          text        NOT NULL,
    punycode_domain text        NOT NULL,
    ip_address      inet        NOT NULL,
    found           boolean     NOT NULL,
    score           real,
    duration_ms     integer,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX web_checks_ip_created_idx ON web_checks (ip_address, created_at);
CREATE INDEX web_checks_created_idx ON web_checks (created_at);

COMMENT ON COLUMN zones.domains_laravel IS 'Кэш числа Laravel-доменов зоны (пересчёт ZoneStatsLoop, T-120)';
COMMENT ON TABLE web_checks IS 'Лог ручных проверок из формы лендинга (пишет cmd/web, T-120/T-123)';

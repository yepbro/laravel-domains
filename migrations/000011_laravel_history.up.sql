-- T-118: история детекции Laravel appeared/lost (аналог laravel_histories
-- старой PHP-реализации larasites): aggregateDomain перезаписывает
-- laravel_domains начисто (§3.5) — тренды «сайт пришёл/ушёл с Laravel»
-- терялись. laravel_history — append-only журнал переходов: строка
-- домена появилась в laravel_domains (appeared = true) или исчезла
-- (false); changed_at — время транзакции сервера БД.
--
-- Семантика эпизода (не факт о правилах): запись только при свежем
-- эпизоде HTTP-проверки (queue.Result.Fresh, ставят HTTP/рендер-этапы;
-- переанализ T-074 не ставит) — смена набора правил и флап
-- borderline-скоров вокруг порога (T-116) не нагенерируют ложных
-- appeared/lost: переоценка того же скачанного контента — не событие
-- о сайте.
--
-- id IDENTITY PK, а не (zone_id, domain_id): журнал append-only, у
-- домена много переходов за жизнь (появился/ушёл/вернулся); parтицирование
-- по зонам не заводится сознательно: пишут только переходы (не каждый
-- домен и не каждый эпизод — только смены is_laravel), объём на порядки
-- меньше laravel_findings; индекс по changed_at закрывает оба запроса
-- потребителя — «находки за 24 ч» лендинга (T-120) и суточную динамику.
CREATE TABLE laravel_history (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    zone_id    smallint    NOT NULL,
    domain_id  bigint      NOT NULL,
    appeared   boolean     NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX laravel_history_changed_at_idx ON laravel_history (changed_at);

COMMENT ON TABLE laravel_history IS 'История детекции Laravel: переходы appeared/lost строки laravel_domains (T-118)';
COMMENT ON COLUMN laravel_history.appeared IS 'true — строка домена появилась в laravel_domains, false — исчезла';

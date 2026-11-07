-- T-259: откат CHECK-инварианта рендер-состояния.
--
-- Откат делать только вместе с откатом render-worker и http-worker на
-- версии до T-259: писать колонки перестанут только новые пути, пару
-- колонок по-прежнему ведут охраны прежних писателей (renderUpdateSQL /
-- renderFailMarkerSQL). Окно смешанных версий безвредно: старые сервисы
-- ограничение не проверяют, новые применяют миграцию при старте до
-- запуска воркеров (migrations/embed.go).

ALTER TABLE subdomain_results
    DROP CONSTRAINT IF EXISTS subdomain_results_rendered_check;

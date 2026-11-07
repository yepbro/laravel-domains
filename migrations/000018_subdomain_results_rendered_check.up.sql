-- T-259 (ТЗ-fsm §3 ФТ-5.2): инвариант рендер-состояния строки
-- subdomain_results — CHECK «rendered = true ⇒ render_failed_at IS NULL».
--
-- Инвариант всегда держали SQL-охраны писателей (internal/renderworker:
-- renderUpdateSQL пишет rendered = true и render_failed_at = NULL одной
-- командой; маркер renderFailMarkerSQL ставится только при rendered =
-- false; спецификация автомата — doc-комментарий internal/renderworker/
-- worker.go, ТЗ-fsm §3 ФТ-5, инвариант И1): расхождение колонок не
-- порождается кодом вовсе. CHECK переводит инвариант из процедурного в
-- схемный: ручной SQL, будущие писатели и эксперименты с таблицей не
-- смогут зафиксировать «отрендерена И невозможна» — бессмысленную
-- комбинацию, ломающую выборки проходов (renderPendingRowsSQL
-- фильтрует render_failed_at IS NULL, поздний переиздатель
-- internal/httpworker/renderlate.go — тот же предикат: rendered-строка
-- с маркером выпадала бы из обеих семантик сразу).
--
-- NOT VALID — по регламенту миграций (ТЗ-fsm §4): subdomain_results —
-- 40+ млн строк, сканировать таблицу на деплое запрещено; ограничение
-- с NOT VALID проверяет ТОЛЬКО новые записи (существующие данные не
-- трогает — нарушителей среди них нет, инвариант держат охраны).
-- VALIDATE CONSTRAINT — отдельным шагом по регламенту после выдержки
-- (docs/schema.md «CHECK-инварианты»): на секционированном родителе
-- PG16 валидация рекурсивна на все секции одним оператором.
--
-- Партиционирование (миграции 000001/000003): ADD CONSTRAINT ... NOT
-- VALID на родителе subdomain_results создаёт ограничение на родителе
-- и рекурсивно на всех существующих секциях (проверено на живом
-- PostgreSQL 16: convalidated = f у всех), новые секции
-- create_zone_partitions наследуют его уже валидированным; enforcement
-- для новых/изменяемых строк действует с момента применения.
--
-- HOT: колонки ограничения (rendered, render_failed_at) не входят ни в
-- один индекс (минимум индексов, ТЗ 6) — индексных записей не добавляет,
-- картину HOT-апдейтов не меняет (docs/schema.md «HOT-апдейты»).

ALTER TABLE subdomain_results
    ADD CONSTRAINT subdomain_results_rendered_check
    CHECK (rendered = false OR render_failed_at IS NULL) NOT VALID;

COMMENT ON CONSTRAINT subdomain_results_rendered_check ON subdomain_results IS
    'Инвариант рендер-состояния (T-259): отрендеренная строка не несёт маркер «рендер невозможен» (render_failed_at); добавлено NOT VALID, VALIDATE — по регламенту docs/schema.md';

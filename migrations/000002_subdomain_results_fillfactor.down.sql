-- Откат fillfactor партиции subdomain_results_default к значению
-- по умолчанию (100).

ALTER TABLE subdomain_results_default RESET (fillfactor);

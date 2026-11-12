-- Rollback of the subdomain_results_default partition fillfactor to the
-- default value (100).

ALTER TABLE subdomain_results_default RESET (fillfactor);

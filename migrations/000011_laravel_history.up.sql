-- T-118: the Laravel appeared/lost detection history (an analog of laravel_histories
-- from the old PHP implementation larasites): aggregateDomain rewrites
-- laravel_domains from scratch (§3.5) — the trends "a site came to/went off Laravel"
-- were lost. laravel_history is an append-only journal of transitions: the domain's
-- row appeared in laravel_domains (appeared = true) or disappeared
-- (false); changed_at is the DB server's transaction time.
--
-- Episode semantics (not a fact about the rules): a record only on a fresh
-- HTTP check episode (queue.Result.Fresh, set by the HTTP/render stages;
-- the T-074 re-analysis does not set it) — a rule set change and the flap
-- of borderline scores around the threshold (T-116) will not generate false
-- appeared/lost: re-evaluating the same downloaded content is not an event
-- about the site.
--
-- id IDENTITY PK, not (zone_id, domain_id): the journal is append-only, a
-- domain has many transitions over its life (appeared/left/returned); partitioning
-- by zones is deliberately not introduced: only transitions are written (not every
-- domain and not every episode — only is_laravel changes), the volume is orders of magnitude
-- smaller than laravel_findings; the index on changed_at covers both
-- consumer queries — the landing's "finds of the last 24 h" (T-120) and the daily dynamics.
CREATE TABLE laravel_history (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    zone_id    smallint    NOT NULL,
    domain_id  bigint      NOT NULL,
    appeared   boolean     NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX laravel_history_changed_at_idx ON laravel_history (changed_at);

COMMENT ON TABLE laravel_history IS 'Laravel detection history: appeared/lost transitions of the laravel_domains rows (T-118)';
COMMENT ON COLUMN laravel_history.appeared IS 'true — the domain row appeared in laravel_domains, false — it disappeared';

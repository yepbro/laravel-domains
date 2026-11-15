# questions/

Settled cross-cutting questions worth keeping: why a decision was made, the
upstream state at the time, and the exact conditions to revisit it — so nobody
has to re-research from scratch (deja, issue threads, commit archaeology).

A file per question, named after the subject. It must contain: the question,
the answer with links (upstream issues/PRs, in-repo code and docs), the date it
was settled, and a "Retrigger" section.

- [`lightpanda-browser-contexts.md`](lightpanda-browser-contexts.md) — why
  Lightpanda is a render-engine candidate and why it stays red: one browser
  context per CDP connection breaks the per-task isolation model; 1.0.4's
  multi-page does not fix it; waiting for upstream PR #4151 (settled
  2026-11-15).

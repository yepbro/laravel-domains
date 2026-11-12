# AGENTS.md

Instructions for an agent working in a loop on project tasks.

## Project

Go monorepo: a service for bulk domain checking and discovery of Laravel sites.
The dev environment is Docker (Go is not needed on the host). Structure and description: `README.md`.
Specs: `tasks/TZ_*.md`. Task breakdown: `tasks/tasks.md`.

## New task format

- Details of a new task (incident context and symptoms, root
  cause, requirements, acceptance criteria, links to specs/reports/commits) go
  into a separate file `tasks/task/<T-NNN>-<slug>.md`.
- The `tasks/tasks.md` plan holds only a title line with the status and
  a link to the task file:
  `- **T-NNN** [status] — <title> ([details](task/<T-NNN>-<slug>.md))`.
  Details are not duplicated in the plan; detail edits live in the task file.
- Pass the task file link to the task subagent — it reads the file itself.

## Work cycle

1. Pick a task: the first `[in progress]` one; if there is none — the topmost `[new]` one (top to bottom).
   No suitable tasks — the cycle is finished.
2. Set its status to `[in progress]` in `tasks/tasks.md`.
3. Implement the task according to its description and the spec links.
4. Run the checks (see "Commands"). All green — mandatory.
5. If needed, create/update documentation: `docs/`, `README.md`; if needed — `AGENTS.md`.
6. Set the status to `[done]` and make the final commit. Go to step 1.

## Cycle operating rules

The main session drives the cycle; each task is executed by a separate subagent.
The "Work cycle" section above is the subagent's inner loop.

### Task execution order

The main session keeps a sliding window of up to 4 tasks at a time (see
"Parallel execution"): while there are free slots and suitable
independent tasks, it launches new ones without waiting for the running
ones to finish.

1. The main session picks the next independent task (the first
   `[in progress]` one; if none — the topmost `[new]` one, skipping those
   dependent on the running tasks), sets it to `[in progress]` with a status
   commit in the main branch, creates a feature branch `t-NNN` and a
   worktree for the task (see "Branches and worktrees") and launches a
   subagent **in the background** with the model
   `zai-coding-plan/glm-5.3` (all development goes through it only)
   with full context: the task text, links to the specs, the current
   repository state, and the check requirements.
2. The subagent runs the inner loop (implementation → tests → documentation)
   in the worktree of the `t-NNN` branch and makes **one commit per task**
   (format `<type>: <description> (T-0NN)`). The subagent **does not touch**
   `tasks/tasks.md`: only the main session changes statuses (step 5) —
   this removes rebase conflicts between parallel waves and keeps the
   status honest ("done" = "in master"). Review fixes are applied via
   `git commit --amend` — the commit stays single. The subagent does not
   push and does not merge.
3. After green tests the subagent runs a code review per the scheme from
   the "Review scheme" section below, with follow-up work in its own
   session: agrees with a finding — fix + amend + a re-run of the tests;
   disagrees — a reasoned justification. The review is one round (a failed
   reviewer is covered by a fallback from the chain — still one round).
   If all reviewers in the chain are unavailable (limits, errors) — no
   review is performed; the fact is recorded in the report and the commit.
4. On the subagent completion notification the main session verifies the
   result: the commit is in place and there is exactly one, `git status`
   is clean, the status in `tasks/tasks.md` is updated; if in doubt — a
   control `make test` / `make lint`. Verification and merging run in
   parallel with the other window tasks; the freed slot is immediately
   taken by the next task.
5. After verification the main session merges the task branch into `master`
   linearly — rebase + `--ff-only` (see "Linear master history"),
   sequentially; after each merge — `make test` +
   `make test-integration` in the main tree, then it removes the
   task worktree and branch. Pushing — only on an explicit instruction
   (see "Rules").
   After a green merge — the status commit `chore: T-0NN [done] — <short
   summary from the report>` in `tasks/tasks.md` (only the main session
   sets the status — see step 2).
6. The subagent returns a full report: what was done and which decisions
   were made, the check results, the review outcomes (findings / fixes /
   rejections), the commit hash. The main session validates the report
   and wraps up.

### Review scheme

The reviewer chain — strictly by the list; a fallback is launched **only**
if the previous one produced no review (a non-zero exit, an auth/
provider/quota error in the output file, or no findings text); the chain
stops at the first one that produced findings. The review runs **once
per task** — no repeat rounds; a failed primary covered
by a fallback is still one round.

1. Primary reviewer — `opencode run` with `opencode-go/deepseek-v4.1-flash`,
   from the worktree under review:

   ```bash
   timeout 1200 opencode run --auto \
     -m "opencode-go/deepseek-v4.1-flash" \
     --title "review-<slug>" "$(cat /tmp/opencode/review/prompt-<slug>.md)" \
     > /tmp/opencode/review/<slug>-deepseek.md 2>&1; echo "exit=$?"
   ```

   `--auto` — auto-approval of all permissions not explicitly denied:
   without it a non-interactive run cannot answer an `ask` request and
   hangs or gets rejected. Access to paths outside the working directory
   is allowed by default; the reviewer's read-only mode is ensured by the
   prompt (rules below), not by permissions.

   The reviewer is a separate CLI process from the executing session's
   shell (there is no nested-subagent spawn tool — do not look for one).
   The prompt carries the read-only rules and the report path; the final
   message remains in the stdout file as a backup.
2. First fallback — `cursor-agent` (Cursor CLI, model `grok-4.7-high`;
   auth/diagnostics — `~/.config/opencode/agent-cursor.md`):

   ```bash
   timeout 1200 cursor-agent -p --output-format text --mode ask \
     --model grok-4.7-high --trust \
     "$(cat /tmp/opencode/review/prompt-<slug>.md)" </dev/null \
     > /tmp/opencode/review/<slug>-cursor.md 2>&1; echo "exit=$?"
   ```

   Parameters: `-p` — headless mode with the response printed to stdout
   (without it a TUI comes up); `--mode ask` — read-only at the CLI level
   (edits and shell are blocked by the mode itself — stronger than the
   prompt); `--trust` — removes the interactive directory-trust question,
   without it a headless run hangs; `--model` — the model for a single
   run; `</dev/null` keeps it headless. Do not use `-f`/`--yolo` —
   they force-allow commands and contradict read-only.
   As with kimi, the text output may contain a working transcript —
   look for the report body (P0–P3 labels) before declaring the run
   failed.
3. Second fallback — `opencode run` with `zai-coding-plan/glm-5.3#max`
   (the same command shape as the primary, a different model):

   ```bash
   timeout 1200 opencode run --auto \
     -m "zai-coding-plan/glm-5.3#max" \
     --title "review-<slug>" "$(cat /tmp/opencode/review/prompt-<slug>.md)" \
     > /tmp/opencode/review/<slug>-glm.md 2>&1; echo "exit=$?"
   ```
4. Third fallback — `kimi` (Kimi Code CLI, model `kimi-code/k3.1-pro`;
   auth/diagnostics — `~/.config/opencode/agent-kimi.md`):

   ```bash
   timeout 1200 kimi -m kimi-code/k3.1-pro \
     -p "$(cat /tmp/opencode/review/prompt-<slug>.md)" --output-format text </dev/null \
     > /tmp/opencode/review/<slug>-kimi.md 2>&1; echo "exit=$?"
   ```

   Never combine `-p` with `-y`/`--auto`; `</dev/null` keeps it
   headless. The text output may contain a working transcript before
   the report body — look for the body (findings with P0–P3 labels)
   before declaring the run failed.

Reviewer rules (the same for the whole chain; explicitly duplicated in the
prompt — read-only and the P0–P3 format must be present in the prompt
text):

- Strictly read-only: does not modify repository files, makes no commits
  or branches. The only write is the report `/tmp/opencode/review/NN-<slug>.md`.
- Report: P0–P3 findings (P0 — security/data loss/crash; P1 —
  a significant bug; P2 — code quality; P3 — ideas), file:line, essence,
  a proposed fix, confidence (high/medium/low). Every finding is verified
  against context: callers/consumers via grep across the
  repository before inclusion in the report; unverified — not a finding.

The winning reviewer runs **synchronously in the subagent's session**:
the subagent waits for the CLI call to finish, processes the findings and
only then returns the final report (not in the background and not "after
the report" — the session would close earlier and the findings would stay
unprocessed).

If everyone in the chain is unavailable (a non-zero exit, provider errors/
limits in the output files) — no review is performed (see "Task execution
order", step 3).

### Bulk code review and fixes

A full codebase audit is a separate mode on top of the task cycle
(for example, before a release). Procedure:

1. **Baseline.** `make test` + `make lint` in the main tree — a green
   reference point to tell "we broke it" from "it was already broken".
2. **Stages.** The code is split into package groups that do not overlap
   by files, ~5–10k lines per stage (the current split — 12 stages: ingest,
   httpfetch, httpworker, dns, render, analyzer+rules, content,
   scheduler+queue, core-infra, CLI/testbed, e2e tests, deploy/infra;
   update as the code grows).
3. **Review.** Stages are reviewed by subagents per the "Review scheme" in
   a sliding window (see "Parallel execution"); the review is read-only
   against the main tree.
4. **Consolidation.** Deduplicate findings across stages, prioritize
   P0→P3, group the fixes by domains.
5. **Fixes.** Subagents with the model `zai-coding-plan/glm-5.3#max`
   (glm models — only via the `zai-coding-plan` provider);
   a fix group
   (domain) = one worktree `~/worktrees/laravel-domains/fix-<slug>`
   (rules — "Branches and worktrees"). **Each change is a separate
   commit** `fix: <description> (<finding ID>)`. Before each commit —
   `COMPOSE_PROJECT_NAME=crawler make test` + `make lint` in the worktree.
   An invalid finding is not fixed but is analyzed in the report with
   argumentation; P3 items at the level of new functionality are not
   forced in — take them out as a task in `tasks/tasks.md`.
6. **Merge.** After green tests the main session merges
   `fix-<slug>` into `master` linearly — rebase + `--ff-only`
   (see "Linear master history"), sequentially; it removes the worktree
   and the branch. The finale — `make test` + `make lint` +
   `make test-integration` in the main tree and a consolidated report on
   the findings and fixes.

### Branches and worktrees

- Work on cycle tasks is **not done** in the main branch (`master`). Any
  task — sequential or parallel with others — is executed
  by a subagent in a feature branch `t-NNN` in a separate worktree
  `~/worktrees/laravel-domains/<t-NNN>` (the recipe is in the global
  AGENTS.md: creating the worktree and branch, bootstrapping `.env`/working
  directories, removal
  after the merge).
- In the main branch the main session makes only service commits:
  status commits `chore: … in progress` when launching tasks (one at a
  time or in a batch — whichever is more convenient), targeted fixes of
  checks that failed after a merge
  (with the task noted in the message), AGENTS.md edits
  on direct instructions. No merge commits are created (linear history —
  see "Linear master history").
- Fixes following a bulk review — the `fix-<slug>` branch and the worktree
  `~/worktrees/laravel-domains/fix-<slug>` (see "Bulk code review
  and fixes"); the operating rules in it are the same.
- Only `make test` / `make lint` / `make fmt` /
  `make mutate` with `COMPOSE_PROJECT_NAME=crawler` are allowed in a
  worktree; `make up/down/clean/test-integration`
  and other targets that change the main docker stack are forbidden.

### Linear master history

On `master` the GitHub branch protection "Require linear
history" is in effect: merge commits are forbidden (the owner
bypass is not used). Merging a task/fix branch — only linearly:

    git -C <worktree> rebase master   # rebuild the branch on the current master
    git merge --ff-only <branch>      # in the main tree; fast-forward

- The rebase is performed in the worktree (the branch is checked out
  there); conflicts are resolved by the main session in the same worktree.
- Merges are sequential; after each — `make test`
  (+ `make test-integration` for tasks touching migrations, the DB,
  queues, the testbed), then removal of the worktree and the branch.
  Pushing (`git push origin master`) — only on an explicit instruction
  (see "Rules").

### Deploy: the production branch

Production is deployed only from the `production` branch (a push to
production → ci →
deploy → healthcheck with auto-rollback; details — docs/ci-cd.md).

- Commits — **only to `master`** (feature branches are merged into it
  linearly, see "Linear master history"). Commits are never made to
  `production`: it is a moving ff pointer to a commit from the
  `master` history. Its own commits to `production` and force-pushes to it
  are forbidden (GitHub itself rejects a non-ff push).
- A release is `git push origin master:production` (or `<sha>:production`);
  before pushing, check that ff is possible:
  `git fetch && git merge-base --is-ancestor origin/production master`.
  A push to `production` = a deploy to the production server.
- A hotfix and a release rollback — a commit/revert in `master`, then ff
  `production` (history forward, content backward). Cherry-picking
  directly to `production` breaks the ff link with `master` — not allowed.
- The `workflow_run` trigger in `deploy.yml` reads only from the branch
  by default (`master`) — deploy workflow edits go through
  `master`; changes only in `production` do not affect the triggers.
- A temporary stop of a unit on the server — `systemctl stop` (without
  disable):
  the unit stays enabled and comes back on its own on the next deploy
  (`systemctl start crawler.target` in install brings it up before the
  healthcheck). A deliberately disabled unit (`disable --now` + `mask`)
  is not checked by the healthcheck or by the unit checks in the
  install/rollback scripts; re-enabling — `systemctl unmask` + `enable --now`
  (+ a restart of `crawler.target`).

### Parallel execution

- By default — a sliding window: up to 4 independent tasks at a time
  (within a stage — top to bottom as they are launched); purely
  sequential execution — only when all remaining tasks are dependent.
- Only independent tasks may run in parallel (different packages/files,
  no shared migrations or DB schema); dependent ones — strictly
  sequentially (the next one starts after the previous one is merged).
- When running in parallel, each task has its own branch and its own
  worktree
  (see "Branches and worktrees"); branches are merged — sequentially from
  the main tree, linearly (see "Linear master history"), after
  each — `make test` + `make test-integration` in the main tree.
- **Subagent sliding window:** no more than 4
  subagents of any type work at a time (cycle tasks, reviews, fixes). All
  are launched in the background; the completion notification of one is
  the signal to immediately launch the next one from the queue, without
  waiting for the rest to finish. The queue may be mixed
  (tasks + reviews + fixes) as long as task files do not overlap.
  Reviews — read-only against the main tree; tasks and fixes — in a
  worktree; merges and service commits are done by the main session.
- **Forced subagent stop:** `opencode session delete
  <sessionID>` (the session is marked cancelled). Before that, take away
  the working directory: remove the worktree (`git worktree remove --force`;
  directories created by the container as root — via sudo; when recreated
  by the agent — remove the branch and the directory, registering the
  worktree anew).
  Use when switching the model/task, for zombie agents, or when an agent
  goes off track.

### One-off tasks outside the plan

Assignments outside `tasks/tasks.md` (CI, infrastructure, documentation) are
executed by a subagent under the same rules; the status in tasks.md is not
changed, the commit has no T-number. Review — as needed, per the scheme
from "Review scheme".

## Tests

- Tests are mandatory: new/changed logic is covered by unit tests.
- Before a commit: `make test` and `make lint` — no errors.
- `make test-race` — mandatory for tasks touching packages with goroutine
  loops/workers (the `RACE_PKGS` set in the Makefile: monitor, scheduler,
  renderworker, httpworker, dnsworker, queue, render, content): `make test`
  runs without `-race` and does not see races in tests with background
  loops (a separate step in CI).
- `make test-integration` — if migrations, the DB, queues, services, or
  the testbed are affected.
- A failing test — fix it; if it is outside the task scope, record the
  cause in the commit and `tasks/tasks.md`.

## Documentation

- Updated after successful test runs, not before.
- What to update when behavior, config, project structure, migrations, or
  the Makefile change: `README.md`, the corresponding file in `docs/`.
- Outdated documentation is a bug: fix it within the same task.

## Commits

- One task — one commit (code, tests, docs); the subagent does not touch `tasks/tasks.md`. The `[done]` status is a separate service commit by the main session after a green merge (`chore: T-0NN [done] — <summary>`); `[in progress]` is the status commit of the wave at launch.
- Message format: `<type>: <description> (T-0NN)`. Types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`.
- Do not mix in changes unrelated to the task. Do not commit junk (`var/`, `tmp/`, local configs).

## Rules

- **No pushing without an explicit instruction:** `git push` (including
  `git push
  origin master` after merges and `git push origin master:production` —
  a production release) is not executed until the user
  explicitly asks for it. Commits and merges are local, without pushing.
- **Subagent models:** all development (cycle tasks, one-off
  assignments) — `zai-coding-plan/glm-5.3`; review fixes within
  a task are made by the same subagent, without changing the model.
  Separate subagents for bulk-review fixes —
  `zai-coding-plan/glm-5.3#max` (see
  "Bulk code review and fixes"). glm models — only via the provider
  `zai-coding-plan`. Review — per the "Review scheme" (chain: `opencode run
  opencode-go/deepseek-v4.1-flash` → `cursor-agent grok-4.7-high` →
  `opencode run zai-coding-plan/glm-5.3#max` → `kimi kimi-code/k3.1-pro`;
  one round).
- Execute the stages in `tasks/tasks.md` in order; within a stage — top to bottom.
- Follow the structure, naming, and style of the existing code.
- Design and refine behavioral logic (statuses, states, lifecycles,
  retries) as an explicit finite-state machine — a fixed
  set of states, events, and a transition table of allowed transitions,
  not scattered flags and conditions (the FSM registry and the
  formalization priorities —
  `tasks/TZ-fsm.md`).
- A task is blocked — return its status to `[new]`, add a line with the
  reason under it, take the next one.
- Doubts about the spec interpretation — resolve by the spec text; briefly
  record the accepted decision in the commit.
- Do not break existing behavior; changes to contracts between services
  must be recorded too.

## Commands

- `make up` / `make down` / `make ps` — the environment.
- `make test` — unit tests; `make test-integration` — integration tests.
- `make mutate` — mutation testing (heavy; docs/mutation-testing.md).
- `make lint`, `make fmt` — the linter and formatting.
- `make logs SERVICE=<name>` — service logs.
- Full list: `make help`.

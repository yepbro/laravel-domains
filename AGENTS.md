# AGENTS.md

Instructions for an agent working in a loop on project tasks.

## Project

Go monorepo: a service for bulk domain checking and discovery of Laravel sites.
The dev environment is Docker (Go is not needed on the host). Structure and description: `README.md`.
Specs — the sources of stage groups: `tasks/TZ_*.md`. The task breakdown
lives on the Todoist board **LaravelDomains** (see "Work cycle").

## Work cycle

Tasks live on the Todoist board **LaravelDomains** (board view), managed
through the Todoist MCP. The board rules — sections as statuses, the
cycle order, workflow comments, `T-NNN` numbering, blocked-task handling —
per the todoist guide (loaded via the global AGENTS.md table). Ids of this
board (the machine-readable block the guide requires):

```text
todoist-project: 6hx3xR5WF9GpPG7P
todoist-inbox-section: 6j2h8f4HJJJ59mww
todoist-drafts-section: 6j2hGJwGW9fgJpMP
todoist-todo-section: 6j2hGJxpF336Gqvw
todoist-processing-section: 6j38CxP3rr4VpVgw
todoist-done-section: 6j2hGJwXCRgQXGFw
todoist-archive-section: 6j2hGM2JW7J42HpP
task-id-project: yepbro/laravel-domains
```

`task-id-project` — канонический идентификатор проекта для task-id-mcp
(выдача номеров `T-NNN`/`D-NNN`): строго это значение из git remote,
не имя каталога и не производные (прецедент: дивергенция счётчиков
`laravel-domains` vs `yepbro/laravel-domains`, 2026-11-16 — сиротские
счётчики удалены, канон выровнен на t=286/d=28).

Project refinements:

- A **Stage** is a group of tasks sharing one source (a `tasks/TZ_*.md`
  spec, a review round, an incident series). Membership is shown by the
  title prefix: `[TZ_render_engines] T-247 — …`. Lone independent tasks
  have no prefix. Stage numbers are not stored in git — the grouping
  lives only on the board; git references only the task number
  (`(T-NNN)` in the commit message).
- Task detail files in the repo are no longer created; the card
  description is the implementation spec, and new tasks are created in
  INBOX.

## Cycle operating rules

The main session drives the cycle; each task is executed by a separate subagent.
The "Work cycle" section above is the subagent's inner loop.

The general workflow rules (the plan before code, mandatory tests, the
check points, the commit discipline, documentation timing) are set by the
global `~/AGENTS.md`; below only the project specifics are given.

### Task execution order

The main session keeps a sliding window of up to 4 tasks at a time (see
"Parallel execution"): while there are free slots and suitable
independent tasks, it launches new ones without waiting for the running
ones to finish.

1. The main session picks the next independent task per the cycle rules
   (skipping those dependent on the running tasks), moves its card to
   PROCESSING, creates a feature branch `t-NNN` and a
   worktree for the task (see "Branches and worktrees") and launches a
   subagent **in the background** with the model
   `zai-coding-plan/glm-5.3` (all development goes through it only)
   with full context: the task description from the card, links to the
   specs, the current repository state, and the check requirements.
2. The subagent runs the inner loop in the worktree of the `t-NNN`
   branch and makes the task commit in the format
   `<type>: <description> (T-0NN)`.
3. The subagent runs a code review per the scheme from the "Review
   scheme" section below, with follow-up work in its own session: agrees
   with a finding — fixes it; disagrees — a reasoned justification.
   The review is one round (a failed
   reviewer is covered by a fallback from the chain — still one round).
   If all reviewers in the chain are unavailable (limits, errors) — no
   review is performed; the fact is recorded in the report and the commit.
4. On the subagent completion notification the main session verifies the
   result: the commit is in place and there is exactly one, `git status`
   is clean; if in doubt — a
   control `make test` / `make lint`. Verification and merging run in
   parallel with the other window tasks; the freed slot is immediately
   taken by the next task.
5. After verification the main session merges the task branch into `master`
   linearly — rebase + `--ff-only` (see "Linear master history"),
   sequentially; the post-merge checks in the main tree are `make test` +
   `make test-integration`, then it removes the task worktree and branch,
   then moves the card to DONE and posts the
   completion comment (end time, merged commit — see "Work cycle").
   Pushing — only on an explicit instruction (see "Rules").
6. The subagent returns a full report: what was done and which decisions
   were made, the check results, the review outcomes (findings / fixes /
   rejections), the commit hash. The main session validates the report,
   posts the essentials as a task comment and wraps up.

### Review scheme

The reviewer chain — strictly by the list; a fallback is launched **only**
if the previous one produced no review (a non-zero exit, an auth/
provider/quota error in the output file, or no findings text); the chain
stops at the first one that produced findings. The review runs **once
per task** — no repeat rounds; a failed primary covered
by a fallback is still one round. The chosen reviewer runs synchronously
in the subagent's session (the code-review guide).

Review artifacts — the prompt, the reviewer stdout files, the report —
live in the project's gitignored `tmp/review/` directory, relative to
the tree under review (the worktree); `/tmp` is not used — the host
cleans it nightly (the global Temporary files rule). The subagent
creates the directory when writing the prompt.

1. Primary reviewer — `opencode run` with `opencode-go/deepseek-v4.1-flash`,
   from the worktree under review:

   ```bash
   timeout 3600 opencode run --auto \
     -m "opencode-go/deepseek-v4.1-flash" \
     --title "review-<slug>" "$(cat tmp/review/prompt-<slug>.md)" \
     > tmp/review/<slug>-deepseek.md 2>&1; echo "exit=$?"
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
2. First fallback — `opencode run` with `zai-coding-plan/glm-5.3#max`
   (the same command shape as the primary, a different model):

   ```bash
   timeout 3600 opencode run --auto \
     -m "zai-coding-plan/glm-5.3#max" \
     --title "review-<slug>" "$(cat tmp/review/prompt-<slug>.md)" \
     > tmp/review/<slug>-glm.md 2>&1; echo "exit=$?"
   ```
3. Second fallback — `cursor-agent` (Cursor CLI, model `grok-4.7-high`):

   ```bash
   timeout 3600 cursor-agent -p --output-format text --mode ask \
     --model grok-4.7-high --trust \
     "$(cat tmp/review/prompt-<slug>.md)" </dev/null \
     > tmp/review/<slug>-cursor.md 2>&1; echo "exit=$?"
   ```

   Parameters: `-p` — headless mode with the response printed to stdout
   (without it a TUI comes up); `--mode ask` — read-only at the CLI level
   (edits and shell are blocked by the mode itself — stronger than the
   prompt); `--trust` — removes the interactive directory-trust question,
   without it a headless run hangs; `--model` — the model for a single
   run; `</dev/null` keeps it headless. Do not use `-f`/`--yolo` —
   they force-allow commands and contradict read-only.
4. Third fallback — `kimi` (Kimi Code CLI, model `kimi-code/k3.1-pro`):

   ```bash
   timeout 3600 kimi -m kimi-code/k3.1-pro \
     -p "$(cat tmp/review/prompt-<slug>.md)" --output-format text </dev/null \
     > tmp/review/<slug>-kimi.md 2>&1; echo "exit=$?"
   ```

   Never combine `-p` with `-y`/`--auto`; `</dev/null` keeps it
   headless.

Reviewer rules — per the code-review guide (loaded via the global AGENTS.md
table); they apply to every reviewer in the chain and must be carried
verbatim in the review prompt. Project-specific: the report file for any
reviewer is `tmp/review/NN-<slug>.md`.

### Bulk code review and fixes

A bulk review — of the whole codebase or of its individual parts — is a
separate mode on top of the task cycle (for example, before a release).
Procedure:

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
   (rules — "Branches and worktrees"). The commit message format is
   `fix: <description> (<finding ID>)`. Before each commit —
   `COMPOSE_PROJECT_NAME=crawler make test` + `make lint` in the worktree.
   An invalid finding is not fixed but is analyzed in the report with
   argumentation; P3 items at the level of new functionality are not
   forced in — take them out as new INBOX cards on the board.
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
  `~/worktrees/laravel-domains/<t-NNN>`. Creation/removal and bootstrap
  recipes — the git-worktree guide (loaded via the global AGENTS.md table);
  the project bootstrap: symlink `.env` from the main tree, `mkdir -p var tmp`.
- In the main branch the main session makes only service commits:
  post-merge check fixes (per the global `~/AGENTS.md`) and AGENTS.md
  edits on direct instructions. Task status lives on the board — no
  status commits. No merge commits are created (linear history —
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
- Merges are sequential; the checks after each merge in the main tree and
  the removal of the worktree and branch — see "Task execution order",
  step 5.
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

- The sliding window — per the work-cycle guide: up to 4 subagents of any
  type (cycle tasks, reviews, fixes), all launched in the background; the
  completion of one immediately launches the next from the queue; the
  queue may mix tasks, reviews and fixes while the tasks' files do not
  overlap.
- Project refinement: only independent tasks may run in parallel —
  different packages/files and no shared migrations or DB schema;
  dependent tasks — strictly sequentially, the next one starts after the
  previous one is merged. Each parallel task — its own branch and
  worktree (see "Branches and worktrees"); merges — sequential, linear,
  with full checks in the main tree (see "Linear master history").
- **Forced subagent stop:** `opencode session delete
  <sessionID>` (the session is marked cancelled). Before that, take away
  the working directory: remove the worktree (`git worktree remove --force`;
  directories created by the container as root — via sudo; when recreated
  by the agent — remove the branch and the directory, registering the
  worktree anew).
  Use when switching the model/task, for zombie agents, or when an agent
  goes off track.

### One-off tasks outside the board

Assignments outside the board (CI, infrastructure, documentation) are
executed by a subagent under the same rules; the board is not touched,
the commit has no T-number. Review — as needed, per the scheme
from "Review scheme".

## Tests

- The project checks are `make test` + `make lint` — no errors.
- `make test-race` — mandatory for tasks touching packages with goroutine
  loops/workers (the `RACE_PKGS` set in the Makefile: monitor, scheduler,
  renderworker, httpworker, dnsworker, queue, render, content): `make test`
  runs without `-race` and does not see races in tests with background
  loops (a separate step in CI).
- `make test-integration` — if migrations, the DB, queues, services, or
  the testbed are affected.
- A failing test outside the task scope — record the cause in the
  commit and a task comment on the board, in addition to the
  `DECISIONS.md` entry (`D-NNN`) per the global rules.

## Documentation

- What to update when behavior, config, project structure, migrations, or
  the Makefile change: `README.md`, the corresponding file in `docs/`.

## Commits

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
- Execute tasks from TODO top to bottom; a Stage is a grouping, not an
  execution order.
- Follow the structure, naming, and style of the existing code.
- Design and refine behavioral logic (statuses, states, lifecycles,
  retries) as an explicit finite-state machine — a fixed
  set of states, events, and a transition table of allowed transitions,
  not scattered flags and conditions (the FSM registry and the
  formalization priorities —
  `tasks/TZ-fsm.md`).
- Doubts about the spec interpretation — resolve by the spec text; briefly
  record the accepted decision in the commit — in addition to the
  `DECISIONS.md` entry (`D-NNN`) per the global rules.
- Do not break existing behavior; changes to contracts between services
  must be recorded too.

## Commands

- `make up` / `make down` / `make ps` — the environment.
- `make test` — unit tests; `make test-integration` — integration tests.
- `make mutate` — mutation testing (heavy; docs/mutation-testing.md).
- `make lint`, `make fmt` — the linter and formatting.
- `make logs SERVICE=<name>` — service logs.
- Full list: `make help`.

#!/bin/sh
# Mutation testing with go-mutesting (the avito-tech/go-mutesting fork of
# the zimmski/go-mutesting tool; how and why — docs/mutation-testing.md).
#
# Run via make mutate (the tools container, the /src worktree).
# What it does:
#  1. copies the sources into an isolated directory inside the container —
#     go-mutesting replaces files with mutants right in the tree, and the
#     bind-mounted worktree (where air/hot-reload watches it) is not touched;
#  2. runs the mutators with the built-in exec: `go test` of a package per
#     mutant (the /gomod and /gocache caches are reused);
#  3. exports the reports to var/mutation/ (the log, report.json, HTML) and
#     gates the final mutation score against the MUTATION_MIN_SCORE threshold.
#
# Variables (set by make mutate):
#   MUTATE_PKGS         — targets, local ./ package paths (mandatory);
#   MUTATE_TIMEOUT      — the go test timeout per mutant, s (60);
#   MUTATION_MIN_SCORE  — the mutation score threshold 0..1, below it — exit 1 (0);
#   MUTATE_BLACKLIST    — the file with MD5 checksums of "bogus" mutants
#                         (test/mutation/blacklist.txt, when non-empty).
set -eu

: "${MUTATE_PKGS:?MUTATE_PKGS env is required (set by make mutate)}"
MUTATE_TIMEOUT="${MUTATE_TIMEOUT:-60}"
MUTATION_MIN_SCORE="${MUTATION_MIN_SCORE:-0}"
MUTATE_BLACKLIST="${MUTATE_BLACKLIST:-test/mutation/blacklist.txt}"

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root_dir=$(CDPATH= cd -- "${script_dir}/../../.." && pwd)
out_dir="${root_dir}/var/mutation"

command -v go-mutesting >/dev/null 2>&1 || {
	echo 'ERROR: go-mutesting is not found in the image. Rebuild: make build' >&2
	exit 1
}

# An isolated copy of the sources: .git (go test does not need it), var
# (environment data, hundreds of MB) and tmp (air junk) are not copied.
work=/tmp/mutation-src
rm -rf "$work"
mkdir -p "$work" "$out_dir"
trap 'rm -rf "$work"' EXIT INT TERM
tar -C "$root_dir" --exclude=./.git --exclude=./var --exclude=./tmp -cf - . \
	| tar -C "$work" -xf -
cd "$work"

blacklist_args=''
if [ -s "$MUTATE_BLACKLIST" ]; then
	blacklist_args="--blacklist $MUTATE_BLACKLIST"
fi

# go-mutesting always exits 0 (even with a low score) — the real result is
# learned by parsing the summary line; pipefail catches panics.
set -o pipefail
go-mutesting \
	--config test/mutation/go-mutesting.yml \
	--exec-timeout "$MUTATE_TIMEOUT" \
	--html-output \
	$blacklist_args \
	$MUTATE_PKGS 2>&1 | tee "${out_dir}/mutate.log"

cp -f report.json "${out_dir}/report.json"
cp -f go-mutesting-report.html "${out_dir}/go-mutesting-report.html"

score=$(sed -n 's/^The mutation score is \([0-9][0-9.]*\).*/\1/p' \
	"${out_dir}/mutate.log" | tail -n 1)
[ -n "$score" ] || {
	echo 'ERROR: no final mutation score in the output — did go-mutesting crash?' >&2
	exit 1
}

status=0
awk -v s="$score" -v m="$MUTATION_MIN_SCORE" 'BEGIN { exit (s + 0 >= m + 0) ? 0 : 1 }' || {
	echo "ERROR: mutation score $score is below the threshold $MUTATION_MIN_SCORE." >&2
	echo 'Survivor analysis — var/mutation/go-mutesting-report.html.' >&2
	status=1
}
echo "Mutation score: $score (threshold $MUTATION_MIN_SCORE). Reports: var/mutation/"

exit $status

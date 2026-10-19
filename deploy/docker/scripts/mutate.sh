#!/bin/sh
# Мутационное тестирование go-mutesting (форк avito-tech/go-mutesting
# инструмента zimmski/go-mutesting; как и зачем — docs/mutation-testing.md).
#
# Запускается через make mutate (контейнер tools, рабочее дерево /src).
# Что делает:
#  1. копирует исходники в изолированный каталог внутри контейнера —
#     go-mutesting подменяет файлы мутантами прямо в дереве, и рабочее
#     дерево на bind-mount (где его смотрит air/hot-reload) не трогаем;
#  2. гоняет мутаторов встроенным exec: `go test` пакета на каждого
#     мутанта (кэши /gomod и /gocache переиспользуются);
#  3. выгружает отчёты в var/mutation/ (лог, report.json, HTML) и гейтит
#     итоговый mutation score по порогу MUTATION_MIN_SCORE.
#
# Переменные (задаёт make mutate):
#   MUTATE_PKGS         — цели, локальные ./-пути пакетов (обязательно);
#   MUTATE_TIMEOUT      — таймаут go test на одного мутанта, с (60);
#   MUTATION_MIN_SCORE  — порог mutation score 0..1, ниже — exit 1 (0);
#   MUTATE_BLACKLIST    — файл с MD5-чексуммами «ложных» мутантов
#                         (test/mutation/blacklist.txt, если непуст).
set -eu

: "${MUTATE_PKGS:?MUTATE_PKGS env is required (задаётся make mutate)}"
MUTATE_TIMEOUT="${MUTATE_TIMEOUT:-60}"
MUTATION_MIN_SCORE="${MUTATION_MIN_SCORE:-0}"
MUTATE_BLACKLIST="${MUTATE_BLACKLIST:-test/mutation/blacklist.txt}"

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root_dir=$(CDPATH= cd -- "${script_dir}/../../.." && pwd)
out_dir="${root_dir}/var/mutation"

command -v go-mutesting >/dev/null 2>&1 || {
	echo 'ОШИБКА: go-mutesting не найден в образе. Пересоберите: make build' >&2
	exit 1
}

# Изолированная копия исходников: .git (не нужен go test), var (данные
# окружения, сотни МБ) и tmp (мусор air) не копируем.
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

# go-mutesting всегда завершается кодом 0 (даже при низком score) —
# реальный результат узнаём парсингом строки итога; pipefail ловит паники.
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
	echo 'ОШИБКА: в выводе нет итогового mutation score — упал go-mutesting?' >&2
	exit 1
}

status=0
awk -v s="$score" -v m="$MUTATION_MIN_SCORE" 'BEGIN { exit (s + 0 >= m + 0) ? 0 : 1 }' || {
	echo "ОШИБКА: mutation score $score ниже порога $MUTATION_MIN_SCORE." >&2
	echo 'Разбор выживших мутантов — var/mutation/go-mutesting-report.html.' >&2
	status=1
}
echo "Mutation score: $score (порог $MUTATION_MIN_SCORE). Отчёты: var/mutation/"

exit $status

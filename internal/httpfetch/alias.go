package httpfetch

import (
	"strings"

	"crawler/internal/model"
)

// AliasInput — обобщённый результат проверки одного субдомена после
// вариант-логики (T-054): одна сохраняемая запись (субдомен × вариант).
// Чистая функция DetectAliases не зависит от типов вариант-логики —
// воркер (T-053) собирает вход из итогов цепочек и хэшей контента.
type AliasInput struct {
	// SubdomainID — subdomains.id ('' — апекс, id 0; миграция 000001).
	SubdomainID int16
	// Label — метка субдомена в punycode, '' — апекс (справочно;
	// собственный адрес субдомена строится как label + "." + domain).
	Label string
	// FinalHost — хост итогового URL записи (punycode, нижний регистр —
	// контракт этапа редиректов T-048); пустой — страницы нет (ошибка
	// загрузки), запись ни с кем не совпадает.
	FinalHost string
	// FinalPath — путь итогового URL; пустой считается корнем «/».
	FinalPath string
	// Scheme — схема сохранённой записи (справочно, на сравнения
	// не влияет: протокол уже свёрнут вариант-логикой).
	Scheme model.Scheme
	// Variant — вариант записи (0 основной, 1 http): участвует только
	// в детерминированном выборе оригинала класса при равенстве
	// subdomain_id.
	Variant model.Variant
	// ContentHash — 16 байт хэша контента (content.HashOf) либо nil,
	// если контента нет (не получен или не сохраняется).
	ContentHash []byte
}

// AliasResult — решение DetectAliases по записи AliasInput: вид связи
// с субдоменом-оригиналом и признак «сохранять контент». Порядок
// результата параллелен порядку входа; SubdomainID/Variant повторяют
// идентичность записи для удобства вызывающего.
type AliasResult struct {
	SubdomainID int16
	Variant     model.Variant
	// AliasKind — 0 нет, 1 идентичен, 2 редирект (ТЗ 5.5 п. 6,
	// model.AliasKind).
	AliasKind model.AliasKind
	// AliasSubdomainID — субдомен-оригинал класса (у кого хранится
	// контент); nil при AliasKind = AliasNone.
	AliasSubdomainID *int16
	// StoreContent — сохранять контент в хранилище: true только у
	// оригинала класса и только при ненулевом хэше (контент класса
	// хранится один раз, ТЗ 5.5 п. 6).
	StoreContent bool
}

// DetectAliases размечает алиасы субдоменов одного домена (ТЗ 5.5 п. 6):
// запись, у которой итоговый URL или хэш контента совпал с записью
// ДРУГОГО субдомена этого домена, помечается alias_kind и
// alias_subdomain_id, контент сохраняется один раз. Функция чистая —
// ничего не грузит и в БД не пишет; вход — записи после вариант-логики,
// включая случай единственной сохраняемой записи субдомена.
//
// Совпадения (те же критерии, что в вариант-логике T-054, но без
// зависимости от её типов):
//
//   - итоговый URL: нормализованные host+path равны (нижний регистр
//     хоста, срез завершающей точки FQDN, пустой путь — корень «/»);
//     query и фрагмент не сравниваются — счётчиковые параметры
//     (?i=1, ?i=2, …) не должны рушить алиас bare ≡ www;
//   - хэш контента: оба ненулевые и равны (парковки и заглушки
//     хостингов, ТЗ 5.7, §8).
//
// Записи, связанные любым из критериев — в том числе транзитивно
// (a=b, b=c ⇒ один класс a=b=c), — образуют класс эквивалентности.
// Варианты одного субдомена алиасами друг друга не объявляются:
// alias_subdomain_id ссылается на другой субдомен (ТЗ п. 6 «другого
// субдомена этого домена»); попав в один класс транзитивно через
// третью запись (нарушение контракта вариант-логики — варианты одного
// субдомена обязаны быть разными сайтами), такая запись остаётся
// независимой.
//
// Оригинал класса (чьей записью хранится контент) выбирается
// детерминированно — независимо от порядка завершения параллельных
// загрузок субдоменов внутри воркера (T-053):
//
//  1. запись «дома»: итоговый хост равен собственному адресу субдомена
//     (label + "." + domain) — страница в конце цепочки живёт на своём
//     адресе, на неё съехались остальные (bare → www); при нескольких
//     «домашних» (одинаковый контент без редиректа) — минимальный
//     (subdomain_id, variant);
//  2. если «дома» никого нет (все записи уехали на один внешний URL —
//     общая парковка) — минимальный (subdomain_id, variant) класса.
//
// Минимальный subdomain_id автоматически отдаёт приоритет апексу
// (id 0), затем www (id 1) — как в тестах ТЗ 11.1 (bare → www).
//
// Виды связи (model.AliasKind, ТЗ 5.5 п. 6): AliasNone — независим;
// AliasIdentical — идентичен оригиналу (тот же итоговый URL либо тот же
// хэш при своём адресе); AliasRedirect — редирект на субдомен-оригинал:
// итоговый хост записи — собственный адрес оригинала, цепочка
// съехалась прямо на него (классика bare → www: итоговый хост bare —
// адрес www, у www — свой). Вид описывает отношение записи именно к
// оригиналу из alias_subdomain_id — пара самосогласована.
//
// domain — punycode-имя домена задачи (T-053): собственные адреса
// субдоменов строятся сращиванием label + "." + domain.
func DetectAliases(domain string, in []AliasInput) []AliasResult {
	out := make([]AliasResult, len(in))
	if len(in) == 0 {
		return out
	}

	// Собственные адреса субдоменов (для «дома» и адреса оригинала).
	own := make([]string, len(in))
	for i, r := range in {
		own[i] = aliasOwnHost(domain, r.Label)
	}

	// Классы эквивалентности: union-find по совпадениям URL и хэша.
	uf := newAliasUF(len(in))
	byURL := make(map[string]int)
	byHash := make(map[string]int)
	for i, r := range in {
		if h := aliasNormHost(r.FinalHost); h != "" {
			key := h + "\x00" + aliasNormPath(r.FinalPath)
			if j, ok := byURL[key]; ok {
				uf.union(i, j)
			} else {
				byURL[key] = i
			}
		}
		if len(r.ContentHash) > 0 {
			key := string(r.ContentHash)
			if j, ok := byHash[key]; ok {
				uf.union(i, j)
			} else {
				byHash[key] = i
			}
		}
	}
	classes := make(map[int][]int, len(in))
	for i := range in {
		root := uf.find(i)
		classes[root] = append(classes[root], i)
	}

	for _, members := range classes {
		origin := aliasPickOrigin(in, own, members)
		originID := in[origin].SubdomainID
		for _, m := range members {
			r := in[m]
			res := AliasResult{
				SubdomainID: r.SubdomainID,
				Variant:     r.Variant,
				AliasKind:   model.AliasNone,
				// Без контента сохранять нечего — у одиночек и
				// оригинала без страницы (хэш nil).
				StoreContent: len(r.ContentHash) > 0,
			}
			if m != origin && originID != r.SubdomainID {
				res.StoreContent = false
				res.AliasSubdomainID = &originID
				if aliasNormHost(r.FinalHost) == own[origin] {
					// Итоговый адрес — собственный адрес оригинала:
					// запись съехалась на него цепочкой редиректов.
					res.AliasKind = model.AliasRedirect
				} else {
					res.AliasKind = model.AliasIdentical
				}
			}
			out[m] = res
		}
	}
	return out
}

// aliasPickOrigin — индекс оригинала класса: первый проход — записи
// «дома» (итоговый хост = собственный адрес субдомена), второй (никого
// дома нет — общий внешний URL) — все записи; в обоих проходах
// минимальный (subdomain_id, variant).
func aliasPickOrigin(in []AliasInput, own []string, members []int) int {
	best := -1
	for pass := 0; pass < 2 && best < 0; pass++ {
		for _, m := range members {
			atHome := aliasNormHost(in[m].FinalHost) == own[m]
			if pass == 0 && !atHome {
				continue
			}
			if best < 0 || aliasLess(in[m], in[best]) {
				best = m
			}
		}
	}
	return best
}

// aliasLess — порядок выбора оригинала при равенстве «домашности»:
// минимальный subdomain_id, затем минимальный variant (детерминизм при
// нескольких записях одного субдомена).
func aliasLess(a, b AliasInput) bool {
	if a.SubdomainID != b.SubdomainID {
		return a.SubdomainID < b.SubdomainID
	}
	return a.Variant < b.Variant
}

// aliasNormHost — нормализация хоста для сравнения: нижний регистр и
// срез завершающей точки (FQDN-форма). Хосты итоговых URL уже
// приведены к punycode этапом редиректов (T-048), повторное кодирование
// не нужно — оно отвергло бы и IP-литералы Location.
func aliasNormHost(h string) string {
	return strings.ToLower(strings.TrimSuffix(h, "."))
}

// aliasNormPath — путь итогового URL для сравнения: пустой путь —
// корень «/» (как при сериализации URL).
func aliasNormPath(p string) string {
	if p == "" {
		return "/"
	}
	return p
}

// aliasOwnHost — собственный адрес субдомена: label + "." + domain,
// апекс (label "") — само имя домена.
func aliasOwnHost(domain, label string) string {
	d := aliasNormHost(domain)
	if label == "" {
		return d
	}
	return strings.ToLower(label) + "." + d
}

// aliasUF — union-find по индексам записей: классы эквивалентности
// совпадений URL/хэша (транзитивность a=b=c).
type aliasUF struct {
	parent []int
}

func newAliasUF(n int) *aliasUF {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	return &aliasUF{parent: p}
}

func (u *aliasUF) find(x int) int {
	for u.parent[x] != x {
		u.parent[x] = u.parent[u.parent[x]] // сжатие пути
		x = u.parent[x]
	}
	return x
}

func (u *aliasUF) union(a, b int) {
	ra, rb := u.find(a), u.find(b)
	if ra == rb {
		return
	}
	u.parent[rb] = ra
}

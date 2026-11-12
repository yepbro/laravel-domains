// The landing i18n (T-122): a port of lang/{ru,en}/landing.php from
// larasites — flat dictionaries per locale, the keys are preserved (the
// form's domain part is added in T-123). The Russian locale is the
// default (a pinned stage-13 decision: i18n ru+en).
package web

// messages — the locale dictionary: key → string; everything is
// escaped by the template.
type messages map[string]string

var enMessages = messages{
	"title":         "LaraSites — Discover Laravel-powered sites",
	"description":   "LaraSites finds sites built with Laravel: instant domain check, project catalog, statistics and fresh discoveries. Subscribe to updates on Telegram.",
	"sharing.title": "LaraSites",
	"sharing.desc":  "Laravel projects, live examples and statistics.",

	"header.nav_statistics":        "Statistics",
	"header.nav_catalog":           "Catalog",
	"header.nav_latest":            "Discovered",
	"header.nav_how_it_works":      "How it works",
	"header.nav_domain_check_form": "Check domain",
	"header.nav_contacts":          "Contacts",

	"hero.title":         "We find Laravel where it is not visible",
	"hero.subtitle":      "We scan millions of domains and detect Laravel — even if it runs as API-only.",
	"hero.cta_primary":   "Check your domain",
	"hero.cta_secondary": "View discovered sites",

	"statistics.title":         "Statistics",
	"statistics.total_zones":   "Total zones",
	"statistics.total_domains": "Total domains",
	"statistics.checked":       "Domains checked",
	"statistics.with_laravel":  "Domains with Laravel found",
	"statistics.per_day":       "Found in 24 hours",

	"zone_cloud.title":    "Most popular TLDs",
	"zone_cloud.subtitle": "Hover a zone to see how many Laravel domains it holds",
	"zone_cloud.domains":  "Laravel domains",

	"zone_table.zone":       "Zone name",
	"zone_table.checked":    "Domains checked",
	"zone_table.laravel":    "Laravel domains",
	"zone_table.percentage": "Share of Laravel domains",
	"zone_table.empty":      "No zones yet.",

	"form.title":         "Check a domain for Laravel",
	"form.label":         "Instant check",
	"form.placeholder":   "домен.рф or example.com",
	"form.help":          "Domains of any level and IDN are supported.",
	"form.button":        "Check domain",
	"form.found":         "Laravel: found",
	"form.not_found":     "Laravel: not found",
	"form.error":         "Check failed — try again later",
	"form.rate_limited":  "No more than 4 checks per minute",
	"form.invalid":       "Invalid domain or URL format",
	"form.server_error":  "Server error — try again later",
	"form.details.title": "Check details",

	"latest.title":    "Latest Laravel projects found",
	"latest.subtitle": "The list grows as we scan.",
	"latest.empty":    "No domains found yet.",
	"latest.updated":  "Updated",
	"latest.catalog":  "Go to catalog",

	"how.title":    "How it works",
	"how.s1.title": "We load domain lists",
	"how.s1.desc":  "Registrar zones and public dumps give millions of names — the pipeline queues them for checks.",
	"how.s2.title": "We check DNS and subdomains",
	"how.s2.desc":  "We resolve the apex and www/api/app/crm/admin subdomains, then fetch both schemes — to find Laravel even in API-only setups.",
	"how.s3.title": "We analyze signals",
	"how.s3.desc":  "Passive analysis of already downloaded pages: XSRF-TOKEN and encrypted cookie envelopes, Vite/Mix builds, welcome pages and API JSON responses.",

	"telegram.title":    "LaraCity news on Telegram",
	"telegram.subtitle": "Parser and catalog updates, other ecosystem projects.",
	"telegram.button":   "Subscribe on Telegram",

	"catalog.title":  "Catalog of Laravel sites",
	"catalog.desc":   "Real projects, live examples on the framework.",
	"catalog.button": "Go to catalog",

	"contacts.title":    "Contacts",
	"contacts.email":    "denis@yepbro.pro",
	"contacts.telegram": "https://t.me/yepbro_en",

	"footer.copyright":      "© 2025 LaraCity",
	"footer.not_affiliated": "Not affiliated with Laravel.",
}

// localeMessages — the locale dictionary (an unknown locale — the
// default ru).
func localeMessages(locale string) messages {
	if m, ok := localeMessagesMap[locale]; ok {
		return m
	}
	return localeMessagesMap[DefaultLocale]
}

// The owner decision (stage eng-55): English is the landing content for
// both locales — the i18n mechanism (locale routing, the dictionary
// lookup, hreflang/sitemap of both locales) is kept, the /ru route serves
// the same English dictionary.
var localeMessagesMap = map[string]messages{"ru": enMessages, "en": enMessages}

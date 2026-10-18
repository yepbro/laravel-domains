// i18n лендинга (T-122): перенос lang/{ru,en}/landing.php из larasites —
// плоские словари по локалям, ключи сохранены (доменная часть формы
// дополняется в T-123). Русская локаль — дефолт (зафиксированное решение
// этапа 13: i18n ru+en).
package web

// messages — словарь локали: ключ → строка; всё экранируется шаблоном.
type messages map[string]string

var ruMessages = messages{
	"title":         "LaraSites — Открой для себя сайты на Laravel",
	"description":   "LaraSites находит сайты на Laravel: мгновенная проверка домена, каталог проектов, статистика и свежие находки. Подписывайтесь на обновления в Telegram.",
	"sharing.title": "LaraSites",
	"sharing.desc":  "Laravel-проекты, живые примеры и статистика.",

	"header.nav_statistics":        "Статистика",
	"header.nav_catalog":           "Каталог",
	"header.nav_latest":            "Уже найдено",
	"header.nav_how_it_works":      "Как это работает",
	"header.nav_domain_check_form": "Проверить домен",
	"header.nav_contacts":          "Контакты",

	"hero.title":         "Мы находим Laravel там, где его не видно",
	"hero.subtitle":      "Сканируем миллионы доменов и детектим Laravel — даже если он работает только как API.",
	"hero.cta_primary":   "Проверить свой домен",
	"hero.cta_secondary": "Посмотреть найденные сайты",

	"statistics.title":         "Статистика",
	"statistics.total_zones":   "Всего зон",
	"statistics.total_domains": "Всего доменов",
	"statistics.checked":       "Проверено доменов",
	"statistics.with_laravel":  "Найдено доменов с Laravel",
	"statistics.per_day":       "Находок за 24 часа",

	"zone_cloud.title":    "Самые популярные доменные зоны",
	"zone_cloud.subtitle": "Наведи на зону, чтобы увидеть, сколько доменов с Laravel в базе",
	"zone_cloud.domains":  "Laravel доменов",

	"zone_table.zone":       "Имя зоны",
	"zone_table.checked":    "Проверено доменов",
	"zone_table.laravel":    "Доменов с Laravel",
	"zone_table.percentage": "Процент доменов с Laravel",
	"zone_table.empty":      "Зоны ещё не добавлены.",

	"form.title":         "Проверь домен на Laravel",
	"form.label":         "Моментальная проверка",
	"form.placeholder":   "домен.рф или example.com",
	"form.help":          "Поддерживаются домены любого уровня и IDN (национальные домены).",
	"form.button":        "Проверить домен",
	"form.found":         "Laravel: найден",
	"form.not_found":     "Laravel: не найден",
	"form.error":         "Не удалось проверить — попробуйте позже",
	"form.rate_limited":  "Не более 4 проверок в минуту",
	"form.invalid":       "Неверный формат домена или URL",
	"form.server_error":  "Ошибка сервера — попробуйте позже",
	"form.details.title": "Информация о проверке",

	"latest.title":    "Последние найденные проекты на Laravel",
	"latest.subtitle": "Мы обновляем список по мере сканирования.",
	"latest.empty":    "Домены ещё не найдены.",
	"latest.updated":  "Обновлено",
	"latest.catalog":  "Перейти в каталог",

	"how.title":    "Как это работает",
	"how.s1.title": "Загружаем списки доменов",
	"how.s1.desc":  "Зоны регистраторов и публичные даммы дают миллионы имён — конвейер ставит их в очередь проверки.",
	"how.s2.title": "Проверяем DNS и поддомены",
	"how.s2.desc":  "Резолвим апекс и поддомены www/api/app/crm/admin, затем HTTP обеих схем — чтобы найти Laravel даже в API-only конфигурациях.",
	"how.s3.title": "Анализируем сигналы",
	"how.s3.desc":  "Пассивно анализируем уже скачанные страницы: куки XSRF-TOKEN и шифрованные конверты, сборку Vite/Mix, welcome-страницы и JSON-ответы API.",

	"telegram.title":    "Новости LaraCity в Telegram",
	"telegram.subtitle": "Апдейты парсера, каталога и других проектов экосистемы.",
	"telegram.button":   "Подписаться в Telegram",

	"catalog.title":  "Каталог сайтов на Laravel",
	"catalog.desc":   "Реальные проекты, живые примеры на этом фреймворке.",
	"catalog.button": "Перейти в каталог",

	"contacts.title":    "Контакты",
	"contacts.email":    "denis@yepbro.ru",
	"contacts.telegram": "https://t.me/yepbro_ru",

	"footer.copyright":      "© 2025 LaraCity",
	"footer.not_affiliated": "Not affiliated with Laravel.",
}

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

// localeMessages — словарь локали (неизвестная локаль — дефолтная ru).
func localeMessages(locale string) messages {
	if m, ok := localeMessagesMap[locale]; ok {
		return m
	}
	return localeMessagesMap[DefaultLocale]
}

var localeMessagesMap = map[string]messages{"ru": ruMessages, "en": enMessages}

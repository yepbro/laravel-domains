# TZ-ld-landing: вынос лендингов в отдельный репозиторий `ld-landing`

Версия: 0.1 (черновик на согласование, 2026-11-13).

Учёт задач: **все задачи ведутся в этом файле** (§11), с собственной
нумерацией **L-01…L-08**, отдельной от T-цикла. В `tasks/tasks.md` задачи
не дублируются; отдельные файлы задач (`tasks/task/<N>-*.md`) не создаются.
Статусы — метками `[new]` / `[in progress]` / `[done]` прямо в §11.

## 1. Контекст (по исследованию от 2026-11-13)

- Лендинг сегодня — одна страница: `internal/web/templates/landing.html` (384 строки,
  html/template) + `internal/web/static/landing.js` (249, vanilla JS) + Tailwind v4
  (`app.src.css`), вшитые в Go-бинарник через `go:embed`, отдаются сервисом `cmd/web`.
- Контент страницы — SSR из 6 SQL-запросов (`internal/web/data.go`): счётчики, облако
  зон top-50, top-таблица, последние находки, справочник поддоменов. **JSON-API для
  этих данных не существует.**
- Интерактив фронтенда — ровно один эндпоинт: `POST /api/check-domain`
  (`internal/web/api.go`; rate limit 4/мин по IP, `X-Real-IP`/`X-Forwarded-For`
  доверяются только loopback/unix-пирам).
- Локали `ru`/`en` — префиксом пути `/en`, `/ru`; словари в `internal/web/i18n.go`
  (~60 ключей; по решению eng-55 обе локали обслуживаются en-словарём).
- Прод: nginx на 155.212.174.88 (конфиг на сервере, в репо нет): `sites.laravel.city`
  (дефолт `/en`), `sites.laracity.ru` (дефолт `/ru`), оба домена сегодня отдают обе
  локали. Яндекс.Метрика №105207729 захардкожена в шаблоне; `sitemap.xml`
  генерируется на каждый запрос.
- Дашборд `internal/dashboard/` — внутренний операторский UI, **не лендинг**,
  не трогаем.

## 2. Цели

1. Отдельный **приватный** репозиторий **`ld-landing`** (gitverse): лендинги
   собираются в статический HTML независимо от Go-монорепы и деплоятся на
   разные серверы.
2. В laravel-domains остаётся **только API**: + новый stats-эндпоинт и CORS;
   лендинговый код вырезается (после переключения прод-nginx).
3. Стек нового репо: **Vue 3 + vite-ssg + TypeScript**.
4. **Язык = домен**: отдельный домен на каждый язык; в URL страниц **нет локали**
   (`/`, `/about`, … — никаких `/ru/...`, `/en/...`).
5. Лендинг может состоять из **нескольких страниц**; лендингов может быть
   несколько (монорепо на pnpm workspaces).
6. Данные статистики — клиентский `fetch GET /api/v1/landing/stats`; рядом
   с блоком доменов — **иконка обновления списка**.
7. Форма проверки домена — прямые запросы `POST /api/check-domain` в API + CORS.
8. **make-команды** для сборки и деплоя (и всех проверок качества).
9. Полный инструментарий качества: TypeScript strict, ESLint, Prettier,
   Stylelint, Vitest, Playwright, husky + lint-staged, commitlint и т.д. (§8).

## 3. Зафиксированные решения

| Развилка | Решение | Комментарий |
|---|---|---|
| SSG-фреймворк | **vite-ssg** (Vite + Vue 3) | выбор пользователя |
| Данные статистики | **клиентский fetch** | всегда свежие; HTML статичен |
| Связь с API | **прямые запросы + CORS** | без nginx-прокси; `trusted_proxies` не нужен — браузер ходит на api-домен напрямую, текущая схема доверия loopback сохраняется |
| Локаль в URL | **нет; домен на язык** | выбор пользователя; следствия — hreflang/редиректы, см. §4.3 |
| Расположение репо | **GitHub, приватный** (решение 13.11 — вместо gitverse) | `gh repo create ld-landing --private`; origin `git@github.com:yepbro/ld-landing.git`; git-автор локально как в laravel-domains |
| Простой при переходе | **ждать статику** (L-05→L-06) | решение 13.11: временный proxy на larasites и откат DNS отклонены; DNS уже переключён, лендинг в дауне до деплоя статики — L-06 становится критическим путём |

## 4. Архитектура

### 4.1 Схема

```
браузер ── https://sites.laravel.city  ──► uspromo (150.241.122.242): nginx, статика dist (locale=en)
браузер ── https://sites.laracity.ru   ──► aphrodite (85.208.85.156): nginx, статика dist (locale=ru)
браузер ── https://api9.laravel.city/api/v1/landing/stats ──► larasites (155.212.174.88): laravel-domains, cmd/web
браузер ── https://api9.laravel.city/api/check-domain     ──► larasites (155.212.174.88): laravel-domains, cmd/web
                                                     (CORS: allowlist доменов лендингов)
```

### 4.2 Сборка per-target

Статику раздают **два разных сервера** (ssh-алиасы машины сборки:
`uspromo` = root@150.241.122.242 — sites.laravel.city (en);
`aphrodite` = forge@85.208.85.156 — sites.laracity.ru (ru)). Nginx и
TLS-сертификаты на них настраиваются в рамках этой работы (L-06/L-07).
API остаётся на larasites — переезда API нет; его домен —
**`api9.laravel.city`** (A → 155.212.174.88, зона reg.ru, добавлена
13.11); дашборд остаётся на **`dashboard.laracity.ru`** (запись уже
существует: CNAME → laracity.ru → larasites). Сегодня оба лендинг-домена
терминирует nginx на larasites: A-записи sites.* переключены на новые
серверы 13.11 (reg.ru, TTL 60 c), старые vhost'ы на larasites снимаются
после стабилизации (L-07). На larasites, кроме того, нужен vhost
`api9.laravel.city` → proxy_pass 127.0.0.1:8080 + TLS (L-06).

Таргет = связка (домен, локаль, api_url, metrika_id) из `deploy/targets.yaml`
(§10). Билд выполняется **на каждый таргет отдельно** (в артефакт вшиваются:
словарь локали — `vue-i18n` с фиксированным `locale` из env; абсолютный `SITE`
для canonical/OG/sitemap; `VITE_API_URL`; `VITE_METRIKA_ID`):

```
make build TARGET=en-laravel-city   # SITE=https://sites.laravel.city LOCALE=en …
make build TARGET=ru-laracity       # SITE=https://sites.laracity.ru  LOCALE=ru …
```

Маршруты (`vue-router`) не содержат локали; страницы — `src/pages/*.vue`.
vite-ssg генерирует HTML на каждую страницу × таргет.

### 4.3 i18n, URL, SEO

- Переключатель языка = **кросс-доменные ссылки** на ту же страницу другого
  домена (карта альтернатив в конфиге таргета).
- `hreflang`: `ru` → `https://sites.laracity.ru/<path>`, `en` →
  `https://sites.laravel.city/<path>`, `x-default` → en-домен.
- `canonical` — абсолютный на свой домен; OG/Twitter-карточки — как сейчас.
- `sitemap.xml` — генерируется на билде **per домен**: все страницы без локалей,
  с `<xhtml:link rel="alternate">` на домены-альтернативы.
- `robots.txt` — статический per таргет (`Sitemap: https://<домен>/sitemap.xml`).
- Метрика — компонент `Metrika.vue`, номер счётчика из конфига таргета
  (не хардкод).
- Редиректы со старых URL — на cut-over (§11, L-07): `/{locale}/...` → новый
  адрес (301), карта в §10.

## 5. Репозиторий ld-landing

Приватный, **GitHub**: `gh repo create ld-landing --private -d "<описание>"`;
создан 13.11 — https://github.com/yepbro/ld-landing, origin
`git@github.com:yepbro/ld-landing.git`. Git-автор в локальном клоне —
как в laravel-domains (`--local`: Den Brooks / denis@yepbro.pro).

Описание (вариант RU): *«Лендинги Laravel Domains — статические сайты
(Vue 3 + vite-ssg, TypeScript): язык = домен, сборка и деплой статики на
несколько серверов»*.
Вариант EN: *“Laravel Domains landing pages — static sites (Vue 3 + vite-ssg,
TypeScript): one domain per language, build and deploy to multiple servers”*.

**Язык репо ld-landing (решение 13.11):** коммиты (conventional) и вся
документация — README.md, AGENTS.md, docs/, комментарии в коде и конфигах —
**только на английском**; описание репо на gitverse — EN-вариант.

Структура (pnpm workspaces):

```
ld-landing/
├─ apps/
│  └─ sites/                      # лендинг «sites» — перенос текущего
│     ├─ src/
│     │  ├─ pages/                # страницы лендинга: index.vue, …
│     │  ├─ components/           # DomainCheckForm.vue, ZoneCloud.vue,
│     │  │                        # StatsCounters.vue, LatestFinds.vue,
│     │  │                        # RefreshButton.vue, LanguageSwitch.vue,
│     │  │                        # Metrika.vue, AppHeader.vue, AppFooter.vue
│     │  ├─ composables/          # useLandingStats.ts, useDomainCheck.ts
│     │  ├─ locales/              # ru.json, en.json (перенос i18n.go)
│     │  ├─ layouts/DefaultLayout.vue
│     │  ├─ router.ts             # маршруты без локали + includedRoutes (SSG)
│     │  ├─ i18n.ts               # vue-i18n, locale фиксирован на билде
│     │  ├─ seo.ts                # canonical/hreflang/OG-хелперы (@unhead/vue)
│     │  └─ styles/app.src.css    # Tailwind v4 — дизайн переносится 1:1
│     ├─ vite.config.ts
│     └─ package.json
├─ packages/
│  ├─ ui/                         # общие компоненты между лендингами
│  └─ config/                     # общие tsconfig/eslint/prettier/stylelint пресеты
├─ scripts/                       # build-verify.mjs, sitemap.mjs, targets.mjs
├─ deploy/
│  ├─ targets.yaml                # таргеты деплоя (§10)
│  └─ nginx/                      # конфиги-сэмплы на каждый домен
├─ Makefile                       # §9
├─ README.md                      # §6
└─ package.json, pnpm-workspace.yaml, .editorconfig, .gitignore, .nvmrc
```

### README.md (обязательный состав)

1. Что это: лендинги Laravel Domains, статическая генерация, язык = домен.
2. Связь с API laravel-domains (`/api/v1/landing/stats`,
   `/api/check-domain`, CORS-allowlist).
3. Требования: node LTS + corepack/pnpm; `make install`.
4. Быстрый старт: `make dev TARGET=en-laravel-city`.
5. Структура монорепо (apps/packages).
6. Языки и домены: как добавить язык (словарь + таргет + домен).
7. Сборка: `make build TARGET=…`, таблица env (SITE, LOCALE, VITE_API_URL,
   VITE_METRIKA_ID).
8. Деплой: `deploy/targets.yaml`, `make deploy`, релизы и откат.
9. Качество: `make check`, `make e2e`, pre-commit хуки, commitlint.
10. Известные ограничения/связи (приватность, счётчик Метрики).

## 6. Страницы и компоненты apps/sites

- **index.vue** — перенос текущего лендинга 1:1 по дизайну (Tailwind-разметка
  из `landing.html`), секции: hero, счётчики, облако зон, top-таблица, последние
  найденные домены, «как это работает», Telegram, контакты, футер.
- **DomainCheckForm.vue** — перенос логики `landing.js`: `fetch
  {VITE_API_URL}/api/check-domain`, обработка 429/422/400/501/5xx, рендер
  результата (found/score/ver_min/ver_max/subdomains[].signals[]).
- **useLandingStats.ts** — `fetch {VITE_API_URL}/api/v1/landing/stats`
  (initial load + `refresh()`), состояние loading/error/updatedAt.
- **LatestFinds.vue + RefreshButton.vue** — блок «последние найденные домены»
  с **иконкой обновления**: SVG-иконка, по клику `refresh()` (refetch),
  вращение на время запроса, `disabled` пока идёт запрос, `aria-label`/
  `title` из словаря («Обновить список»), рядом относительное время
  «обновлено N мин назад».
- **Metrika.vue** — асинхронная загрузка счётчика по `VITE_METRIKA_ID`
  (поведение как сейчас: webvisor, trackHash, clickmap и т.д. + noscript).
- **LanguageSwitch.vue** — ссылки на домены-альтернативы (та же страница).
- Задел под доп. страницы: `router.ts` + `seo.ts` сразу поддерживают
  многостраничность (sitemap/hreflang на каждую).

## 7. API-контракты (laravel-domains)

### 7.1 GET /api/v1/landing/stats — задача L-03

- JSON-эквивалент текущей `landingVM`: счётчики (зоны, laravel за 24 ч),
  облако зон top-50, top-таблица, последние находки, справочник поддоменов.
- Реализация: переиспользовать SQL из `internal/web/data.go` (data.go остаётся
  источником; рендер в шаблон уходит).
- Заголовки: `Cache-Control: public, max-age=60`; `Content-Type: application/json`.
- Тесты: JSON-схема ответа, пустая БД, ошибка БД → 500 без деталей.
- Доки: `docs/services.md` (+ раздел API лендинга).

### 7.2 CORS для /api/* — задача L-04

- Конфиг: `web.cors.allowed_origins` (список точных origin; env
  `CRAWLER_WEB_CORS_ALLOWED_ORIGINS`, запятая). Пустой список = CORS выключен
  (текущее поведение).
- Middleware на `/api/*`: `Access-Control-Allow-Origin` — только точное
  совпадение из списка (не `*`), `Vary: Origin`, `Access-Control-Allow-Methods:
  GET, POST, OPTIONS`, `Access-Control-Allow-Headers: Content-Type`,
  `Access-Control-Max-Age: 600`.
- `OPTIONS /api/check-domain` (preflight, fetch с `Content-Type:
  application/json`) → `204` с заголовками; чужой origin — без CORS-заголовков.
- Тесты: разрешённый/чужой origin, префлайт, отсутствие заголовков при пустом
  конфиге. Доки: `docs/configuration.md` (web-секция), `docs/services.md`.

### 7.3 Что удаляется из laravel-domains — задача L-08 (после cut-over)

- `internal/web`: лендинг-маршруты (`GET /{locale}`, `/static/*`, `/`,
  `/robots.txt`, `/sitemap.xml`), `go:embed templates|static`, `i18n.go`,
  `templates/landing.html`, `static/*` (favicon, robots, webmanifest, js, css),
  лендинг-тесты (`web_test.go` лендинг-сценарии, `data_test.go` — кроме
  покрытий stats-эндпоинта).
- `Makefile`: таргет `web-css`; `deploy/docker/Dockerfile.go`: stage `css`;
  `.github/workflows/deploy.yml`: шаг «Build landing page CSS».
- Доки: `README.md`, `docs/services.md`, `docs/deploy.md`,
  `docs/configuration.md` — секции лендинга.
- Остаётся: `api.go`, `data.go` (источник stats), `metrics.go`, таблица
  `web_checks`, `cmd/web` как API-only сервис (переименование в `cmd/api` —
  вне этого плана).

## 8. Инструментарий качества (ld-landing, «по полной программе»)

| Слой | Инструмент | Замечания |
|---|---|---|
| Язык | TypeScript 5.x, `strict: true` + `noUncheckedIndexedAccess`, `moduleResolution: bundler` | везде, включая компоненты |
| Типы .vue | `vue-tsc --noEmit` | `make typecheck` |
| Lint | ESLint 9 (flat config): typescript-eslint (strict), eslint-plugin-vue (`vue3-recommended`), eslint-plugin-import-x (порядок импортов, дубли) | общий пресет в `packages/config` |
| Форматирование | Prettier 3 + `.prettierignore` | `make fmt` / CI-проверка `--check` |
| CSS | Stylelint 16 (standard) с разрешением tailwind-директив (`@source`, `@theme` и т.п.) | на `app.src.css` и стили в .vue |
| Unit-тесты | Vitest + @vue/test-utils + happy-dom | composables (мок fetch), компоненты (DomainCheckForm: 429/422/success; RefreshButton: клик → refetch, disabled, aria) |
| E2e smoke | Playwright против `vite preview` собранного dist | страницы открываются, hreflang/canonical/sitemap на месте, RefreshButton дергает API (route-mock), форма отправляет запрос |
| Сборка-верификация | `scripts/build-verify.mjs` | в dist есть index.html, sitemap.xml (все страницы SITE), robots.txt, hreflang на альтернативные домены, в разметке нет внутренних ссылок с /ru//en |
| Pre-commit | husky + lint-staged (eslint --fix + prettier по стейджам) | |
| Commits | commitlint (config-conventional) | `feat:`, `fix:`, `docs:` … |
| Окружение | corepack + `packageManager`, `engines` (node/pnpm), `.nvmrc`, `.editorconfig` | версия node зафиксирована |
| Доп. (опц.) | knip (мёртвые экспорты), Renovate | включить после стабилизации каркаса |

CI (GitHub Actions) — те же `make check` + `make build-all`
+ `make verify`; до включения — обязательные локальные проверки перед деплоем
(`make deploy` сам прогоняет `check → build → verify`, см. §9).

## 9. Make-команды (ld-landing)

| Команда | Что делает |
|---|---|
| `make install` | corepack/pnpm install |
| `make dev TARGET=<id>` | dev-сервер с env таргета |
| `make build TARGET=<id>` | vite-ssg build → `apps/sites/dist` (+ sitemap.mjs) |
| `make build-all` | все таргеты из `deploy/targets.yaml` |
| `make preview TARGET=<id>` | `vite preview` собранного dist |
| `make lint` / `make lint:fix` | ESLint |
| `make fmt` / `make fmt:check` | Prettier |
| `make stylelint` | Stylelint |
| `make typecheck` | vue-tsc |
| `make test` / `make test:watch` | Vitest |
| `make e2e` | Playwright smoke (после build) |
| `make check` | typecheck + lint + stylelint + fmt:check + test — обязательный прогон |
| `make verify TARGET=<id>` | build-verify.mjs по dist |
| `make deploy TARGET=<id>` | `check → build → verify → rsync` (§10) |
| `make deploy-all` | все таргеты последовательно |
| `make rollback TARGET=<id>` | symlink на предыдущий релиз (§10) |
| `make new-landing NAME=<x>` | каркас нового лендинга в `apps/<x>` |

## 10. Деплой

`deploy/targets.yaml` (схема расширяется на N серверов):

```yaml
targets:
  en-laravel-city:
    site: https://sites.laravel.city
    locale: en
    api_url: https://api9.laravel.city
    metrika_id: "105207729"
    alternatives: { ru: https://sites.laracity.ru }
    servers:
      - host: uspromo                     # root@150.241.122.242
        docroot: /var/www/sites.laravel.city   # releases/ + current (§ниже)
  ru-laracity:
    site: https://sites.laracity.ru
    locale: ru
    api_url: https://api9.laravel.city
    metrika_id: "105207729"
    alternatives: { en: https://sites.laravel.city }
    servers:
      - host: aphrodite                   # forge@85.208.85.156
        docroot: /var/www/sites.laracity.ru
```

- `make deploy`: `make check` → build таргета → verify → `rsync -az --delete
  dist/ <host>:<docroot>/releases/<ts>/` → переключение symlink `current` →
  healthcheck (`curl -fsS https://<site>/` из таргета). Откат — `make rollback`
  (symlink на предыдущий релиз).
- nginx-конфиги `deploy/nginx/sites.laravel.city.conf` (и .ru): `root
  <docroot>/current; index index.html; try_files $uri $uri/ =404;` + TLS +
  редиректы. Настройка nginx на **uspromo** и **aphrodite** входит в работу
  (L-06): vhost, docroot (владелец forge), выпуск TLS-сертификатов certbot'ом.
  Сертификаты: HTTP-01 валидация требует, чтобы домен уже смотрел на новый
  сервер → порядок: vhost по HTTP → переключение DNS (TTL снизить заранее) →
  `certbot --webroot` → включение HTTPS (либо DNS-01 без ожидания DNS);
  стенд-проверка до DNS — через /etc/hosts или временный поддомен.
- **Карта 301-редиректов на cut-over** (SEO, старые `/en`, `/ru` канонические
  адреса существуют в поиске):
  - `sites.laravel.city`: `/en` и `/en/(.*)` → 301 `/$1`; `/ru` и `/ru/(.*)`
    → 301 `https://sites.laracity.ru/$1`; `= /` больше никуда не редиректит
    (отдаёт страницу).
  - `sites.laracity.ru`: `/ru...` → 301 `/$1`; `/en...` → 301
    `https://sites.laravel.city/$1`.

## 11. Задачи (учёт — только в этом файле)

Задачи laravel-domains (L-03, L-04, L-08) выполняются по механике цикла
AGENTS.md (отдельный worktree, ревью по схеме, один коммит на задачу с
номером в скобках: `feat: … (L-03)`), но статусы и учёт — здесь, не в
`tasks/tasks.md`. Задачи ld-landing (L-01, L-02, L-05) — работа прямо
в новом репо; L-06, L-07 — операторские (сервер).

| № | Статус | Где | Что | Зависимости |
|---|---|---|---|---|
| L-01 | [done] | ld-landing | Каркас готов: github.com/yepbro/ld-landing (private, main, 4 коммита), make check/build/build-all/verify/e2e зелёные | — |
| L-02 | [done] | ld-landing | Лендинг перенесён (5b1951f, запушено): компоненты/словари/Metrika/seo/sitemap, 18 юнит-тестов, verify 11/11, e2e 2/2 | L-01 |
| L-03 | [done] | laravel-domains | `GET /api/v1/landing/stats` (§7.1) — слит в master (3911ece), ревью P2/P3-фиксы применены | — (параллельно L-01/L-02) |
| L-04 | [done] | laravel-domains | CORS для `/api/*` — слит в master (4726aea); `web.cors.allowed_origins` + env, префлайт 204, строгий Validate; ревью-фиксы применены | L-03 (обе в `internal/web`) |
| L-05 | [done] | ld-landing | Деплой-механика готова (87ef8ae, запушено): deploy/release/rollback/releases, ControlMaster-ssh, prune keep-5; релизы залиты на оба сервера, current не тронут; 41 тест | L-02, L-03, L-04 |
| L-06 | [done] | uspromo, aphrodite | **Лендинги в проде 13.11**: релиз API `200792a..29f6a9e` (L-03+L-4, deploy OK); `web.cors.allowed_origins` включён на larasites (config.bak-20261113-cors, префлайт 204/ACAO-эхо проверены); `make release` — релизы 20261113-201209 (uspromo) / 20261113-201241 (aphrodite); чеки зелёные: canonical/hreflang/sitemap/robots, 301-карта, api9 в бандле, браузерный e2e (живые stats, refresh, метрика; единственный console-error — внешний маячок mdd.yandex.net Метрики) | L-05 |
| L-07 | [done] | серверы | Наблюдение чистое (5xx нет, боты режутся deny-правилом, Метрика №105207729 принимает данные, метрики web в норме); старые лендинг-vhost'ы sites.* сняты из `crawler.conf` (бэкап `/etc/nginx/backups/`), общий LE-серт перевыпущен без sites.* (5 имён), корневые редиректы/дашборд/api9 проверены; access-лог добавлен на uspromo | L-06 |
| L-08 | [in progress] | laravel-domains | Вырезание лендинга (§7.3), обновление доков; после слияния и зелёных тестов — **сразу релиз API-only в прод** (разрешение пользователя, отменён двухчасовой таймер) | L-07 |

### 11.1. Выполнено досрочно: nginx + TLS (13.11)

Настройка серверов из L-06 выполнена заранее, чтобы L-06 свёлся к деплою
статики. Фактическое состояние (для реализации L-05):

- **uspromo** (`root@150.241.122.242`, nginx 1.24): vhost
  `/etc/nginx/sites-available/sites.laravel.city` — 80: ACME + редирект;
  443: `root /var/www/sites.laravel.city/current`, `try_files`,
  карта 301 (`/en*` → `/`, `/ru*` → sites.laracity.ru), кэш `^/assets/` 1y
  immutable. LE-сертификат `sites.laravel.city` (webroot
  `<docroot>/shared/acme`). Docroot: `releases/0-init/` (английская
  заглушка), `current → releases/0-init`, владелец `forge`.
- **aphrodite** (`forge@85.208.85.156`, nginx 1.30, Forge-стиль): vhost
  `/etc/nginx/sites-available/sites.laracity.ru` (enabled-симлинк
  `50-sites.laracity.ru`) — симметрично, карта 301 зеркальная (`/ru*` → `/`,
  `/en*` → sites.laravel.city). LE-сертификат свой. Docroot
  `/var/www/sites.laracity.ru/...`, владелец `forge`, русская заглушка.
- **larasites** (`root@155.212.174.88`): vhost
  `/etc/nginx/sites-available/api9.laravel.city` с отдельным
  LE-сертификатом; проксирует **только** `/api/` и `/healthz` →
  `127.0.0.1:8080` (cmd/web), прочее — 404 (лендинг не отдаётся под
  api9-хостом). Старый `crawler.conf` не тронут. Проверено: `/healthz` 200,
  `GET /api/check-domain` 405, `/` 404, сертификат `CN=api9.laravel.city`.
- Деплой статики (L-05) должен: `rsync dist/ → <docroot>/releases/<ts>/`,
  переключить symlink `current`, healthcheck `https://<site>/`; откат —
  symlink на предыдущий релиз. Vite-артефакты класть в `assets/` (под
  кэш-правило уже существующего location).

## 12. Критерии приёмки

- [ ] ld-landing приватный, собирается `make build-all` в статику по всем
      таргетам; `make check` зелёный (typecheck+lint+stylelint+prettier+test),
      e2e зелёные.
- [ ] В URL страниц нет `/ru`/`/en`; каждый домен отдаёт свой язык; переключатель
      языка ведёт на тот же путь другого домена.
- [ ] Страницы в поиске не теряются: 301 со старых `/{locale}/...` работают,
      canonical/hreflang/sitemap/robots корректны на обоих доменах.
- [ ] Блок доменов: иконка обновления перечитывает `/api/v1/landing/stats`,
      состояние загрузки/ошибки отражается, доступность (aria) в порядке.
- [ ] Форма проверки домена работает с браузера на api-домен: CORS, префлайт,
      429/422/… обрабатываются как раньше; rate limit по-прежнему на клиента.
- [ ] Метрика фиксирует посещения обоих доменов после cut-over (сверка с
      до/после).
- [ ] В laravel-domains лендинг-код удалён; `make test`/`make lint`/`make
      test-integration` зелёные; доки актуальны; API (`/api/check-domain`,
      `/api/v1/landing/stats`) работает.

## 13. Открытые вопросы (нужны решения до L-05/L-06)

1. ~~api-домен~~ — **решено 13.11**: `api9.laravel.city` (A → larasites
   155.212.174.88, добавлена в зону reg.ru, TTL 60). Allowlist L-04:
   `https://sites.laravel.city`, `https://sites.laracity.ru`.
2. **Метрика**: один счётчик №105207729 на оба домена (как сейчас) или
   раздельные счётчики на язык?
3. **Deploy-пользователь и docroot**: серверы доступны с машины сборки
   (`uspromo` = root@150.241.122.242, `aphrodite` = forge@85.208.85.156);
   подтвердить docroot `/var/www/<домен>` (владелец forge) и пользователя
   деплоя (root / forge / отдельный deploy).
4. **Сертификаты** (DNS решено): зоны `laravel.city`/`laracity.ru`
   управляются в reg.ru (через MCP); A-записи sites.* переключены на новые
   серверы 13.11 (TTL 60) — HTTP-01/certbot доступен сразу, стенд-проверка
   до переключения DNS не нужна.
5. **Состав страниц**: известны ли страницы сверх `index` (about, pricing…)
   или сначала задел архитектуры.
6. ~~Зеркалировать ли ld-landing на GitHub~~ — **решено 13.11**: репозиторий
   живёт на GitHub (`yepbro/ld-landing`, приватный), gitverse не
   используется.

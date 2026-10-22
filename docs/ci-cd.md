# CI/CD: боевой деплой через GitHub Actions

Непрерывный деплой боевого сервера (дополнение к T-135): после зелёного
CI на `master` тот же коммит собирается в статические бинарники и
устанавливается на сервер systemd-деплоя ([`deploy.md`](deploy.md)).
Конфигурации сервера (`/etc/crawler`, юниты, nginx, сертификаты) конвейер
не трогает — только `/opt/crawler/bin`.

## Конвейер

```
push в master ──▶ ci (тесты, lint, интеграция)
                      │ успех
                      ▼
                 deploy (workflow_run)
                      │ build (setup-go + make web-css + go build)
                      │ upload (scp по deploy-ключу)
                      │ install (deploy/deploy-install.sh: бэкап → замена → рестарт)
                      ▼
                 healthcheck (deploy/deploy-healthcheck.sh)
                      │ провал → откат на /opt/crawler/bin.prev
                      ▼
                 production (10 юнитов, healthz 9100…9108)
```

| Файл | Назначение |
|---|---|
| [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml) | workflow: запуск после успеха `ci` на `master` (или вручную), сборка, деплой |
| [`deploy/deploy-install.sh`](../deploy/deploy-install.sh) | серверная сторона: атомарная замена `/opt/crawler/bin` с откатом |
| [`deploy/deploy-healthcheck.sh`](../deploy/deploy-healthcheck.sh) | серверная сторона: 10 юнитов active + healthz + crawlerctl |

Принципы:

- **деплоится ровно тот SHA, что протестирован** — `workflow_run` передаёт
  `head_sha` прошедшего ci рана, checkout идёт по нему;
- **деплой бинарников — конвейером, деплой конфигурации — оператором**:
  `/etc/crawler/*`, `/etc/systemd/system/crawler-*`, nginx и сертификаты
  меняются вручную по [`deploy.md`](deploy.md) (редкие операции);
- **откат автоматический**: предыдущие бинарники остаются в
  `/opt/crawler/bin.prev`; любая ошибка установки до успешного старта
  восстанавливает их и перезапускает набор (trap в `deploy-install.sh`);
  healthcheck после установки при провале ран красный — набор уже откачен;
- **Chromium не переустанавливается**: версия зафиксирована
  (`CHROMEDP_HEADLESS_IMAGE` в `.env.example`), обновление — ручной шаг
  [`deploy.md` §4.3](deploy.md).

## Секреты репозитория (Settings → Secrets → Actions)

| Секрет | Содержимое |
|---|---|
| `DEPLOY_SSH_KEY` | закрытый ed25519-ключ деплоя (публичная половина — в `/root/.ssh/authorized_keys` сервера) |
| `DEPLOY_SSH_KNOWN_HOSTS` | строка `ssh-keyscan -t ed25519 <host>` — защита от MITM в CI |

Адрес сервера указан в workflow открыто (не секрет).

## Ручной запуск и операции

```bash
# запустить деплой текущего master без ожидания ci (GitHub → Actions → deploy → Run workflow)
gh workflow run deploy

# на сервере: предыдущие бинарники (последний успешный деплой)
ls /opt/crawler/bin.prev

# откат вручную
systemctl stop crawler.target
rm -rf /opt/crawler/bin && cp -a /opt/crawler/bin.prev /opt/crawler/bin
systemctl start crawler.target
```

Первый деплой нового сервера — по [`operations.md` §3](operations.md)
(вручную); CI/CD подключается после заведения секретов и ключа.

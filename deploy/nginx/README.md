# Публичный вход: nginx + TLS (образец, T-124)

Лендинг (`cmd/web`, `web.listen`) — публичная точка входа системы для
посетителей. Сервис слушает loopback (дефолт `127.0.0.1:8080`), наружу его
публикует reverse-proxy, терминирующий TLS. До этапа 13 публичного входа
в репозитории не было — этот каталог фиксирует первый образец.
Операторский дашборд публикуется той же схемой отдельным server-блоком
(docs/deploy.md §6, ТЗД §9.1).

| Файл | Назначение |
|---|---|
| `crawler.example.conf` | сервер nginx: HTTP→HTTPS, TLS, проксирование на `web.listen` |

## Установка

```bash
apt install nginx
cp crawler.example.conf /etc/nginx/sites-available/crawler.conf
ln -s /etc/nginx/sites-available/crawler.conf /etc/nginx/sites-enabled/
# подставить server_name и пути сертификатов (например, certbot --nginx)
nginx -t && systemctl reload nginx
```

Модель доступа (ТЗ §9): наружу — только лендинг; метрики и пробы сервиса
web живут на loopback `127.0.0.1:9107` (`METRICS_ADDR` юнита
`crawler-web.service`, docs/deploy.md) и через прокси не публикуются.
Рейт-лимит формы (`web.rate_limit`, токен-бакет на IP) держит сам сервис:
`X-Real-IP` в образце передаёт ему адрес посетителя (сервис доверяет
заголовку от loopback-пира — nginx перезаписывает его `$remote_addr`),
лимиты nginx не дублируют (одна точка учёта).

## Вариант unix-сокета

Вместо TCP-loopback можно слушать unix-сокет (по месту деплоя): в
`/etc/crawler/crawler.env` задать `CRAWLER_WEB_LISTEN=/run/crawler-web/web.sock`
(юнит создаёт каталог `RuntimeDirectory=crawler-web`; конфиг принимает
абсолютный путь как форму `web.listen`) и в nginx —
`proxy_pass http://unix:/run/crawler-web/web.sock;`. Сокет создаётся с
правами группы сервиса (`UMask=0007` юнита → `0770 crawler:crawler`),
поэтому воркерам nginx нужен доступ: `usermod -aG crawler www-data &&
systemctl reload nginx`. Разницы в
безопасности нет (loopback и сокет — оба «только этот хост»), выбор —
вкусовое предпочтение не занимать TCP-порт.

## Схема для sitemap.xml

`GET /sitemap.xml` лендинга собирает абсолютные URL из запроса: за этим
прокси схему передаёт `X-Forwarded-Proto`, хост — `Host` (оба заголовка
уже в образце). Без них sitemap честно получит `http://` — публичный вход
обязан передавать заголовки, если меняете конфиг.

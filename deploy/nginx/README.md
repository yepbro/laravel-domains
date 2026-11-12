# Public entrance: nginx + TLS (sample, T-124)

The landing page (`cmd/web`, `web.listen`) is the system's public entrance
for visitors. The service listens on loopback (the default `127.0.0.1:8080`);
a reverse proxy terminating TLS publishes it outward. Until stage 13 there
was no public entrance in the repository — this directory records the first
sample. The operator dashboard is published by the same scheme in a
separate server block (docs/deploy.md §6, dashboard spec §9.1).

| File | Purpose |
|---|---|
| `crawler.example.conf` | the nginx server: HTTP→HTTPS, TLS, proxying to `web.listen` |

## Installation

```bash
apt install nginx
cp crawler.example.conf /etc/nginx/sites-available/crawler.conf
ln -s /etc/nginx/sites-available/crawler.conf /etc/nginx/sites-enabled/
# substitute server_name and the certificate paths (for example, certbot --nginx)
nginx -t && systemctl reload nginx
```

The access model (spec §9): only the landing page goes outward; the web
service's metrics and probes live on loopback `127.0.0.1:9107`
(`METRICS_ADDR` of the `crawler-web.service` unit, docs/deploy.md) and are
not published through the proxy. The form's rate limit (`web.rate_limit`,
a token bucket per IP) is enforced by the service itself: `X-Real-IP` in
the sample passes it the visitor's address (the service trusts the header
from a TCP loopback peer and from a unix-socket peer — nginx overwrites it
with `$remote_addr`); the nginx limits do not duplicate it (a single
accounting point).

## The unix-socket variant

Instead of TCP loopback, the service can listen on a unix socket (at the
deploy site): set `CRAWLER_WEB_LISTEN=/run/crawler-web/web.sock` in
`/etc/crawler/crawler.env` (the unit creates the directory
`RuntimeDirectory=crawler-web`; the config accepts an absolute path as a
form of `web.listen`) and in nginx —
`proxy_pass http://unix:/run/crawler-web/web.sock;`. The socket is created
with the service group's permissions (the unit's `UMask=0007` →
`0770 crawler:crawler`), so the nginx workers need access:
`usermod -aG crawler www-data && systemctl reload nginx`. There is no
security difference (loopback and the socket are both "this host only");
the choice is a matter of taste about not occupying a TCP port.

The proxy must pass `X-Real-IP` on the socket too: a unix-socket peer's
RemoteAddr carries no IP (the "@" of autobind), and without the header the
visitor's address is unknown — the form's rate limit shares one bucket
among all such clients, and the `web_checks` manual-check log
(`ip_address inet NOT NULL`) skips the row with a Warn to journald (also
visible as the `crawler_web_check_errors_total{type="non_ip"}` counter).
The fallback source is `X-Forwarded-For` without `X-Real-IP`: the last
position of the list is taken as the address (it was appended by this
proxy, `$proxy_add_x_forwarded_for`; the head of the list came from the
client and is not trusted — otherwise the rate limit is bypassed).

## The scheme and host for sitemap.xml

The landing page's `GET /sitemap.xml` builds absolute URLs: the host is
the canonical `web.public_host` from the configuration (T-227: behind the
proxy the request's `Host` is the visitor's `$host`; without the canon
set, `<loc>` would depend on a foreign header; an empty field — a fallback
to the request's `Host`); behind this proxy the scheme is passed by
`X-Forwarded-Proto` — the service trusts the header only from a loopback
peer and a unix-socket peer (like `X-Real-IP`); a foreign value is ignored
and the connection's scheme stays. The public entrance must pass both
headers and set `web.public_host` if you change the config.

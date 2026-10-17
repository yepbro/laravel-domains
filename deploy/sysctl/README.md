# Применение sysctl-тюнинга краулера

`99-crawler.conf` — drop-in для sysctl.d(5): параметры ядра боевого
сервера под нагрузку краулера с одного IP (ТЗ §10.2). Обоснование и расчёт
каждого значения — [`docs/os-tuning.md`](../../docs/os-tuning.md).

Применять на боевом сервере (Ubuntu LTS, systemd; ТЗ §10.1). Dev-окружение
в Docker контейнерах изолировано и этих настроек не требует — файл нужен
хосту только для нагрузочных тестов, приближенных к боевым
([`docs/dev-environment.md` §9](../../docs/dev-environment.md)).

## Установка и применение

```bash
# от root (или через sudo)
install -m 0644 99-crawler.conf /etc/sysctl.d/99-crawler.conf

# применить все файлы /etc/sysctl.d (порядок лексикографический)
sysctl --system
```

Персистентность: `/etc/sysctl.d` читается `systemd-sysctl.service` при
каждой загрузке — для большинства ключей после копирования ничего
дополнительно включать не нужно. Исключение — conntrack, см. ниже.

Строки `net.netfilter.*` применятся, только если модуль `nf_conntrack`
уже загружен (правила iptables/nftables, Docker). Без модуля ключей в
`/proc/sys` нет и `systemd-sysctl` пропускает их с предупреждением — это
не ошибка. Важно для персистентности: `systemd-sysctl` отрабатывает на
раннем этапе загрузки, и если модуль к тому моменту никто не загрузил,
значения не применятся вовсе (модуль, поднятый позже Docker'ом, получит
дефолты). Поэтому вместе с файлом включается автозагрузка модуля —
`systemd-modules-load` выполняется до `systemd-sysctl`:

```bash
# применить сейчас и на каждой загрузке
modprobe nf_conntrack
echo nf_conntrack | tee /etc/modules-load.d/nf_conntrack.conf
sysctl --system
```

## Хэш-таблица conntrack на ядрах < 5.15

`net.netfilter.nf_conntrack_buckets` записывается через sysctl только с
ядра 5.15; на старых LTS-ядрах размер задаётся параметром модуля —
`/etc/modprobe.d/nf_conntrack-crawler.conf`:

```
options nf_conntrack hashsize=262144
```

(действует со следующей загрузки модуля). Изменить размер на лету, не
выгружая модуль (на боевом сервере с живым трафиком выгрузка невозможна —
`Module nf_conntrack is in use`), можно записью в sysfs — интерфейс
доступен на всех ядрах:

```bash
echo 262144 > /sys/module/nf_conntrack/parameters/hashsize
```

## Проверка

```bash
# значения применены
sysctl net.ipv4.ip_local_port_range net.ipv4.tcp_tw_reuse \
       net.ipv4.tcp_max_tw_buckets net.core.somaxconn fs.file-max \
       net.netfilter.nf_conntrack_max net.core.rmem_max

# всё ядро целиком (медленно, для сверки после инцидентов)
sysctl -a | less

# под нагрузкой: TIME_WAIT в норме не приближается к tcp_max_tw_buckets
ss -tan state time-wait | wc -l

# conntrack: count не должен приближаться к max
cat /proc/sys/net/netfilter/nf_conntrack_count

# дропы на переполнении (должно быть пусто)
dmesg | grep -E "table full|time wait bucket"

# лимиты дескрипторов процесса (LimitNOFILE — юниты T-085)
cat /proc/$(pidof dns-worker)/limits | grep "open files"
```

Рост `nf_conntrack_count` к максимуму или записи `table full` в dmesg —
сигнал пересчитать таблицу (docs/os-tuning.md, «Conntrack»).

## Откат

Все ключи файла обратимы на лету (`sysctl -w`) — перезагрузка не нужна;
действующие дефолты конкретного сервера смотрите `sysctl -a` до
применения файла. Ориентиры дефолтов ядра:

| Ключ | Файл ставит | Дефолт ядра |
|---|---|---|
| net.ipv4.ip_local_port_range | 10240 65535 | 32768 60999 |
| net.ipv4.tcp_tw_reuse | 1 | 2 (ядра ≥ 4.12) |
| net.ipv4.tcp_timestamps | 1 | 1 |
| net.ipv4.tcp_max_tw_buckets | 262144 | зависит от RAM (~65k–262k) |
| net.core.somaxconn | 4096 | 128 / 4096 (от ядра) |
| net.ipv4.tcp_max_syn_backlog | 4096 | 128–2048 |
| net.core.netdev_max_backlog | 4096 | 1000 |
| fs.file-max | 2097152 | LONG_MAX (новые) / по RAM (старые) |
| net.netfilter.nf_conntrack_max | 1048576 | по RAM (~262k на 8 ГБ) |
| net.netfilter.nf_conntrack_buckets | 262144 | max/4 |
| net.core.rmem_max / wmem_max | 16777216 | 212992 |

```bash
# вернуть отдельный ключ
sysctl -w net.ipv4.ip_local_port_range="32768 60999"

# убрать всё, что ставит файл (после — при желании вернуть дефолты
# по таблице выше)
rm /etc/sysctl.d/99-crawler.conf /etc/modules-load.d/nf_conntrack.conf
```

Полная гарантия чистого состояния — перезагрузка после удаления файла;
для целей отката и отладки достаточно `sysctl -w`.

# Применение мониторинга SMART/TBW NVMe (T-089)

Комплект боевого мониторинга накопителя сервера (ТЗ §10.3: «мониторинг
SMART и износа NVMe (TBW), так как нагрузка записи высокая»). Обоснование
раскладки дисков, оценка TBW от нагрузки краулера и связь слоёв мониторинга —
[`docs/disks.md`](../../docs/disks.md). Применять на боевом сервере
(Ubuntu Server LTS, systemd; ТЗ §10.1).

| Файл | Назначение |
|---|---|
| `smartd.conf` | конфиг smartd: NVMe-проверки, предупреждения в syslog/journald |
| `smartd-warning.sh` | обработчик предупреждений smartd (ставится в `/usr/local/lib/crawler`) |
| `99-crawler-nvme.rules` | udev: узлы `/dev/nvme*` — группе `nvme-smart` |
| `crawler-monitor.service.d/` | drop-in юнита monitor: supplementary-группа `nvme-smart` |

Слои: smartd (демон пакета, root) — критические предупреждения здоровья,
рост журнала ошибок, температура; алерт `nvme_wear` сервиса monitor
(T-080) — percentage_used ≥ 90 % и TBW в деталях, работает после выдачи
доступа к устройствам (правила + drop-in). Оба пишут в journald.

## Установка

```bash
apt install smartmontools

# 1. smartd: наш конфиг вместо образца пакета (оригинал сохранить)
cp -a /etc/smartd.conf /etc/smartd.conf.orig
install -m 0644 smartd.conf /etc/smartd.conf

# 2. обработчик предупреждений (путь — из smartd.conf, -M exec)
install -D -m 0755 smartd-warning.sh /usr/local/lib/crawler/smartd-warning.sh

# 3. доступ алерта мониторинга к NVMe (слой 2, docs/disks.md)
groupadd -r nvme-smart
install -m 0644 99-crawler-nvme.rules /etc/udev/rules.d/99-crawler-nvme.rules
udevadm control --reload && udevadm trigger --subsystem-match=nvme
cp -r crawler-monitor.service.d /etc/systemd/system/
systemctl daemon-reload

# 4. запуск
systemctl enable --now smartmontools      # алиас юнита: smartd.service
systemctl restart crawler-monitor
```

Шаг 3 и drop-in можно пропустить: алерт износа останется в честном
«не измеряется», smartd продолжит работать (см. границы слоёв в
docs/disks.md — на smartmontools 7.4 шаг 3 нужен для порога TBW).

## Проверка

```bash
systemctl status smartmontools            # активен; найдено устройств: 1
journalctl -t smartd -b                   # регистрация NVMe без предупреждений

# живые показания — те же страницы, что читают smartd и алерт monitor
smartctl -H -A /dev/nvme0                 # health, percentage_used, TBW
smartctl -l error /dev/nvme0              # журнал ошибок NVMe

# доступ слоя monitor (алерт открывает узел на запись — ioctl admin)
ls -l /dev/nvme0                          # root:nvme-smart 0660
sudo -u crawler-monitor test -r /dev/nvme0 -a -w /dev/nvme0 && echo доступ есть
journalctl -t crawler-monitor | grep -i алерт   # Warn при срабатывании (≥90 %)
```

Состояние алерта «не измеряется» видно при `CRAWLER_LOG_LEVEL=debug`
(env сервиса): до включения доступа каждая оценка пишет Debug «алерт не
измеряется: износ NVMe», после — записи нет; срабатывание (≥ 90 % ресурса)
пишется Warn всегда.

Тест обработчика: добавить в `/etc/smartd.conf` к строке DEVICESCAN
директиву `-M test` и перечитать конфиг (`systemctl kill -s HUP
smartmontools`) — в `journalctl -t smartd` появится запись EmailTest
(info), затем директиву убрать. `smartd -d -q onecheck -c /etc/smartd.conf`
выполняет один проход по устройствам и выходит — удобен для проверки
правок конфига без перезапуска демона.

## Откат

```bash
systemctl disable --now smartmontools
mv /etc/smartd.conf.orig /etc/smartd.conf
rm /usr/local/lib/crawler/smartd-warning.sh /etc/udev/rules.d/99-crawler-nvme.rules
rm -r /etc/systemd/system/crawler-monitor.service.d   # если других drop-in'ов нет
udevadm control --reload && udevadm trigger --subsystem-match=nvme
systemctl daemon-reload && systemctl restart crawler-monitor
groupdel nvme-smart
```

## Проверка синтаксиса конфигурации

На машине разработки (с NVMe) конфиг принят `smartd` (smartmontools 7.5):
`smartd -d -q onecheck -c smartd.conf` — директивы принимаются, устройство
регистрируется, предупреждений нет. Использованные директивы (`DEVICESCAN
-d nvme`, `-H`, `-l error`, `-W`) для NVMe одинаковы в 7.4 и 7.5 —
целевой Ubuntu 24.04 LTS (7.4) конфиг подходит без правок; закомментированная
строка с `-f`/`-p` — только для 7.5+ (см. примечания в `smartd.conf`).

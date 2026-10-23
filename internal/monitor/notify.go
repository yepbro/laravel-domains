// Доставка алертов оператору в Telegram (T-162, ТЗ 5.9): сработавшие и
// снятые алерты monitor отправляет сообщениями в ту же выделенную группу,
// что и извещения CI/CD (ТЗ 10.4), — вызовом deploy/telegram-notify.sh
// (T-136) с режимом --message: exec внешнего скрипта, как зовёт его
// конвейер, HTTP-клиент Telegram в Go не заводится (одна точка правды об
// отправке: маркеры, маскировка секретов, поведение при их отсутствии).
//
// Секреты TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID скрипт берёт из окружения:
// monitor наследует их из EnvironmentFile юнита (/etc/crawler/crawler.env
// — crawler-monitor.service), в YAML они не попадают. Нет любого секрета
// или скрипта — доставка выключена, алерты остаются Warn в журнале
// мониторинга (как шаг CI молчит без секретов, §10.4).
//
// Дебаунс — эпизод алерта (открытие…закрытие) доставляется ровно двумя
// сообщениями: 🚨 при первом срабатывании, «✅ Восстановлено» при
// снятии; повторные проходы сработавшего алерта сообщений не порождают.
// Первый проход — база без рассылки (рестарт monitor на деплое не
// рассылает состояния всех алертов). Перерыв источника («не измеряется»)
// эпизод не рвёт: возврат измерений сразу в ok закрывает эпизод честным
// ✅ (алерт перестал срабатывать — оператору нужна точка), возврат в
// firing второго 🚨 не делает (алерт и не снимался — дребезг источника
// не спамит); сам переход в «не измеряется» сообщением не отмечается:
// «восстановлено» было бы ложью, переход виден в журнале (Info).
package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Переменные окружения секретов Telegram (EnvironmentFile юнита monitor,
// deploy/systemd/crawler.env.example — те же значения, что у извещений
// CI/CD, docs/ci-cd.md §«Извещения Telegram»).
const (
	EnvTelegramBotToken = "TELEGRAM_BOT_TOKEN"
	EnvTelegramChatID   = "TELEGRAM_CHAT_ID"
)

// telegramNotifyTimeout — бюджет одного вызова скрипта: curl внутри с
// --max-time 30 плюс запуск bash/jq — с запасом, но короче периода
// цикла алертов.
const telegramNotifyTimeout = 40 * time.Second

// TelegramOptions — параметры доставки алертов (NewTelegramNotifier).
type TelegramOptions struct {
	// Enabled — включение доставки (monitor.alerts.telegram): false —
	// доставка выключена явно.
	Enabled bool
	// Script — путь telegram-notify.sh на хосте
	// (monitor.alerts.telegram_script).
	Script string
	// Log — логгер (nil — записи подавлены, slog.DiscardHandler).
	Log *slog.Logger
	// Getenv — чтение окружения секретов (nil — os.Getenv); тесты
	// подменяют (t.Setenv непригоден: проверяется и пустое значение).
	Getenv func(string) string
}

// TelegramNotifier — доставка переходов алертов в Telegram: подключается
// хуком в AlertsOptions.OnPass (цикл проходов вызывает из одного потока —
// блокировки состояния не нужны). Отправка асинхронна: проход алертов не
// ждёт curl (зависший скрипт срезается таймаутом), сбой — Warn: алерт уже
// в журнале и метриках, повторная попытка — следующий переход состояния.
type TelegramNotifier struct {
	// script — путь скрипта; "" — доставка выключена (все причины —
	// конфигурация, секреты, доступность скрипта — решает конструктор).
	script string
	log    *slog.Logger
	// state — трекинг эпизодов по именам алертов: эпизод — от первого
	// срабатывания до снятия; в его пределах ровно одно 🚨 и одно ✅.
	state map[string]notifyEpisode
}

// notifyEpisode — состояние одного алерта для доставки.
type notifyEpisode struct {
	// seen — проход алерта уже наблюдался (стартовая база).
	seen bool
	// wasFiring — алерт срабатывал с последнего снятия (эпизод открыт):
	// перерывы источника («не измеряется») эпизод не закрывают —
	// восстановление измерений в ok честно закрывает его ✅, возврат в
	// firing второго 🚨 не делает (алерт и не снимался).
	wasFiring bool
}

// NewTelegramNotifier решает, включена ли доставка: выключена явно,
// нет секретов окружения, скрипт недоступен/не исполняем — адресат
// сообщений не появляется, каждая причина — запись в журнале (Info для
// ожидаемых состояний, Warn для сломанной настройки боевого хоста).
func NewTelegramNotifier(opts TelegramOptions) *TelegramNotifier {
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	getenv := opts.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	n := &TelegramNotifier{script: "", log: log, state: make(map[string]notifyEpisode)}
	if !opts.Enabled {
		log.Info("telegram: доставка алертов выключена (monitor.alerts.telegram=false)")
		return n
	}
	if getenv(EnvTelegramBotToken) == "" || getenv(EnvTelegramChatID) == "" {
		log.Info("telegram: доставка алертов выключена — не задан TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID " +
			"(EnvironmentFile юнита, deploy/systemd/crawler.env.example); алерты остаются Warn в журнале monitor")
		return n
	}
	script := opts.Script
	if script == "" {
		log.Warn("telegram: доставка алертов выключена — не задан путь скрипта (monitor.alerts.telegram_script)")
		return n
	}
	if st, err := os.Stat(script); err != nil || st.IsDir() || st.Mode()&0o111 == 0 {
		log.Warn("telegram: доставка алертов выключена — скрипт не доступен или не исполняем",
			"script", script, "err", err)
		return n
	}
	n.script = script
	log.Info("telegram: доставка алертов включена", "script", script)
	return n
}

// OnPass — хук AlertsOptions.OnPass: эпизод алерта (срабатывание … снятие)
// доставляется ровно двумя сообщениями — 🚨 при открытии и ✅ при
// закрытии. Дебаунс: повторные проходы сработавшего алерта, возврат в
// firing после перерыва источника и шум несработавших алертов — тишина.
// Первый проход — стартовая база без рассылки: рестарт monitor на деплое
// не рассылает состояния всех алертов.
func (n *TelegramNotifier) OnPass(res AlertsResult) {
	if n.script == "" {
		return
	}
	for _, a := range res.Alerts {
		st := n.state[a.Name]
		firstPass := !st.seen
		st.seen = true
		switch {
		case a.Status == AlertStatusFiring:
			if !st.wasFiring && !firstPass {
				n.send(fmt.Sprintf("🚨 Алерт %s: %s\n%s", a.Name, alertDesc(a.Name), a.Detail))
			}
			st.wasFiring = true
		case a.Status == AlertStatusOK:
			if st.wasFiring {
				n.send(fmt.Sprintf("✅ Восстановлено: %s (%s)\n%s", a.Name, alertDesc(a.Name), a.Detail))
			}
			st.wasFiring = false
		case a.Status == AlertStatusNotMeasured && st.wasFiring:
			// Без сообщения — см. шапку файла: восстановление не доказано,
			// а дребезг источника не должен ни спамить, ни закрывать эпизод.
			n.log.Info("telegram: сработавший алерт больше не измеряется — без сообщения",
				"alert", a.Name, "причина", a.Detail)
		}
		n.state[a.Name] = st
	}
}

// alertDesc — описание алерта для текста сообщения (alertDescs).
func alertDesc(name string) string {
	if d, ok := alertDescs[name]; ok {
		return d
	}
	return name
}

// send запускает скрипт в фоновой горутине: текст — единственный
// аргумент --message (exec без оболочки: текст не интерпретируется,
// кавычки/переводы строк сохраняются; секреты скрипт берёт из унаследованного
// окружения monitor и в вывод не печатает).
func (n *TelegramNotifier) send(text string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), telegramNotifyTimeout)
		defer cancel()
		out, err := exec.CommandContext(ctx, n.script, "--message", text).CombinedOutput()
		switch {
		case ctx.Err() != nil:
			n.log.Warn("telegram: отправка алерта прервана таймаутом",
				"script", n.script, "timeout", telegramNotifyTimeout.String())
		case err != nil:
			n.log.Warn("telegram: отправка алерта не удалась",
				"script", n.script, "err", err, "вывод", strings.TrimSpace(string(out)))
		default:
			n.log.Debug("telegram: алерт отправлен", "вывод", strings.TrimSpace(string(out)))
		}
	}()
}

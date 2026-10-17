// Логика вариантов http/https (T-054; ТЗ 5.5 п. 5, сводка алгоритма §7
// «variant 0/1 (только если разные сайты)»). Для субдомена воркер
// проверяет оба протокола (T-053), а чистые функции этого файла решают,
// сколько записей subdomain_results создать и с какими variant/scheme.
// Вход обобщён (VariantInput) и не зависит от внутренностей
// FetchResult/ChainResult — отображение делает воркер T-053; критерий
// одинаковости (VariantSameSite) переиспользуют алиасы субдоменов
// (T-055, ТЗ 5.5 п. 6 «итоговый URL … или хэш контента совпадает») и
// корпус тестов T-059.

package httpfetch

import (
	"fmt"
	"net/url"
	"strings"

	"crawler/internal/content"
	"crawler/internal/model"
)

// VariantInput — итог проверки субдомена по одному протоколу (https или
// http). Scheme — протокол проверки (стартового запроса), а не
// обязательно итогового URL: http-проверка может привести редиректом на
// https-страницу и наоборот.
type VariantInput struct {
	// Scheme — каким протоколом делалась проверка (model.SchemeHTTP /
	// model.SchemeHTTPS). Определяет scheme записи, созданной по этому
	// итогу (см. VariantRecord).
	Scheme model.Scheme
	// Available — проверка дошла до итоговой страницы: сервер ответил
	// любым статусом, включая 4xx/5xx (протокол «работает», ТЗ 5.5 п. 5);
	// сетевые ошибки, таймауты, превышение лимита/цикл редиректов —
	// false. Код ошибки для результата классифицирует потребитель
	// (T-058, §8); здесь важна только доступность.
	Available bool
	// Host — хост ИТОГОВОГО URL (страница в конце цепочки редиректов,
	// ТЗ 5.5 п. 4): punycode, нижний регистр, без порта. Нормализацию
	// делает цепочка T-048 (хосты Location/meta refresh — punycode и
	// нижний регистр, ТЗ 5.2 п. 8), стартовый URL — punycode по
	// построению задачи; NewVariantInput срезает порт.
	Host string
	// Path — путь итогового URL (декодированная форма url.URL.Path).
	// Query и фрагмент в критерий вариантов не входят (см.
	// VariantSameSite) и в этом поле не представляются.
	Path string
	// ContentHash — хэш контента итоговой страницы (content.Hash,
	// 16 байт, ТЗ 5.7); nil — контент не сохранён (лимит
	// max_store_bytes T-056, ошибка до сохранения): критерий
	// одинаковости вырождается в host+path. Хэш пустого тела —
	// валидное значение, а не nil.
	ContentHash *content.Hash
}

// NewVariantInput собирает вход из итогового URL проверки. Представление
// итогового URL в логике вариантов зафиксировано здесь в одном месте
// (воркер T-053 не дублирует правила): хост — без порта (url.Hostname,
// стандартный :80/:443 и нестандартные порты не разваливают один сайт
// на варианты), путь — u.Path, без query и фрагмента. final == nil
// (страницы нет) даёт нулевые Host/Path. Punycode-нормализацию не
// повторяет: цепочка T-048 уже привела хосты к punycode и нижнему
// регистру.
func NewVariantInput(scheme model.Scheme, available bool, final *url.URL, hash *content.Hash) VariantInput {
	in := VariantInput{Scheme: scheme, Available: available, ContentHash: hash}
	if final != nil {
		in.Host = final.Hostname()
		in.Path = final.Path
	}
	return in
}

// VariantSameSite — критерий одинаковости сайтов двух итогов (ТЗ 5.5
// п. 5: «одинаковые итоговые хост и путь либо одинаковый хэш контента»,
// ИЛИ-критерий). true, когда:
//
//   - совпали итоговые хост и путь: хост — без учёта регистра (регистр в
//     DNS незначим; punycode-хост уже нижний регистр, EqualFold —
//     страховка от ненормализованного входа), путь — байт-точно. ТЗ
//     называет только «хост и путь», поэтому query в критерий не входит:
//     счётчиковые параметры (?i=1) не разваливают один сайт на два
//     варианта, а реальное различие страниц при одном пути ловит хэш;
//     фрагмент серверу не отправляется вовсе. Percent-кодирование
//     сравнивается в декодированной форме (url.URL.Path);
//   - или хэши контента обоих итогов не nil и равны — парковки и
//     заглушки хостингов с разных путей (§8 «естественная дедупликация
//     по хэшу»). Двух nil недостаточно: «контент не сохранён» ничего не
//     доказывает о одинаковости, критерий вырождается в host+path.
//
// Недоступный итог не «тот же сайт» ни с чем: оба аргумента должны быть
// Available (DecideVariants гарантирует; алиасы T-055 сравнивают
// доступные итоги).
func VariantSameSite(a, b VariantInput) bool {
	if !a.Available || !b.Available {
		return false
	}
	if strings.EqualFold(a.Host, b.Host) && a.Path == b.Path {
		return true
	}
	return a.ContentHash != nil && b.ContentHash != nil && *a.ContentHash == *b.ContentHash
}

// VariantRecord — запись субдомен-результата, которую создаёт логика
// вариантов: значения колонок variant/scheme (модель subdomain_results,
// ТЗ 6.1). Scheme одновременно указывает проверку-источник данных
// записи (итоговый URL, хэш, TLS, заголовки): вариант 0 при единственной
// записи в паре итогов и в паре записей несёт данные https-проверки,
// вариант 1 — http-проверки.
type VariantRecord struct {
	Variant model.Variant
	Scheme  model.Scheme
}

// DecideVariants решает, какие записи субдомен-результатов создать по
// итогам проверок субдомена по обоим протоколам (ТЗ 5.5 п. 5):
//
//   - оба недоступны — записей нет (nil): финальный статус домена
//     («нет ответа ни у кого → no_http») решает агрегатор эпизода
//     T-058 по §7;
//   - доступен один — одна запись variant=0 со схемой этой проверки
//     («работает только один протокол → одна запись с соответствующей
//     scheme»); схема итогового URL может отличаться — запись несёт
//     данные своей проверки;
//   - оба доступны и ведут на один сайт (VariantSameSite) — одна запись
//     variant=0 scheme=https: https — канонический протокол, ТЗ п. 5
//     явно назначает его варианту 0 («две записи: variant = 0 (https) и
//     variant = 1 (http)»), данные берутся из https-проверки;
//   - протоколы ведут на разные сайты — две записи: variant=0 (https),
//     variant=1 (http).
//
// Аргументы — итоги https- и http-проверок в любом порядке, различаются
// полем Scheme; пара обязана быть {http, https}, иначе ошибка (молча
// перепутанные итоги дали бы неверные variant/scheme в БД). Порядок
// записей детерминирован: [variant 0, variant 1].
func DecideVariants(a, b VariantInput) ([]VariantRecord, error) {
	var httpsIn, httpIn VariantInput
	switch {
	case a.Scheme == model.SchemeHTTPS && b.Scheme == model.SchemeHTTP:
		httpsIn, httpIn = a, b
	case a.Scheme == model.SchemeHTTP && b.Scheme == model.SchemeHTTPS:
		httpsIn, httpIn = b, a
	default:
		return nil, fmt.Errorf("httpfetch: итоги проверок не пара http+https: схемы %v и %v",
			a.Scheme, b.Scheme)
	}

	switch {
	case !httpsIn.Available && !httpIn.Available:
		return nil, nil
	case httpsIn.Available && !httpIn.Available:
		return []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}}, nil
	case !httpsIn.Available && httpIn.Available:
		return []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTP}}, nil
	}

	if VariantSameSite(httpsIn, httpIn) {
		return []VariantRecord{{Variant: model.VariantMain, Scheme: model.SchemeHTTPS}}, nil
	}
	return []VariantRecord{
		{Variant: model.VariantMain, Scheme: model.SchemeHTTPS},
		{Variant: model.VariantHTTP, Scheme: model.SchemeHTTP},
	}, nil
}

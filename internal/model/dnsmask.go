package model

import "fmt"

// MaxSubdomains — максимум субдоменов (ТЗ 5.10): маска занимает bigint
// (int8), доступны биты 0..62, бит 63 — знаковый и запрещён. Добавление
// нового субдомена — новый бит, без миграции данных.
const MaxSubdomains = 63

// maxSubdomainID — верхняя граница номера бита маски (62).
const maxSubdomainID = MaxSubdomains - 1

// DNSMask — битовая маска субдоменов: бит с номером subdomains.id
// означает соответствующий субдомен. Используется в колонках
// domains.dns_mask (резолвящиеся субдомены, ТЗ 5.4 п. 4) и
// laravel_domains.found_on_mask (субдомены, где найден Laravel, ТЗ 5.8).
//
// Номера вне 0..62 запрещены: бит 63 знаковый, его установка сделала бы
// значение отрицательным и сломала бы сравнения масок в SQL и Go.
// Set/Unset отвергают такие номера ошибкой (все номера проверяются до
// изменения — маска не меняется частично), Test для недопустимого номера
// возвращает false: несуществующий бит не установлен.
type DNSMask int64

// ValidateSubdomainID проверяет, что id допустим как номер бита маски
// (целое 0..62). Используется и при добавлении субдоменов в справочник
// (crawlerctl subdomains add, ТЗ 5.10).
func ValidateSubdomainID(id int) error {
	if id < 0 || id > maxSubdomainID {
		return fmt.Errorf("model: номер субдомена %d вне диапазона 0..%d (максимум %d субдомена, бит 63 маски знаковый; ТЗ 5.10)", id, maxSubdomainID, MaxSubdomains)
	}
	return nil
}

// NewDNSMask строит маску с битами указанных субдоменов.
func NewDNSMask(ids ...int) (DNSMask, error) {
	var m DNSMask
	if err := m.Set(ids...); err != nil {
		return 0, err
	}
	return m, nil
}

// Set включает биты указанных субдоменов. При любом недопустимом номере —
// ошибка, маска остаётся без изменений.
func (m *DNSMask) Set(ids ...int) error {
	for _, id := range ids {
		if err := ValidateSubdomainID(id); err != nil {
			return err
		}
	}
	for _, id := range ids {
		*m |= subdomainBit(id)
	}
	return nil
}

// Unset выключает биты указанных субдоменов. При любом недопустимом
// номере — ошибка, маска остаётся без изменений.
func (m *DNSMask) Unset(ids ...int) error {
	for _, id := range ids {
		if err := ValidateSubdomainID(id); err != nil {
			return err
		}
	}
	for _, id := range ids {
		*m &^= subdomainBit(id)
	}
	return nil
}

// Test сообщает, установлен ли бит субдомена id (для id вне 0..62 — false).
func (m DNSMask) Test(id int) bool {
	return id >= 0 && id <= maxSubdomainID && m&subdomainBit(id) != 0
}

// IDs возвращает номера установленных битов по возрастанию — расшифровка
// маски в терминах subdomains.id.
func (m DNSMask) IDs() []int {
	ids := make([]int, 0, MaxSubdomains)
	// Границы итерации избыточны для результата: недопустимые номера
	// (-1, 63) Test отсекает проверкой диапазона (false), лишние итерации
	// цикла ничего не добавляют в ids — оба мутанта строки эквивалентны.
	// mutator-disable-next-line expression/comparison, numbers/decrementer
	for id := 0; id < MaxSubdomains; id++ {
		if m.Test(id) {
			ids = append(ids, id)
		}
	}
	return ids
}

// Count возвращает число установленных битов (0..63).
func (m DNSMask) Count() int { return len(m.IDs()) }

// IsEmpty сообщает, что не установлено ни одного бита.
func (m DNSMask) IsEmpty() bool { return m == 0 }

// Valid проверяет инвариант маски, прочитанной из внешнего источника
// (БД): значение обязано быть неотрицательным — бит 63 не используется.
func (m DNSMask) Valid() bool { return m >= 0 }

// subdomainBit возвращает маску из одного бита; id обязан быть проверен
// ValidateSubdomainID заранее.
func subdomainBit(id int) DNSMask { return DNSMask(1) << id }

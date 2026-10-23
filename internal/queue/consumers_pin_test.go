// Пин-тест соответствия групп воркеров (T-155): WorkingGroups зеркалит
// DefaultGroup пакетов воркеров — пакет queue не может импортировать их
// напрямую (воркеры строятся поверх queue, цикл импорта), поэтому имена
// продублированы строками и сверяются здесь. Внешний тест-пакет
// queue_test импортирует и queue, и воркеров без цикла.

package queue_test

import (
	"testing"

	"crawler/internal/analyzer"
	"crawler/internal/dnsworker"
	"crawler/internal/httpworker"
	"crawler/internal/queue"
	"crawler/internal/renderworker"
)

// TestWorkingGroupsMatchWorkerDefaults — смена DefaultGroup воркера
// (или стрима этапа) обязана попасть в queue.WorkingGroups, иначе группы
// этого этапа молча не чистятся.
func TestWorkingGroupsMatchWorkerDefaults(t *testing.T) {
	want := map[string]string{
		queue.StreamDNS:     dnsworker.DefaultGroup,
		queue.StreamHTTP:    httpworker.DefaultGroup,
		queue.StreamRender:  renderworker.DefaultGroup,
		queue.StreamAnalyze: analyzer.DefaultGroup,
	}
	got := queue.WorkingGroups()
	if len(got) != len(want) {
		t.Fatalf("WorkingGroups = %v, хочу ровно пары %v (лишний/недостающий стрим — обновите очередь и воркеров согласованно)", got, want)
	}
	for stream, group := range want {
		if got[stream] != group {
			t.Errorf("WorkingGroups[%s] = %q, хочу %q (%s DefaultGroup воркера)", stream, got[stream], group, stream)
		}
	}
}

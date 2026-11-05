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
// этого этапа молча не чистятся. Пара q:render-fallback сверяется
// литералом: группа render-fallback-worker — профиль воркера B (ФТ-12/
// T-240: тот же бинарь renderworker, отдельная группа), а не DefaultGroup
// рендер-воркера.
func TestWorkingGroupsMatchWorkerDefaults(t *testing.T) {
	want := map[string]string{
		queue.StreamDNS:     dnsworker.DefaultGroup,
		queue.StreamHTTP:    httpworker.DefaultGroup,
		queue.StreamRender:  renderworker.DefaultGroup,
		queue.StreamAnalyze: analyzer.DefaultGroup,
	}
	got := queue.WorkingGroups()
	if len(got) != len(want)+1 { // + пара q:render-fallback (профиль воркера B)
		t.Fatalf("WorkingGroups = %v, хочу пары %v + q:render-fallback=render-fallback-worker (лишний/недостающий стрим — обновите очередь и воркеров согласованно)", got, want)
	}
	for stream, group := range want {
		if got[stream] != group {
			t.Errorf("WorkingGroups[%s] = %q, хочу %q (%s DefaultGroup воркера)", stream, got[stream], group, stream)
		}
	}
	if got[queue.StreamRenderFallback] != "render-fallback-worker" {
		t.Errorf("WorkingGroups[%s] = %q, хочу %q (группа профиля воркера B, T-240/ФТ-12)",
			queue.StreamRenderFallback, got[queue.StreamRenderFallback], "render-fallback-worker")
	}
}

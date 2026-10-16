// Package watch — каркас наблюдения за каталогом входных файлов ingest
// (ТЗ 5.1: fsnotify + периодический опрос как страховка).
//
// Полная реализация (опрос-страховка, игнор *.tmp/*.part, проверка
// стабильности размера/mtime, разбор структуры <зона>/add|delete) —
// задача T-013. Сейчас пакет пересылает события fsnotify наблюдаемого
// каталога в канал и закрывает его при отмене контекста.
package watch

import (
	"context"
	"fmt"

	"github.com/fsnotify/fsnotify"
)

// Dir наблюдает за каталогом dir (не рекурсивно) и пересылает события
// fsnotify в возвращаемый канал. Канал закрывается при отмене ctx.
func Dir(ctx context.Context, dir string) (<-chan fsnotify.Event, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := w.Add(dir); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("watch %s: %w", dir, err)
	}

	events := make(chan fsnotify.Event)
	go func() {
		defer close(events)
		defer func() { _ = w.Close() }()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-w.Events:
				if !ok {
					return
				}
				select {
				case events <- ev:
				case <-ctx.Done():
					return
				}
			case err, ok := <-w.Errors:
				if !ok {
					return
				}
				// TODO(T-013): логировать ошибки наблюдения.
				_ = err
			}
		}
	}()
	return events, nil
}

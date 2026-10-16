package watch

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestDir(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events, err := Dir(ctx, dir)
	if err != nil {
		t.Fatalf("Dir: %v", err)
	}

	name := filepath.Join(dir, "com.add.txt")
	if err := os.WriteFile(name, []byte("example.com\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-events:
		if ev.Name != name {
			t.Errorf("событие = %q, want %q", ev.Name, name)
		}
		if ev.Op&fsnotify.Create == 0 {
			t.Errorf("операция = %v, want Create", ev.Op)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("событие создания файла не получено")
	}
}

func TestDir_ClosesOnCancel(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	events, err := Dir(ctx, dir)
	if err != nil {
		t.Fatalf("Dir: %v", err)
	}

	cancel()
	select {
	case _, ok := <-events:
		if ok {
			t.Error("после cancel в канале осталось событие")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("канал не закрылся после cancel")
	}
}

func TestDir_MissingDir(t *testing.T) {
	if _, err := Dir(context.Background(), filepath.Join(t.TempDir(), "нет-такого")); err == nil {
		t.Error("ожидалась ошибка наблюдения за несуществующим каталогом")
	}
}

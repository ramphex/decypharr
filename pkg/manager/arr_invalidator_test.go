package manager

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr/reacquire"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func newInvalidateTestManager(t *testing.T) (*Manager, *storage.Entry) {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	m := &Manager{storage: store, clients: xsync.NewMap[string, debrid.Client](), logger: zerolog.Nop()}
	m.initEntryCache()

	entry := &storage.Entry{InfoHash: "pack", Name: "Example.Show.S01.1080p", Protocol: config.ProtocolNZB, Files: map[string]*storage.File{}}
	for _, name := range []string{"e01.mkv", "e02.mkv", "e03.mkv"} {
		entry.Files[name] = &storage.File{Name: name, InfoHash: "pack", Size: 100, AddedOn: time.Unix(1000, 0)}
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	saved, err := store.Get("pack")
	if err != nil {
		t.Fatal(err)
	}
	return m, saved
}

func TestFileScopedInvalidationKeepsRestOfPack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, entry := newInvalidateTestManager(t)
		broken := entry.Files["e02.mkv"]
		if broken.ID == "" {
			t.Fatal("stored file has no ID")
		}
		job := reacquire.Job{
			EntryID: "pack", FileID: broken.ID, FileScoped: true,
			Bindings: []reacquire.Binding{{EntryID: "pack", EntryFileID: broken.ID}},
		}
		if err := m.InvalidateReacquire(t.Context(), job); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()

		if _, err := m.storage.Get("pack"); err != nil {
			t.Fatalf("pack entry was deleted: %v", err)
		}
		folder := entry.GetFolder()
		for name, want := range map[string]bool{"e01.mkv": true, "e02.mkv": false, "e03.mkv": true} {
			if _, err := m.GetTorrentFile(folder, name); (err == nil) != want {
				t.Errorf("%s present = %v, want %v", name, err == nil, want)
			}
		}
		// Invalidating again is a no-op once the file is gone.
		if err := m.InvalidateReacquire(t.Context(), job); err != nil {
			t.Fatalf("repeat invalidation: %v", err)
		}
	})
}

func TestDownloadInvalidationDeletesEntry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, entry := newInvalidateTestManager(t)
		file := entry.Files["e01.mkv"]
		job := reacquire.Job{
			EntryID: "pack", FileID: file.ID,
			Bindings: []reacquire.Binding{{EntryID: "pack", EntryFileID: file.ID}},
		}
		if err := m.InvalidateReacquire(t.Context(), job); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if _, err := m.storage.Get("pack"); err == nil {
			t.Fatal("entry survived a whole-download invalidation")
		}
	})
}

func TestDisabledStreamReacquireOnlyFailsRead(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	if _, err := config.Update(func(cfg *config.Config) error {
		cfg.Repair.DisableStreamReacquire = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	recovery := &fakeArrRecovery{
		binding: reacquire.Binding{EntryID: "nzb-1", EntryFileID: "file-1"},
		started: make(chan struct{}),
	}
	m := &Manager{activeStreams: xsync.NewMap[string, *ActiveStream]()}
	m.SetArrRecovery(recovery)

	m.submitStreamReacquire("nzb-1", "file-1")
	select {
	case <-recovery.started:
		t.Fatal("stream reacquire ran while disabled")
	case <-time.After(100 * time.Millisecond):
	}
}

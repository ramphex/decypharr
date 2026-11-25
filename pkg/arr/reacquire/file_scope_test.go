package reacquire

import (
	"fmt"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/arr"
)

// packBindings indexes the episodes of one season pack download.
func packBindings(t *testing.T, service *Service, downloadID string, episodes int) []Binding {
	t.Helper()
	bindings := make([]Binding, episodes)
	for i := range bindings {
		bindings[i] = Binding{
			ArrName:                "sonarr",
			ArrType:                arr.Sonarr,
			ArrInstanceFingerprint: testArrInstanceFingerprint,
			EntryID:                "pack-entry",
			EntryFileID:            fmt.Sprintf("file-%02d", i+1),
			DownloadID:             downloadID,
			ArrFileID:              100 + i,
			LibraryPath:            fmt.Sprintf("/library/Example Show/Season 01/e%02d.mkv", i+1),
			SeriesID:               7,
			SeasonNumber:           1,
			EpisodeIDs:             []int{1000 + i},
			Confidence:             ConfidenceExactPath,
		}
		if err := service.UpsertBinding(bindings[i]); err != nil {
			t.Fatal(err)
		}
	}
	return bindings
}

func newFileScopeService(t *testing.T, dir string) *Service {
	t.Helper()
	service, err := NewService(ServiceOptions{Directory: dir, Handler: &waitingReacquireHandler{called: make(chan string, 16)}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	if err := service.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return service
}

func TestAutomaticReacquireOfPackFileReplacesOnlyThatFile(t *testing.T) {
	for _, cause := range []Cause{CauseStream, CauseRepair} {
		t.Run(string(cause), func(t *testing.T) {
			service := newFileScopeService(t, t.TempDir())
			pack := packBindings(t, service, "pack-download", 3)

			job, err := service.Reacquire(Request{EntryID: "pack-entry", FileID: pack[1].EntryFileID, Cause: cause, Strategy: StrategyHistoryFailed})
			if err != nil {
				t.Fatal(err)
			}
			if !job.FileScoped {
				t.Fatal("job is not file scoped")
			}
			if job.Strategy != StrategyCommandSearch {
				t.Fatalf("strategy = %q, want %q so the grab is not failed", job.Strategy, StrategyCommandSearch)
			}
			if len(job.Bindings) != 1 || job.Bindings[0].ArrFileID != pack[1].ArrFileID {
				t.Fatalf("job covers %d files (%v), want only arr file %d", len(job.Bindings), job.Bindings, pack[1].ArrFileID)
			}
			if job.DownloadID != "pack-download" {
				t.Fatalf("download ID = %q, needed to recognise the replacement", job.DownloadID)
			}

			// Another unreadable episode of the same pack gets its own job.
			other, err := service.Reacquire(Request{EntryID: "pack-entry", FileID: pack[2].EntryFileID, Cause: cause})
			if err != nil {
				t.Fatal(err)
			}
			if other.ID == job.ID {
				t.Fatal("second episode was folded into the first episode's job")
			}
			again, err := service.Reacquire(Request{EntryID: "pack-entry", FileID: pack[1].EntryFileID, Cause: cause})
			if err != nil {
				t.Fatal(err)
			}
			if again.ID != job.ID {
				t.Fatal("repeated request for the same file started a second job")
			}
		})
	}
}

func TestManualReacquireOfPackFileKeepsWholeDownload(t *testing.T) {
	service := newFileScopeService(t, t.TempDir())
	pack := packBindings(t, service, "pack-download", 3)

	job, err := service.Reacquire(Request{EntryID: "pack-entry", FileID: pack[0].EntryFileID, Cause: CauseManual})
	if err != nil {
		t.Fatal(err)
	}
	if job.FileScoped || job.Strategy != StrategyHistoryFailed || len(job.Bindings) != 3 {
		t.Fatalf("job = scoped %v, strategy %q, %d files; want the whole download failed in history", job.FileScoped, job.Strategy, len(job.Bindings))
	}
}

func TestStreamReacquireOfSingleFileDownloadStillFailsGrab(t *testing.T) {
	service := newFileScopeService(t, t.TempDir())
	single := packBindings(t, service, "single-download", 1)

	job, err := service.Reacquire(Request{EntryID: "pack-entry", FileID: single[0].EntryFileID, Cause: CauseStream})
	if err != nil {
		t.Fatal(err)
	}
	if job.FileScoped || job.Strategy != StrategyHistoryFailed {
		t.Fatalf("job = scoped %v, strategy %q; a single-file grab should still be failed", job.FileScoped, job.Strategy)
	}
}

// File-scoped jobs of one pack keep separate keys after a restart, so loading
// them does not cancel one as a duplicate of the other.
func TestFileScopedJobsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	service := newFileScopeService(t, dir)
	pack := packBindings(t, service, "pack-download", 2)
	var ids []string
	for _, binding := range pack {
		job, err := service.Reacquire(Request{EntryID: "pack-entry", FileID: binding.EntryFileID, Cause: CauseStream})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, job.ID)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	reloaded := newFileScopeService(t, dir)
	for _, id := range ids {
		job, ok := reloaded.Job(id)
		if !ok {
			t.Fatalf("job %s was not reloaded", id)
		}
		if job.Status == StatusCancelled || !job.FileScoped {
			t.Fatalf("job %s after restart = %s, scoped %v", id, job.Status, job.FileScoped)
		}
	}
}

func TestFileScopedJobCompletesWhenItsEpisodeIsReplaced(t *testing.T) {
	service := newFileScopeService(t, t.TempDir())
	pack := packBindings(t, service, "pack-download", 3)
	job, err := service.Reacquire(Request{EntryID: "pack-entry", FileID: pack[1].EntryFileID, Cause: CauseStream})
	if err != nil {
		t.Fatal(err)
	}
	waitForJobStatus(t, service, job.ID, StatusWaitingForImport)

	replacement := pack[1]
	replacement.EntryID = "single-episode-entry"
	replacement.EntryFileID = "single-episode-file"
	replacement.DownloadID = "single-episode-download"
	replacement.ArrFileID = 200
	if err := service.UpsertBinding(replacement); err != nil {
		t.Fatal(err)
	}
	ready := waitForJobStatus(t, service, job.ID, StatusReady)
	if ready.ReplacementDownloadID != "single-episode-download" {
		t.Fatalf("replacement download = %q", ready.ReplacementDownloadID)
	}
}

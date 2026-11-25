package manager

import (
	"context"
	"fmt"

	"github.com/sirrobot01/decypharr/pkg/arr/reacquire"
)

func (m *Manager) InvalidateReacquire(ctx context.Context, job reacquire.Job) error {
	if job.FileScoped {
		// The rest of the download stays: remove only the replaced files.
		for _, binding := range job.Bindings {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := m.removeBoundFile(binding.EntryID, binding.EntryFileID); err != nil {
				return fmt.Errorf("invalidate managed file %q/%q: %w", binding.EntryID, binding.EntryFileID, err)
			}
		}
		return m.deleteJobBindings(job)
	}

	entryIDs := make(map[string]struct{}, len(job.Bindings))
	for _, binding := range job.Bindings {
		if binding.EntryID != "" {
			entryIDs[binding.EntryID] = struct{}{}
		}
	}
	if len(entryIDs) == 0 && job.EntryID != "" {
		entryIDs[job.EntryID] = struct{}{}
	}

	for entryID := range entryIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		exists, err := m.storage.Exists(entryID)
		if err != nil {
			return fmt.Errorf("check managed entry %q: %w", entryID, err)
		}
		if !exists {
			continue
		}
		if err := m.DeleteEntry(entryID, true); err != nil {
			return fmt.Errorf("invalidate managed entry %q: %w", entryID, err)
		}
	}
	return m.deleteJobBindings(job)
}

func (m *Manager) deleteJobBindings(job reacquire.Job) error {
	if m.arrService == nil {
		return nil
	}
	for _, binding := range job.Bindings {
		if err := m.arrService.DeleteBinding(binding.EntryID, binding.EntryFileID); err != nil {
			return fmt.Errorf("remove Arr binding %q/%q: %w", binding.EntryID, binding.EntryFileID, err)
		}
	}
	return nil
}

// removeBoundFile removes one file of an entry, as repair does for a broken
// file. A file or entry that is already gone needs nothing.
func (m *Manager) removeBoundFile(entryID, fileID string) error {
	exists, err := m.storage.Exists(entryID)
	if err != nil || !exists {
		return err
	}
	entry, err := m.storage.Get(entryID)
	if err != nil {
		return err
	}
	file, err := entry.GetFileByID(fileID)
	if err != nil {
		return nil
	}
	folder := entry.GetFolder()
	item, err := m.storage.GetEntryItem(folder)
	if err != nil {
		return nil
	}
	// The folder may already hide the file, or serve another entry's copy.
	if listed, ok := item.Files[file.Name]; !ok || listed.Deleted || listed.InfoHash != entryID {
		return nil
	}
	return m.RemoveTorrentFile(folder, file.Name)
}

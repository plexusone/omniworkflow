package durable

import (
	"context"
	"testing"

	"github.com/plexusone/omniworkflow"
)

func sampleSnapshot(id string, status omniworkflow.RunStatus) Snapshot {
	return Snapshot{
		RunID:     id,
		Plan:      omniworkflow.Plan{Steps: []omniworkflow.PlanStep{{ID: "a"}, {ID: "b", DependsOn: []string{"a"}}}},
		Succeeded: map[string]bool{"a": true},
		Failed:    map[string]bool{},
		Approved:  map[string]bool{},
		Status:    status,
	}
}

func TestFileStoreRoundTrip(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if _, ok, err := store.Load(ctx, "missing"); err != nil || ok {
		t.Fatalf("Load(missing) = ok %v, err %v; want false, nil", ok, err)
	}

	snap := sampleSnapshot("run-1", omniworkflow.StatusRunning)
	if err := store.Save(ctx, snap); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, ok, err := store.Load(ctx, "run-1")
	if err != nil || !ok {
		t.Fatalf("Load: ok %v, err %v", ok, err)
	}
	if got.Status != omniworkflow.StatusRunning || !got.Succeeded["a"] || len(got.Plan.Steps) != 2 {
		t.Fatalf("round-tripped snapshot mismatch: %+v", got)
	}

	// Overwrite (atomic) then confirm the new value.
	snap.Status = omniworkflow.StatusSucceeded
	if err := store.Save(ctx, snap); err != nil {
		t.Fatalf("Save overwrite: %v", err)
	}
	got, _, _ = store.Load(ctx, "run-1")
	if got.Status != omniworkflow.StatusSucceeded {
		t.Fatalf("overwrite not reflected: %+v", got)
	}
}

func TestFileStoreListSkipsNonSnapshots(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = store.Save(ctx, sampleSnapshot("r1", omniworkflow.StatusRunning))
	_ = store.Save(ctx, sampleSnapshot("r2", omniworkflow.StatusSucceeded))

	snaps, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("List returned %d snapshots, want 2", len(snaps))
	}
}

func TestFileStorePathSanitized(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// A hostile run ID must not escape the store directory.
	snap := sampleSnapshot("../../etc/passwd", omniworkflow.StatusRunning)
	if err := store.Save(ctx, snap); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, ok, err := store.Load(ctx, "../../etc/passwd")
	if err != nil || !ok || got.RunID != "../../etc/passwd" {
		t.Fatalf("sanitized round-trip failed: ok %v, err %v", ok, err)
	}
}

func TestMemStoreRoundTrip(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()
	if err := store.Save(ctx, sampleSnapshot("r1", omniworkflow.StatusRunning)); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := store.Load(ctx, "r1")
	if !ok || !got.Succeeded["a"] {
		t.Fatalf("MemStore round-trip failed: %+v", got)
	}
	// Stored snapshot is cloned: mutating the returned map must not affect the store.
	got.Succeeded["a"] = false
	again, _, _ := store.Load(ctx, "r1")
	if !again.Succeeded["a"] {
		t.Fatal("MemStore did not clone on read; external mutation leaked in")
	}
}

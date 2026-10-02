// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package scheduler

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dagucloud/dagu/v2/internal/intake"
	"github.com/dagucloud/dagu/v2/internal/ir"
	"github.com/dagucloud/dagu/v2/internal/persis"
	"github.com/dagucloud/dagu/v2/internal/persis/file"
	"github.com/dagucloud/dagu/v2/internal/persis/store"
	queuedomain "github.com/dagucloud/dagu/v2/internal/queue"
	"github.com/dagucloud/dagu/v2/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestPlanQueuedRunByTrigger(t *testing.T) {
	tests := []struct {
		trigger ir.TriggerType
		runs    int
	}{
		{ir.TriggerTypeManual, 1},
		{ir.TriggerTypeWebhook, 1},
		{ir.TriggerTypeScheduler, 0},
		{ir.TriggerTypeCatchUp, 0},
		{ir.TriggerTypeRetry, 0},
	}
	for _, tt := range tests {
		t.Run(tt.trigger.String(), func(t *testing.T) {
			dir := t.TempDir()
			runs := testutil.NewFileDAGRunRepository(filepath.Join(dir, "dag-runs"), persis.DAGRunRepositoryOptions{})
			queueStore := store.NewQueueStore(file.NewCollection(filepath.Join(dir, "queue")))
			dag := &ir.DAG{Name: "queued-dag", Schedule: []ir.Schedule{mustParseSchedule(t, "0 * * * *")}}
			now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)

			_, err := intake.EnqueueRun(t.Context(), intake.QueueRequest{
				DAGRunRepository: runs,
				QueueStore:       queueStore,
				DAG:              dag,
				DAGRunID:         "queued-run",
				LogBaseDir:       filepath.Join(dir, "logs"),
				ArtifactBaseDir:  filepath.Join(dir, "artifacts"),
				TriggerType:      tt.trigger,
			})
			require.NoError(t, err)

			planner, _ := newTestTickPlanner(&mockStateStore{state: newMockState(now.Add(-time.Minute))})
			planner.cfg.IsQueued = func(ctx context.Context, dag *ir.DAG) (bool, error) {
				return hasQueuedRun(ctx, queueStore, runs, dag)
			}
			planner.cfg.GetLatestStatus = func(ctx context.Context, dag *ir.DAG) (ir.DAGRunStatus, error) {
				statuses, err := runs.RecentStatuses(ctx, dag.Name, 1)
				require.NoError(t, err)
				require.Len(t, statuses, 1)
				require.Equal(t, ir.Queued, statuses[0].Status)
				return statuses[0], nil
			}
			require.NoError(t, planner.Init(t.Context(), testDAGEntries(dag)))

			require.Len(t, planner.Plan(t.Context(), now), tt.runs)
		})
	}
}

func TestPlanQueuedRunUnreadableDefersCatchup(t *testing.T) {
	dir := t.TempDir()
	runs := testutil.NewFileDAGRunRepository(filepath.Join(dir, "dag-runs"), persis.DAGRunRepositoryOptions{})
	queueStore := store.NewQueueStore(file.NewCollection(filepath.Join(dir, "queue")))
	dag := newHourlyCatchupDAG(t, "unreadable-dag")
	dag.OverlapPolicy = ir.OverlapPolicySkip
	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)

	// The queued run has no status on disk.
	require.NoError(t, queueStore.Enqueue(t.Context(), dag.ProcGroup(), queuedomain.QueuePriorityLow, ir.NewDAGRunRef(dag.Name, "missing-run")))

	planner, _ := newTestTickPlanner(&mockStateStore{state: newMockState(now.Add(-time.Hour))})
	planner.cfg.IsQueued = func(ctx context.Context, dag *ir.DAG) (bool, error) {
		return hasQueuedRun(ctx, queueStore, runs, dag)
	}
	require.NoError(t, planner.Init(t.Context(), testDAGEntries(dag)))
	before, ok := planner.buffers[dag.Name].Peek()
	require.True(t, ok)
	watermark := planner.watermarkState.DAGs[dag.Name].LastScheduledTime

	require.Empty(t, planner.Plan(t.Context(), now))

	buf, ok := planner.buffers[dag.Name]
	require.True(t, ok, "catch-up buffer dropped")
	after, ok := buf.Peek()
	require.True(t, ok)
	require.Equal(t, before.ScheduledTime, after.ScheduledTime)
	require.Equal(t, watermark, planner.watermarkState.DAGs[dag.Name].LastScheduledTime)
}

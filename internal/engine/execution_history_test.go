package engine

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	opsv1alpha1 "de.yusaozdemir.resource-action-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestTrimExecutionRecords_KeepsNewestAndCarriesOverCreates(t *testing.T) {
	ra := &opsv1alpha1.ResourceAction{}
	// Legacy status: Create records without HandledCreateUIDs.
	for i := 0; i < maxExecutionRecords+10; i++ {
		ra.Status.Executions = append(ra.Status.Executions, opsv1alpha1.ExecutionRecord{
			ResourceUID: fmt.Sprintf("uid-%d", i),
			Event:       string(EventCreate),
		})
	}

	trimExecutionRecords(ra)

	if got := len(ra.Status.Executions); got != maxExecutionRecords {
		t.Fatalf("expected %d records, got %d", maxExecutionRecords, got)
	}
	if got := ra.Status.Executions[0].ResourceUID; got != "uid-10" {
		t.Fatalf("expected oldest kept record uid-10, got %s", got)
	}
	// Dropped records must still count as handled.
	if !createHandled(ra, "uid-0") {
		t.Fatalf("expected trimmed Create record uid-0 to stay handled")
	}
	if got := len(ra.Status.HandledCreateUIDs); got != maxExecutionRecords+10 {
		t.Fatalf("expected %d handled UIDs, got %d", maxExecutionRecords+10, got)
	}
}

func TestExecute_UpdateHistoryIsBounded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ra := newLabelChangeHTTPResourceAction(srv.URL)
	exec, cl := newTestExecutor(t, ra)
	enabled := map[string]string{"demo.resource-action-operator/enabled": "true"}

	for i := 0; i < maxExecutionRecords+5; i++ {
		input := newNodeUpdateInput("uid-node-1", "node-a", map[string]string{}, enabled)
		if err := exec.Execute(context.Background(), input); err != nil {
			t.Fatalf("execute #%d: %v", i+1, err)
		}
	}

	var got opsv1alpha1.ResourceAction
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(ra), &got); err != nil {
		t.Fatalf("get resourceaction: %v", err)
	}
	if n := len(got.Status.Executions); n != maxExecutionRecords {
		t.Fatalf("expected %d execution records, got %d", maxExecutionRecords, n)
	}
	if n := len(got.Status.HandledCreateUIDs); n != 0 {
		t.Fatalf("expected no handled Create UIDs for Update events, got %d", n)
	}
}

func TestExecute_DeleteForgetsHandledCreate(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Only subscribed to Create: the Delete must still clean up the UID.
	ra := newLabelChangeHTTPResourceAction(srv.URL)
	ra.Spec.Events = []string{"Create"}
	ra.Spec.Filters = nil
	exec, cl := newTestExecutor(t, ra)

	input := newNodeUpdateInput("uid-node-1", "node-a", nil, nil)
	input.Event = EventCreate
	input.OldObj = nil
	if err := exec.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute create: %v", err)
	}

	var got opsv1alpha1.ResourceAction
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(ra), &got); err != nil {
		t.Fatalf("get resourceaction: %v", err)
	}
	if len(got.Status.HandledCreateUIDs) != 1 || got.Status.HandledCreateUIDs[0] != "uid-node-1" {
		t.Fatalf("expected uid-node-1 to be handled, got %v", got.Status.HandledCreateUIDs)
	}

	input.Event = EventDelete
	if err := exec.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute delete: %v", err)
	}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(ra), &got); err != nil {
		t.Fatalf("get resourceaction: %v", err)
	}
	if len(got.Status.HandledCreateUIDs) != 0 {
		t.Fatalf("expected handled UIDs to be empty after delete, got %v", got.Status.HandledCreateUIDs)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected only the Create action to run, got %d calls", got)
	}
}

func TestExecute_LegacyCreateRecordStillDeduplicates(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ra := newLabelChangeHTTPResourceAction(srv.URL)
	ra.Spec.Filters = nil
	ra.Status.Executions = []opsv1alpha1.ExecutionRecord{
		{ResourceUID: "uid-node-1", Event: string(EventCreate)},
	}
	exec, _ := newTestExecutor(t, ra)

	input := newNodeUpdateInput("uid-node-1", "node-a", nil, nil)
	input.Event = EventCreate
	input.OldObj = nil
	if err := exec.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("expected Create recorded before the upgrade not to run again, got %d calls", got)
	}
}

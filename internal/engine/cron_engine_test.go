package engine

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	opsv1alpha1 "de.yusaozdemir.resource-action-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newCronResourceAction(url, schedule string) *opsv1alpha1.ResourceAction {
	return &opsv1alpha1.ResourceAction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ra-cron",
			Namespace: "default",
		},
		Spec: opsv1alpha1.ResourceActionSpec{
			Selector: opsv1alpha1.ResourceSelector{
				Group:   "apps",
				Version: "v1",
				Kind:    "Deployment",
			},
			Events: []string{"Create"},
			Actions: []opsv1alpha1.ActionSpec{
				{
					Type:      "http",
					Mode:      "cron",
					Schedule:  schedule,
					URL:       url,
					URLPolicy: &opsv1alpha1.URLPolicySpec{AllowUnsafeLocalTargets: true},
				},
			},
		},
	}
}

func TestExecuteScheduledAction_RunsCronAction(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ra := newCronResourceAction(srv.URL, "30s")
	exec, cl := newTestExecutor(t, ra)
	input := newDeploymentInput("uid-1", "demo", "default")
	key := client.ObjectKeyFromObject(ra)

	for i := 0; i < 2; i++ {
		if err := exec.ExecuteScheduledAction(context.Background(), key, 0, input); err != nil {
			t.Fatalf("execute scheduled action: %v", err)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 HTTP calls, got %d", got)
	}

	var got opsv1alpha1.ResourceAction
	if err := cl.Get(context.Background(), types.NamespacedName{Name: ra.Name, Namespace: ra.Namespace}, &got); err != nil {
		t.Fatalf("get resourceaction: %v", err)
	}
	if len(got.Status.Executions) != 0 {
		t.Fatalf("expected scheduled runs not to append execution records, got %d", len(got.Status.Executions))
	}
	if len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("expected Ready=True condition, got %+v", got.Status.Conditions)
	}
}

func TestExecuteScheduledAction_GoneWhenResourceActionOrActionMissing(t *testing.T) {
	ra := newCronResourceAction("http://example.invalid", "30s")
	exec, _ := newTestExecutor(t, ra)
	input := newDeploymentInput("uid-1", "demo", "default")

	err := exec.ExecuteScheduledAction(context.Background(), client.ObjectKey{Name: "missing", Namespace: "default"}, 0, input)
	if !errors.Is(err, errScheduledActionGone) {
		t.Fatalf("expected errScheduledActionGone for missing ResourceAction, got %v", err)
	}

	err = exec.ExecuteScheduledAction(context.Background(), client.ObjectKeyFromObject(ra), 1, input)
	if !errors.Is(err, errScheduledActionGone) {
		t.Fatalf("expected errScheduledActionGone for missing action index, got %v", err)
	}
}

func TestCronEngine_ExecutesScheduledActionOnTick(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ra := newCronResourceAction(srv.URL, "20ms")
	exec, cl := newTestExecutor(t, ra)
	cron := NewCronEngine(cl, exec)

	if err := cron.EnsureForMatch(context.Background(), newDeploymentInput("uid-1", "demo", "apps")); err != nil {
		t.Fatalf("ensure for match: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := calls.Load(); got < 2 {
		t.Fatalf("expected cron action to run at least twice, got %d", got)
	}

	// Deleting the ResourceAction stops the cron loop and unregisters it.
	if err := cl.Delete(context.Background(), ra); err != nil {
		t.Fatalf("delete resourceaction: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cron.mu.Lock()
		remaining := len(cron.jobs)
		cron.mu.Unlock()
		if remaining == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected cron job to be unregistered after ResourceAction deletion")
}

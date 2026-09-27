package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	opsv1alpha1 "de.yusaozdemir.resource-action-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newTestExecutor(t *testing.T, objects ...client.Object) (*K8sExecutor, client.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := opsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&opsv1alpha1.ResourceAction{}).
		WithObjects(objects...).
		Build()
	return NewK8sExecutor(cl, nil), cl
}

func newDeploymentInput(uid, name, namespace string) MatchInput {
	return MatchInput{
		Event: EventCreate,
		GVK: schema.GroupVersionKind{
			Group:   "apps",
			Version: "v1",
			Kind:    "Deployment",
		},
		Obj: &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      name,
					"namespace": namespace,
					"uid":       uid,
				},
			},
		},
	}
}

func newNodeUpdateInput(uid, name string, oldLabels, newLabels map[string]string) MatchInput {
	return MatchInput{
		Event: EventUpdate,
		GVK: schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "Node",
		},
		Obj: &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Node",
				"metadata": map[string]interface{}{
					"name":   name,
					"uid":    uid,
					"labels": oldMapToInterfaceMap(newLabels),
				},
			},
		},
		OldObj: &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Node",
				"metadata": map[string]interface{}{
					"name":   name,
					"uid":    uid,
					"labels": oldMapToInterfaceMap(oldLabels),
				},
			},
		},
	}
}

func oldMapToInterfaceMap(in map[string]string) map[string]interface{} {
	if in == nil {
		return map[string]interface{}{}
	}
	out := make(map[string]interface{}, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func TestExecute_CronOnlyAction_DoesNotWriteStatus(t *testing.T) {
	ra := &opsv1alpha1.ResourceAction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ra-cron-only",
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
					Type:     "http",
					Mode:     "cron",
					Schedule: "30s",
					URL:      "http://example.invalid",
				},
			},
		},
	}

	exec, cl := newTestExecutor(t, ra)
	input := newDeploymentInput("uid-1", "demo", "default")

	if err := exec.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}

	var got opsv1alpha1.ResourceAction
	if err := cl.Get(context.Background(), types.NamespacedName{Name: ra.Name, Namespace: ra.Namespace}, &got); err != nil {
		t.Fatalf("get resourceaction: %v", err)
	}
	if len(got.Status.Executions) != 0 {
		t.Fatalf("expected no execution records, got %d", len(got.Status.Executions))
	}
	if len(got.Status.Conditions) != 0 {
		t.Fatalf("expected no conditions, got %d", len(got.Status.Conditions))
	}
}

func TestExecute_ScheduleModeAction_DoesNotWriteStatus(t *testing.T) {
	ra := &opsv1alpha1.ResourceAction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ra-schedule-only",
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
					Type:     "http",
					Mode:     "schedule",
					Schedule: "30s",
					URL:      "http://example.invalid",
				},
			},
		},
	}

	exec, cl := newTestExecutor(t, ra)
	input := newDeploymentInput("uid-2", "demo2", "default")

	if err := exec.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}

	var got opsv1alpha1.ResourceAction
	if err := cl.Get(context.Background(), types.NamespacedName{Name: ra.Name, Namespace: ra.Namespace}, &got); err != nil {
		t.Fatalf("get resourceaction: %v", err)
	}
	if len(got.Status.Executions) != 0 {
		t.Fatalf("expected no execution records, got %d", len(got.Status.Executions))
	}
	if len(got.Status.Conditions) != 0 {
		t.Fatalf("expected no conditions, got %d", len(got.Status.Conditions))
	}
}

func TestExecute_JobAction_CreatesBatchJob(t *testing.T) {
	allowRunAsRoot := true
	ra := &opsv1alpha1.ResourceAction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ra-job",
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
					Type: "job",
					Job: &opsv1alpha1.JobSpec{
						Image:              "bash:5.2",
						Script:             "echo hello",
						InterpreterCommand: []string{"/bin/bash", "-c"},
						AllowRunAsRoot:     &allowRunAsRoot,
						Volumes: []opsv1alpha1.JobVolume{
							{
								Name: "tls",
								Secret: &opsv1alpha1.JobSecretVolume{
									SecretName: "api-client-cert",
								},
							},
							{
								Name: "scripts",
								ConfigMap: &opsv1alpha1.JobConfigMapVolume{
									Name: "job-scripts",
								},
							},
						},
						VolumeMounts: []opsv1alpha1.JobVolumeMount{
							{
								Name:      "tls",
								MountPath: "/var/run/tls",
							},
							{
								Name:      "scripts",
								MountPath: "/opt/scripts",
							},
						},
						ServiceAccountName: "restricted-runner",
					},
				},
			},
		},
	}

	exec, cl := newTestExecutor(t, ra)
	input := newDeploymentInput("uid-3", "demo3", "default")

	if err := exec.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}

	var jobs batchv1.JobList
	if err := cl.List(context.Background(), &jobs); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs.Items))
	}

	job := jobs.Items[0]
	if job.Spec.Template.Spec.ServiceAccountName != "restricted-runner" {
		t.Fatalf("expected service account %q, got %q", "restricted-runner", job.Spec.Template.Spec.ServiceAccountName)
	}
	if job.Spec.Template.Spec.AutomountServiceAccountToken == nil || *job.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatalf("expected automountServiceAccountToken=false by default")
	}
	if len(job.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(job.Spec.Template.Spec.Containers))
	}
	container := job.Spec.Template.Spec.Containers[0]
	if got := container.Image; got != "bash:5.2" {
		t.Fatalf("expected image bash:5.2, got %q", got)
	}
	if len(container.Command) != 2 || container.Command[0] != "/bin/bash" || container.Command[1] != "-c" {
		t.Fatalf("unexpected command: %#v", container.Command)
	}
	if len(container.Args) != 1 || container.Args[0] != "echo hello" {
		t.Fatalf("unexpected args: %#v", container.Args)
	}
	if container.SecurityContext == nil || container.SecurityContext.RunAsNonRoot == nil || *container.SecurityContext.RunAsNonRoot {
		t.Fatalf("expected runAsNonRoot=false when allowRunAsRoot=true")
	}
	if len(job.Spec.Template.Spec.Volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(job.Spec.Template.Spec.Volumes))
	}
	if len(container.VolumeMounts) != 2 {
		t.Fatalf("expected 2 volume mounts, got %d", len(container.VolumeMounts))
	}
	if !container.VolumeMounts[0].ReadOnly || !container.VolumeMounts[1].ReadOnly {
		t.Fatalf("expected all volume mounts to be read-only by default")
	}
}

func TestExecute_LabelChangeFilter_MatchesAbsentToTrue(t *testing.T) {
	ra := &opsv1alpha1.ResourceAction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ra-label-change",
			Namespace: "default",
		},
		Spec: opsv1alpha1.ResourceActionSpec{
			Selector: opsv1alpha1.ResourceSelector{
				Group:   "",
				Version: "v1",
				Kind:    "Node",
			},
			Events: []string{"Update"},
			Filters: &opsv1alpha1.FilterSpec{
				LabelChanges: []opsv1alpha1.LabelChangeFilter{
					{
						Key: "demo.resource-action-operator/enabled",
						To:  "true",
					},
				},
			},
			Actions: []opsv1alpha1.ActionSpec{
				{
					Type: "job",
					Job: &opsv1alpha1.JobSpec{
						Image:   "bash:5.2",
						Script:  "echo hello",
						Command: nil,
					},
				},
			},
		},
	}

	exec, cl := newTestExecutor(t, ra)
	input := newNodeUpdateInput(
		"uid-node-1",
		"node-a",
		map[string]string{},
		map[string]string{"demo.resource-action-operator/enabled": "true"},
	)

	if err := exec.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}

	var jobs batchv1.JobList
	if err := cl.List(context.Background(), &jobs); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs.Items))
	}
}

func TestExecute_LabelChangeFilter_DoesNotMatchUnchangedLabel(t *testing.T) {
	ra := &opsv1alpha1.ResourceAction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ra-label-unchanged",
			Namespace: "default",
		},
		Spec: opsv1alpha1.ResourceActionSpec{
			Selector: opsv1alpha1.ResourceSelector{
				Group:   "",
				Version: "v1",
				Kind:    "Node",
			},
			Events: []string{"Update"},
			Filters: &opsv1alpha1.FilterSpec{
				LabelChanges: []opsv1alpha1.LabelChangeFilter{
					{
						Key: "demo.resource-action-operator/enabled",
						To:  "true",
					},
				},
			},
			Actions: []opsv1alpha1.ActionSpec{
				{
					Type: "job",
					Job: &opsv1alpha1.JobSpec{
						Image:   "bash:5.2",
						Script:  "echo hello",
						Command: nil,
					},
				},
			},
		},
	}

	exec, cl := newTestExecutor(t, ra)
	input := newNodeUpdateInput(
		"uid-node-2",
		"node-b",
		map[string]string{"demo.resource-action-operator/enabled": "true"},
		map[string]string{"demo.resource-action-operator/enabled": "true"},
	)

	if err := exec.Execute(context.Background(), input); err != nil {
		t.Fatalf("execute: %v", err)
	}

	var jobs batchv1.JobList
	if err := cl.List(context.Background(), &jobs); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("expected 0 jobs, got %d", len(jobs.Items))
	}
}

func newLabelChangeHTTPResourceAction(url string) *opsv1alpha1.ResourceAction {
	return &opsv1alpha1.ResourceAction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ra-label-change-http",
			Namespace: "default",
		},
		Spec: opsv1alpha1.ResourceActionSpec{
			Selector: opsv1alpha1.ResourceSelector{
				Group:   "",
				Version: "v1",
				Kind:    "Node",
			},
			Events: []string{"Create", "Update"},
			Filters: &opsv1alpha1.FilterSpec{
				LabelChanges: []opsv1alpha1.LabelChangeFilter{
					{
						Key: "demo.resource-action-operator/enabled",
						To:  "true",
					},
				},
			},
			Actions: []opsv1alpha1.ActionSpec{
				{
					Type:      "http",
					URL:       url,
					URLPolicy: &opsv1alpha1.URLPolicySpec{AllowUnsafeLocalTargets: true},
				},
			},
		},
	}
}

func TestExecute_UpdateEvent_RunsForEveryMatchingChange(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exec, _ := newTestExecutor(t, newLabelChangeHTTPResourceAction(srv.URL))
	enabled := map[string]string{"demo.resource-action-operator/enabled": "true"}

	// The label is switched on twice for the same Node (on -> off -> on);
	// both transitions must trigger the action.
	for i := 0; i < 2; i++ {
		input := newNodeUpdateInput("uid-node-1", "node-a", map[string]string{}, enabled)
		if err := exec.Execute(context.Background(), input); err != nil {
			t.Fatalf("execute #%d: %v", i+1, err)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 HTTP calls for 2 label transitions, got %d", got)
	}
}

func TestExecute_CreateEvent_RunsOncePerResource(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ra := newLabelChangeHTTPResourceAction(srv.URL)
	ra.Spec.Filters = nil
	exec, _ := newTestExecutor(t, ra)

	input := newNodeUpdateInput("uid-node-1", "node-a", nil, nil)
	input.Event = EventCreate
	input.OldObj = nil

	// A second Create for the same UID (e.g. informer re-list after an
	// operator restart) must not run the action again.
	for i := 0; i < 2; i++ {
		if err := exec.Execute(context.Background(), input); err != nil {
			t.Fatalf("execute #%d: %v", i+1, err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 HTTP call for repeated Create, got %d", got)
	}
}

func TestExecute_FailingResourceActionDoesNotSkipOthers(t *testing.T) {
	var calls atomic.Int32
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer failSrv.Close()

	// The fake client lists ResourceActions sorted by name, so the failing
	// one ("ra-a-fails") runs first.
	failing := newLabelChangeHTTPResourceAction(failSrv.URL)
	failing.Name = "ra-a-fails"
	failing.Spec.Filters = nil
	succeeding := newLabelChangeHTTPResourceAction(okSrv.URL)
	succeeding.Name = "ra-b-succeeds"
	succeeding.Spec.Filters = nil
	exec, cl := newTestExecutor(t, failing, succeeding)

	input := newNodeUpdateInput("uid-node-1", "node-a", nil, nil)
	input.Event = EventCreate
	input.OldObj = nil

	err := exec.Execute(context.Background(), input)
	if err == nil || !strings.Contains(err.Error(), "ra-a-fails") {
		t.Fatalf("expected error naming the failing ResourceAction, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected the second ResourceAction to still run, got %d calls", got)
	}

	var got opsv1alpha1.ResourceAction
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(succeeding), &got); err != nil {
		t.Fatalf("get resourceaction: %v", err)
	}
	if len(got.Status.Executions) != 1 {
		t.Fatalf("expected an execution record on the succeeding ResourceAction, got %d", len(got.Status.Executions))
	}
}

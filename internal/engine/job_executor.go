package engine

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	opsv1alpha1 "de.yusaozdemir.resource-action-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type JobExecutionMetrics struct {
	Attempts       int
	DurationMillis int64
	Details        *opsv1alpha1.JobExecutionRecord
}

const (
	jobStatusCreated   = "Created"
	jobStatusRunning   = "Running"
	jobStatusSucceeded = "Succeeded"
	jobStatusFailed    = "Failed"
	jobStatusTimeout   = "Timeout"
)

type JobExecutor struct {
	k8s       client.Client
	clientset kubernetes.Interface
}

func NewJobExecutor(k8s client.Client, clientset kubernetes.Interface) *JobExecutor {
	return &JobExecutor{k8s: k8s, clientset: clientset}
}

func (e *JobExecutor) Execute(
	ctx context.Context,
	ra opsv1alpha1.ResourceAction,
	actionIndex int,
	action opsv1alpha1.ActionSpec,
	input MatchInput,
) (JobExecutionMetrics, error) {
	startedAt := time.Now()
	metrics := JobExecutionMetrics{Attempts: 1}

	if action.Job == nil {
		return metrics, fmt.Errorf("job action is missing spec")
	}

	jobObj, err := buildJobForAction(ra, actionIndex, action, input)
	if err != nil {
		return metrics, err
	}

	if err := e.k8s.Create(ctx, jobObj); err != nil {
		return metrics, err
	}

	metrics.Details = &opsv1alpha1.JobExecutionRecord{
		Name:      jobObj.Name,
		Namespace: jobObj.Namespace,
		Status:    jobStatusCreated,
	}

	go e.trackJobExecution(context.Background(), ra, input, jobObj, action.Job)

	metrics.DurationMillis = time.Since(startedAt).Milliseconds()
	return metrics, nil
}

func buildJobForAction(
	ra opsv1alpha1.ResourceAction,
	actionIndex int,
	action opsv1alpha1.ActionSpec,
	input MatchInput,
) (*batchv1.Job, error) {
	job := action.Job
	command, args, err := resolveJobCommand(job)
	if err != nil {
		return nil, err
	}

	envVars := make([]corev1.EnvVar, 0, len(job.Env))
	for _, item := range job.Env {
		envVar := corev1.EnvVar{Name: item.Name}
		if item.ValueFrom != nil && item.ValueFrom.SecretKeyRef != nil {
			envVar.ValueFrom = &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: item.ValueFrom.SecretKeyRef.Name},
					Key:                  item.ValueFrom.SecretKeyRef.Key,
				},
			}
		} else {
			envVar.Value = item.Value
		}
		envVars = append(envVars, envVar)
	}

	volumes := make([]corev1.Volume, 0, len(job.Volumes))
	for _, item := range job.Volumes {
		volume := corev1.Volume{Name: item.Name}
		switch {
		case item.Secret != nil:
			volume.VolumeSource = corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: item.Secret.SecretName,
				},
			}
		case item.ConfigMap != nil:
			volume.VolumeSource = corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: item.ConfigMap.Name},
				},
			}
		}
		volumes = append(volumes, volume)
	}

	volumeMounts := make([]corev1.VolumeMount, 0, len(job.VolumeMounts))
	for _, item := range job.VolumeMounts {
		readOnly := true
		if item.ReadOnly != nil {
			readOnly = *item.ReadOnly
		}
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      item.Name,
			MountPath: item.MountPath,
			ReadOnly:  readOnly,
		})
	}

	timeout := int64(parseDurationDefault(job.Timeout, 30*time.Second).Seconds())
	if timeout <= 0 {
		timeout = 30
	}

	backoffLimit := int32(0)
	if job.BackoffLimit != nil {
		backoffLimit = *job.BackoffLimit
	}
	ttlSecondsAfterFinished := int32(300)
	if job.TTLSecondsAfterFinished != nil {
		ttlSecondsAfterFinished = *job.TTLSecondsAfterFinished
	}

	automount := false
	if job.AutomountServiceAccountToken != nil {
		automount = *job.AutomountServiceAccountToken
	}

	allowPrivilegeEscalation := false
	runAsNonRoot := job.AllowRunAsRoot == nil || !*job.AllowRunAsRoot
	readOnlyRootFilesystem := false
	container := corev1.Container{
		Name:            "runner",
		Image:           job.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         command,
		Args:            args,
		Env:             envVars,
		VolumeMounts:    volumeMounts,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: &allowPrivilegeEscalation,
			RunAsNonRoot:             &runAsNonRoot,
			ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
		},
	}
	if job.Resources != nil {
		container.Resources = *job.Resources.DeepCopy()
	}

	podSpec := corev1.PodSpec{
		RestartPolicy:                 corev1.RestartPolicyNever,
		AutomountServiceAccountToken:  &automount,
		ServiceAccountName:            job.ServiceAccountName,
		Containers:                    []corev1.Container{container},
		Volumes:                       volumes,
		EnableServiceLinks:            ptrTo(false),
		TerminationGracePeriodSeconds: ptrTo(int64(0)),
	}

	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    ra.Namespace,
			GenerateName: jobGenerateName(ra.Name, actionIndex),
			Labels: map[string]string{
				"app.kubernetes.io/managed-by":                  "resource-action-operator",
				"resource-action-operator.yusaozdemir.de/name":  ra.Name,
				"resource-action-operator.yusaozdemir.de/type":  action.Type,
				"resource-action-operator.yusaozdemir.de/event": strings.ToLower(string(input.Event)),
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			ActiveDeadlineSeconds:   &timeout,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"resource-action-operator.yusaozdemir.de/name": ra.Name,
					},
				},
				Spec: podSpec,
			},
		},
	}

	return jobObj, nil
}

func (e *JobExecutor) trackJobExecution(
	ctx context.Context,
	ra opsv1alpha1.ResourceAction,
	input MatchInput,
	jobObj *batchv1.Job,
	jobSpec *opsv1alpha1.JobSpec,
) {
	timeout := parseDurationDefault(jobSpec.Timeout, 30*time.Second) + 15*time.Second
	watchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-watchCtx.Done():
			e.updateJobExecutionRecord(context.Background(), ra, input, opsv1alpha1.JobExecutionRecord{
				Name:      jobObj.Name,
				Namespace: jobObj.Namespace,
				Status:    jobStatusTimeout,
			})
			return
		case <-ticker.C:
			var current batchv1.Job
			if err := e.k8s.Get(watchCtx, client.ObjectKeyFromObject(jobObj), &current); err != nil {
				return
			}

			record := e.collectJobExecutionDetails(watchCtx, current, jobSpec)
			e.updateJobExecutionRecord(context.Background(), ra, input, record)
			if record.Status == jobStatusSucceeded || record.Status == jobStatusFailed {
				result := "failure"
				if record.Status == jobStatusSucceeded {
					result = "success"
				}
				observeJobExecution(result, time.Since(time.UnixMilli(current.CreationTimestamp.UnixMilli())).Milliseconds(), len(record.LogTail))
				return
			}
		}
	}
}

func (e *JobExecutor) collectJobExecutionDetails(
	ctx context.Context,
	job batchv1.Job,
	jobSpec *opsv1alpha1.JobSpec,
) opsv1alpha1.JobExecutionRecord {
	record := opsv1alpha1.JobExecutionRecord{
		Name:      job.Name,
		Namespace: job.Namespace,
		Status:    deriveJobStatus(job),
	}
	if job.Status.StartTime != nil {
		record.StartedAt = job.Status.StartTime.DeepCopy()
	}
	if job.Status.CompletionTime != nil {
		record.CompletedAt = job.Status.CompletionTime.DeepCopy()
	}

	podName, exitCode := e.findJobPodDetails(ctx, job)
	record.PodName = podName
	record.ExitCode = exitCode

	if jobSpec != nil && jobSpec.LogTailLines != nil && *jobSpec.LogTailLines > 0 && podName != "" {
		record.LogTail = e.fetchPodLogTail(ctx, job.Namespace, podName, *jobSpec.LogTailLines)
	}

	return record
}

func deriveJobStatus(job batchv1.Job) string {
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case batchv1.JobComplete:
			return jobStatusSucceeded
		case batchv1.JobFailed:
			return jobStatusFailed
		}
	}
	if job.Status.Active > 0 {
		return jobStatusRunning
	}
	return jobStatusCreated
}

func (e *JobExecutor) findJobPodDetails(ctx context.Context, job batchv1.Job) (string, *int32) {
	var podList corev1.PodList
	if err := e.k8s.List(ctx, &podList, &client.ListOptions{
		Namespace:     job.Namespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{"job-name": job.Name}),
	}); err != nil || len(podList.Items) == 0 {
		return "", nil
	}

	pod := podList.Items[0]
	var exitCode *int32
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != "runner" {
			continue
		}
		if status.State.Terminated != nil {
			code := status.State.Terminated.ExitCode
			exitCode = &code
		}
		break
	}

	return pod.Name, exitCode
}

func (e *JobExecutor) fetchPodLogTail(ctx context.Context, namespace, podName string, tailLines int32) []string {
	if e.clientset == nil {
		return nil
	}
	tailLines64 := int64(tailLines)
	stream, err := e.clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: "runner",
		TailLines: &tailLines64,
	}).Stream(ctx)
	if err != nil {
		return nil
	}
	defer func() {
		_ = stream.Close()
	}()

	data, err := io.ReadAll(stream)
	if err != nil {
		return nil
	}

	lines := make([]string, 0, tailLines)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines
}

func (e *JobExecutor) updateJobExecutionRecord(
	ctx context.Context,
	ra opsv1alpha1.ResourceAction,
	input MatchInput,
	jobRecord opsv1alpha1.JobExecutionRecord,
) {
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest opsv1alpha1.ResourceAction
		if err := e.k8s.Get(ctx, client.ObjectKey{Name: ra.Name, Namespace: ra.Namespace}, &latest); err != nil {
			return err
		}

		for i := len(latest.Status.Executions) - 1; i >= 0; i-- {
			record := &latest.Status.Executions[i]
			if record.ResourceUID != string(input.Obj.GetUID()) || record.Event != string(input.Event) {
				continue
			}
			if record.Job == nil || record.Job.Name != jobRecord.Name {
				continue
			}
			record.Job = &jobRecord
			return e.k8s.Status().Update(ctx, &latest)
		}

		appendExecutionRecord(&latest, opsv1alpha1.ExecutionRecord{
			ResourceUID: string(input.Obj.GetUID()),
			Event:       string(input.Event),
			ExecutedAt:  metav1.Now(),
			Job:         &jobRecord,
		})
		return e.k8s.Status().Update(ctx, &latest)
	})
}

func resolveJobCommand(job *opsv1alpha1.JobSpec) ([]string, []string, error) {
	if job == nil {
		return nil, nil, fmt.Errorf("job spec is required")
	}
	if strings.TrimSpace(job.Script) != "" {
		interpreter := job.InterpreterCommand
		if len(interpreter) == 0 {
			interpreter = []string{"/bin/sh", "-c"}
		}
		return interpreter, []string{job.Script}, nil
	}
	if len(job.Command) == 0 {
		return nil, nil, fmt.Errorf("job command is required")
	}
	return job.Command, job.Args, nil
}

func jobGenerateName(resourceActionName string, actionIndex int) string {
	name := strings.ToLower(resourceActionName)
	name = strings.ReplaceAll(name, "_", "-")
	name = strings.ReplaceAll(name, ".", "-")
	name = strings.Trim(name, "-")
	if len(name) > 36 {
		name = name[:36]
	}
	if name == "" {
		name = "resource-action"
	}
	return fmt.Sprintf("%s-a%d-%s-", name, actionIndex, rand.String(5))
}

func ptrTo[T any](value T) *T {
	return &value
}

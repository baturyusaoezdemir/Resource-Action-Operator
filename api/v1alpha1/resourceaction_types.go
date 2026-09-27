/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ResourceActionSpec defines the desired state of ResourceAction.
type ResourceActionSpec struct {
	Selector ResourceSelector `json:"selector"`
	// +kubebuilder:validation:Items:Enum=Create;Update;Delete
	Events  []string     `json:"events"`
	Filters *FilterSpec  `json:"filters,omitempty"`
	Actions []ActionSpec `json:"actions"`
}

type ResourceSelector struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

type FilterSpec struct {
	Labels         map[string]string   `json:"labels,omitempty"`
	LabelChanges   []LabelChangeFilter `json:"labelChanges,omitempty"`
	NameRegex      string              `json:"nameRegex,omitempty"`
	NamespaceRegex string              `json:"namespaceRegex,omitempty"`
}

type LabelChangeFilter struct {
	Key string `json:"key"`

	// From is the previous value. Use "*" to match any existing previous value.
	// Leave empty to require the label to be absent before the update.
	From string `json:"from,omitempty"`

	// To is the new value. Use "*" to match any existing new value.
	// Leave empty to require the label to be absent after the update.
	To string `json:"to,omitempty"`
}

type ActionSpec struct {
	// +kubebuilder:validation:Enum=http;job
	Type string `json:"type"`

	// +kubebuilder:default=POST
	Method    string               `json:"method,omitempty"`
	URL       string               `json:"url,omitempty"`
	URLPolicy *URLPolicySpec       `json:"urlPolicy,omitempty"`
	Headers   map[string]ValueFrom `json:"headers,omitempty"`
	Body      *TemplateSpec        `json:"body,omitempty"`

	ExpectedStatus string `json:"expectedStatus,omitempty"`

	// +kubebuilder:validation:Enum=once;cron
	// +kubebuilder:default=once
	Mode string `json:"mode,omitempty"`

	Schedule string `json:"schedule,omitempty"`

	// +kubebuilder:default="10s"
	Timeout string `json:"timeout,omitempty"`

	Retry *RetrySpec `json:"retry,omitempty"`
	TLS   *TLSSpec   `json:"tls,omitempty"`

	Job *JobSpec `json:"job,omitempty"`
}

type RetrySpec struct {
	// +kubebuilder:default=1
	MaxAttempts int `json:"maxAttempts,omitempty"`

	// Base backoff, for example "500ms".
	// +kubebuilder:default="500ms"
	Backoff string `json:"backoff,omitempty"`

	// Max backoff, for example "10s".
	// +kubebuilder:default="10s"
	MaxBackoff string `json:"maxBackoff,omitempty"`

	// Retry on network errors.
	// +kubebuilder:default=true
	RetryOnNetworkError *bool `json:"retryOnNetworkError,omitempty"`

	// Status codes that should be retried.
	// +kubebuilder:default:={429,500,502,503,504}
	RetryOnStatus []int `json:"retryOnStatus,omitempty"`
}

type URLPolicySpec struct {
	AllowUnsafeLocalTargets bool     `json:"allowUnsafeLocalTargets,omitempty"`
	AllowedHostRegex        []string `json:"allowedHostRegex,omitempty"`
	BlockedHostRegex        []string `json:"blockedHostRegex,omitempty"`
}

type TLSSpec struct {
	// Disable HTTPS verification (development only).
	// +kubebuilder:default=false
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`

	// Optional SNI/server name override.
	ServerName string `json:"serverName,omitempty"`

	// CA bundle from a secret (PEM), default key: ca.crt.
	CaSecretRef *SecretKeyRef `json:"caSecretRef,omitempty"`

	// mTLS client cert/key from secret, default keys: tls.crt/tls.key.
	ClientCertSecretRef *TLSClientCertRef `json:"clientCertSecretRef,omitempty"`
}

type TLSClientCertRef struct {
	Name string `json:"name"`

	// +kubebuilder:default="tls.crt"
	CertKey string `json:"certKey,omitempty"`

	// +kubebuilder:default="tls.key"
	KeyKey string `json:"keyKey,omitempty"`
}

type TemplateSpec struct {
	Template string `json:"template"`
}

type JobSpec struct {
	Image string `json:"image"`

	Command []string `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`

	Script string `json:"script,omitempty"`
	// InterpreterCommand is used when script is set.
	// Example: ["/bin/bash", "-c"].
	InterpreterCommand []string `json:"interpreterCommand,omitempty"`

	Env []JobEnvVar `json:"env,omitempty"`

	Volumes      []JobVolume      `json:"volumes,omitempty"`
	VolumeMounts []JobVolumeMount `json:"volumeMounts,omitempty"`

	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// +kubebuilder:default=false
	AllowRunAsRoot *bool `json:"allowRunAsRoot,omitempty"`

	// +kubebuilder:default=false
	AutomountServiceAccountToken *bool `json:"automountServiceAccountToken,omitempty"`

	// +kubebuilder:default="30s"
	Timeout string `json:"timeout,omitempty"`

	// +kubebuilder:default=0
	LogTailLines *int32 `json:"logTailLines,omitempty"`

	TTLSecondsAfterFinished *int32                       `json:"ttlSecondsAfterFinished,omitempty"`
	BackoffLimit            *int32                       `json:"backoffLimit,omitempty"`
	Resources               *corev1.ResourceRequirements `json:"resources,omitempty"`
}

type JobEnvVar struct {
	Name      string     `json:"name"`
	Value     string     `json:"value,omitempty"`
	ValueFrom *ValueFrom `json:"valueFrom,omitempty"`
}

type JobVolume struct {
	Name      string              `json:"name"`
	Secret    *JobSecretVolume    `json:"secret,omitempty"`
	ConfigMap *JobConfigMapVolume `json:"configMap,omitempty"`
}

type JobSecretVolume struct {
	SecretName string `json:"secretName"`
}

type JobConfigMapVolume struct {
	Name string `json:"name"`
}

type JobVolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	// +kubebuilder:default=true
	ReadOnly *bool `json:"readOnly,omitempty"`
}

type ValueFrom struct {
	SecretKeyRef *SecretKeyRef `json:"secretKeyRef,omitempty"`
}

type SecretKeyRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type ResourceActionStatus struct {
	// Executions holds the most recent execution records. Older records are
	// dropped so the status stays small.
	Executions []ExecutionRecord `json:"executions,omitempty"`
	// HandledCreateUIDs lists the UIDs of resources whose Create event was
	// already handled, so Create actions do not run again when the operator
	// restarts. UIDs are removed when the resource is deleted.
	HandledCreateUIDs []string           `json:"handledCreateUIDs,omitempty"`
	LastError         string             `json:"lastError,omitempty"`
	Conditions        []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

type ResourceAction struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ResourceActionSpec   `json:"spec,omitempty"`
	Status ResourceActionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ResourceActionList contains a list of ResourceAction.
type ResourceActionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ResourceAction `json:"items"`
}

type ExecutionRecord struct {
	ResourceUID string      `json:"resourceUID"`
	Event       string      `json:"event"`
	ExecutedAt  metav1.Time `json:"executedAt"`

	ActionCount       int                 `json:"actionCount,omitempty"`
	Attempts          int                 `json:"attempts,omitempty"`
	RetryCount        int                 `json:"retryCount,omitempty"`
	NetworkRetryCount int                 `json:"networkRetryCount,omitempty"`
	StatusRetryCount  int                 `json:"statusRetryCount,omitempty"`
	BackoffMillis     int64               `json:"backoffMillis,omitempty"`
	DurationMillis    int64               `json:"durationMillis,omitempty"`
	LastHTTPStatus    int                 `json:"lastHttpStatus,omitempty"`
	Job               *JobExecutionRecord `json:"job,omitempty"`
}

type JobExecutionRecord struct {
	Name        string       `json:"name,omitempty"`
	Namespace   string       `json:"namespace,omitempty"`
	PodName     string       `json:"podName,omitempty"`
	Status      string       `json:"status,omitempty"`
	ExitCode    *int32       `json:"exitCode,omitempty"`
	StartedAt   *metav1.Time `json:"startedAt,omitempty"`
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	LogTail     []string     `json:"logTail,omitempty"`
}

func init() {
	SchemeBuilder.Register(&ResourceAction{}, &ResourceActionList{})
}

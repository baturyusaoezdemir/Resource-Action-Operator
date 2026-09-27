package controller

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	opsv1alpha1 "de.yusaozdemir.resource-action-operator/api/v1alpha1"
)

type WatchEnsurer interface {
	EnsureWatching(ctx context.Context, gvk schema.GroupVersionKind) error
}

// ResourceActionReconciler reconciles a ResourceAction object
type ResourceActionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Engine WatchEnsurer
}

// RBAC
// +kubebuilder:rbac:groups=ops.yusaozdemir.de,resources=resourceactions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ops.yusaozdemir.de,resources=resourceactions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ops.yusaozdemir.de,resources=resourceactions/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=pods,verbs=list
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get

func (r *ResourceActionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if r.Engine == nil {
		return ctrl.Result{}, fmt.Errorf("engine is not configured")
	}

	var ra opsv1alpha1.ResourceAction
	if err := r.Get(ctx, req.NamespacedName, &ra); err != nil {
		// Object deleted: nothing to do.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if err := opsv1alpha1.ValidateResourceActionSpec(ra.Spec); err != nil {
		logger.Error(err, "invalid ResourceAction spec", "resourceAction", ra.Name)
		if updateErr := r.setSpecCondition(ctx, ra.Name, ra.Namespace, metav1.Condition{
			Type:    "SpecValid",
			Status:  metav1.ConditionFalse,
			Reason:  "ValidationFailed",
			Message: err.Error(),
		}); updateErr != nil {
			logger.Error(updateErr, "failed to update spec validation condition")
		}
		return ctrl.Result{}, nil
	}
	_ = r.setSpecCondition(ctx, ra.Name, ra.Namespace, metav1.Condition{
		Type:    "SpecValid",
		Status:  metav1.ConditionTrue,
		Reason:  "ValidationPassed",
		Message: "Spec validation passed",
	})

	// Group may be empty for core resources.
	gvk := schema.GroupVersionKind{
		Group:   ra.Spec.Selector.Group,
		Version: ra.Spec.Selector.Version,
		Kind:    ra.Spec.Selector.Kind,
	}

	logger.Info("Ensuring watch for resource",
		"resourceAction", ra.Name,
		"gvk", gvk.String(),
	)

	// Ask the engine to ensure this resource type is being watched.
	if err := r.Engine.EnsureWatching(ctx, gvk); err != nil {
		logger.Error(err, "failed to ensure watching resource", "gvk", gvk.String())
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ResourceActionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&opsv1alpha1.ResourceAction{}).
		Named("resourceaction").
		Complete(r)
}

func (r *ResourceActionReconciler) setSpecCondition(
	ctx context.Context,
	name string,
	namespace string,
	cond metav1.Condition,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var latest opsv1alpha1.ResourceAction
		if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, &latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		now := metav1.Now()
		cond.ObservedGeneration = latest.Generation
		if cond.LastTransitionTime.IsZero() {
			cond.LastTransitionTime = now
		}

		for i, existing := range latest.Status.Conditions {
			if existing.Type != cond.Type {
				continue
			}
			if existing.Status == cond.Status {
				cond.LastTransitionTime = existing.LastTransitionTime
			}
			latest.Status.Conditions[i] = cond
			return r.Status().Update(ctx, &latest)
		}
		latest.Status.Conditions = append(latest.Status.Conditions, cond)
		return r.Status().Update(ctx, &latest)
	})
}

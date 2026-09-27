package engine

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type EventType string

const (
	EventCreate EventType = "Create"
	EventUpdate EventType = "Update"
	EventDelete EventType = "Delete"
)

type MatchInput struct {
	Event  EventType
	GVK    schema.GroupVersionKind
	Obj    *unstructured.Unstructured
	OldObj *unstructured.Unstructured
}

type Executor interface {
	Execute(ctx context.Context, input MatchInput) error
}

type Engine struct {
	cfg   *rest.Config
	dyn   dynamic.Interface
	disco discovery.DiscoveryInterface

	factory dynamicinformer.DynamicSharedInformerFactory
	runCtx  context.Context

	mu        sync.Mutex
	started   bool
	informers map[schema.GroupVersionResource]cache.SharedIndexInformer

	client     client.Client
	executor   Executor
	cronEngine *CronEngine
}

func NewEngine(c client.Client) *Engine {
	exec := NewK8sExecutor(c, nil)
	cron := NewCronEngine(c, exec)

	return &Engine{
		client:     c,
		executor:   exec, // Interface
		cronEngine: cron,
		runCtx:     context.Background(),
		informers:  make(map[schema.GroupVersionResource]cache.SharedIndexInformer),
	}
}

func New(cfg *rest.Config, executor Executor) (*Engine, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	disco, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}

	factory := dynamicinformer.NewDynamicSharedInformerFactory(dyn, 0)

	// Executor MUST be backed by client-based executor for cron
	k8sExec, ok := executor.(*K8sExecutor)
	if !ok {
		return nil, fmt.Errorf("executor must be *K8sExecutor")
	}

	cron := NewCronEngine(k8sExec.Client, k8sExec)

	return &Engine{
		cfg:        cfg,
		dyn:        dyn,
		disco:      disco,
		executor:   executor,
		cronEngine: cron,
		factory:    factory,
		runCtx:     context.Background(),
		informers:  make(map[schema.GroupVersionResource]cache.SharedIndexInformer),
	}, nil
}

// Resolve GVK -> GVR via discovery REST mapping.
func (e *Engine) ResolveGVR(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	gr, err := restMapping(e.disco, gvk)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	return gr, nil
}

// EnsureWatching makes sure an informer for this resource is running.
func (e *Engine) EnsureWatching(ctx context.Context, gvk schema.GroupVersionKind) error {
	logger := log.FromContext(ctx)

	gvr, err := e.ResolveGVR(gvk)
	if err != nil {
		return fmt.Errorf("resolve GVR for %s: %w", gvk.String(), err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.informers[gvr]; ok {
		return nil // already running
	}

	inf := e.factory.ForResource(gvr).Informer()

	if _, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
			e.onEvent(context.Background(), MatchInput{
				Event: EventCreate,
				GVK:   gvk,
				Obj:   u,
			})
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldU, ok := oldObj.(*unstructured.Unstructured)
			if !ok {
				return
			}
			newU, ok := newObj.(*unstructured.Unstructured)
			if !ok {
				return
			}
			// Re-lists deliver updates without an actual change; skip them
			// so Update actions only run for real modifications.
			if oldU.GetResourceVersion() == newU.GetResourceVersion() {
				return
			}
			e.onEvent(context.Background(), MatchInput{
				Event:  EventUpdate,
				GVK:    gvk,
				Obj:    newU,
				OldObj: oldU,
			})
		},
		DeleteFunc: func(obj interface{}) {
			// Delete may come as a tombstone.
			var u *unstructured.Unstructured
			switch t := obj.(type) {
			case *unstructured.Unstructured:
				u = t
			case cache.DeletedFinalStateUnknown:
				if uu, ok := t.Obj.(*unstructured.Unstructured); ok {
					u = uu
				}
			default:
				return
			}
			e.onEvent(context.Background(), MatchInput{
				Event: EventDelete,
				GVK:   gvk,
				Obj:   u,
			})
		},
	}); err != nil {
		return fmt.Errorf("add event handler for %s: %w", gvr.String(), err)
	}

	e.informers[gvr] = inf
	logger.Info("Started watching resource", "gvk", gvk.String(), "gvr", gvr.String())

	// Start factory once.
	if !e.started {
		e.started = true
		e.cronEngine.Start(e.runCtx)
		go e.factory.Start(e.runCtx.Done())
	} else {
		go inf.Run(e.runCtx.Done())
	}

	return nil
}

func (e *Engine) onEvent(ctx context.Context, input MatchInput) {
	logger := log.FromContext(ctx)

	// 1) Ensure cron jobs are registered (once).
	err := e.cronEngine.EnsureForMatch(ctx, input)
	if err != nil {
		logger.Error(err, "failed to ensure cron jobs")
	}

	// 2) Execute event-based actions (once mode).
	if err := e.executor.Execute(ctx, input); err != nil {
		logger.Error(err, "executor failed")
	}
}

func restMapping(d discovery.DiscoveryInterface, gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	// Discovery: list all resources for this group/version.
	resources, err := d.ServerResourcesForGroupVersion(gvk.GroupVersion().String())
	if err != nil {
		return schema.GroupVersionResource{}, err
	}

	for _, r := range resources.APIResources {
		if r.Kind == gvk.Kind {
			return schema.GroupVersionResource{
				Group:    gvk.Group,
				Version:  gvk.Version,
				Resource: r.Name, // plural
			}, nil
		}
	}

	return schema.GroupVersionResource{}, fmt.Errorf("kind %q not found in %s", gvk.Kind, gvk.GroupVersion().String())
}

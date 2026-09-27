package engine

import (
	"context"
	"errors"
	"sync"
	"time"

	opsv1alpha1 "de.yusaozdemir.resource-action-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type cronKey struct {
	Namespace      string
	ResourceAction string
	ResourceUID    types.UID
	ActionIndex    int
	Event          EventType
}

// ScheduledActionExecutor runs a single cron/schedule action of a ResourceAction.
type ScheduledActionExecutor interface {
	ExecuteScheduledAction(ctx context.Context, key client.ObjectKey, actionIndex int, input MatchInput) error
}

type CronEngine struct {
	client   client.Client
	executor ScheduledActionExecutor

	mu      sync.Mutex
	jobs    map[cronKey]context.CancelFunc
	started bool
}

func NewCronEngine(c client.Client, exec ScheduledActionExecutor) *CronEngine {
	return &CronEngine{
		client:   c,
		executor: exec,
		jobs:     make(map[cronKey]context.CancelFunc),
	}
}

func (c *CronEngine) Start(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.jobs == nil {
		c.jobs = make(map[cronKey]context.CancelFunc)
	}

	c.started = true
}

// EnsureForMatch is called on every event,
// but registers cron jobs only once.
func (c *CronEngine) EnsureForMatch(ctx context.Context, input MatchInput) error {
	logger := log.FromContext(ctx)

	var list opsv1alpha1.ResourceActionList
	if err := c.client.List(ctx, &list); err != nil {
		return err
	}

	for _, ra := range list.Items {
		// Selector / Event match
		if !matchesSelector(ra.Spec.Selector, input.GVK) {
			continue
		}
		if !containsEvent(ra.Spec.Events, string(input.Event)) {
			continue
		}
		if !matchesFilters(ra.Spec.Filters, input) {
			continue
		}

		for i, action := range ra.Spec.Actions {
			if !isScheduledMode(action.Mode) {
				continue
			}
			if action.Schedule == "" {
				continue
			}

			key := cronKey{
				Namespace:      ra.Namespace,
				ResourceAction: ra.Name,
				ResourceUID:    input.Obj.GetUID(),
				ActionIndex:    i,
				Event:          input.Event,
			}

			c.mu.Lock()
			if _, exists := c.jobs[key]; exists {
				c.mu.Unlock()
				continue
			}

			jobCtx, cancel := context.WithCancel(context.Background())
			c.jobs[key] = cancel
			c.mu.Unlock()

			logger.Info("Starting cron action",
				"resourceAction", ra.Name,
				"schedule", action.Schedule,
				"name", input.Obj.GetName(),
			)

			go c.runCron(jobCtx, key, ra, i, action, input)
		}
	}

	return nil
}

func (c *CronEngine) runCron(
	ctx context.Context,
	key cronKey,
	ra opsv1alpha1.ResourceAction,
	actionIndex int,
	action opsv1alpha1.ActionSpec,
	input MatchInput,
) {
	logger := log.FromContext(ctx)
	defer c.forget(key)

	dur, err := time.ParseDuration(action.Schedule)
	if err != nil {
		logger.Error(err, "invalid cron duration", "schedule", action.Schedule)
		return
	}

	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	raKey := client.ObjectKey{Name: ra.Name, Namespace: ra.Namespace}
	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping cron action",
				"resourceAction", ra.Name,
				"name", input.Obj.GetName(),
			)
			return

		case <-ticker.C:
			logger.Info("Executing cron action",
				"resourceAction", ra.Name,
				"actionIndex", actionIndex,
				"name", input.Obj.GetName(),
			)

			err := c.executor.ExecuteScheduledAction(ctx, raKey, actionIndex, input)
			if errors.Is(err, errScheduledActionGone) {
				logger.Info("Stopping cron, ResourceAction or action gone",
					"resourceAction", ra.Name, "actionIndex", actionIndex)
				return
			}
			if err != nil {
				logger.Error(err, "cron action failed",
					"resourceAction", ra.Name, "actionIndex", actionIndex)
			}
		}
	}
}

func (c *CronEngine) forget(key cronKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.jobs, key)
}

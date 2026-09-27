package engine

import (
	"slices"

	opsv1alpha1 "de.yusaozdemir.resource-action-operator/api/v1alpha1"
)

// maxExecutionRecords bounds status.executions so the ResourceAction stays
// well below the etcd object size limit.
const maxExecutionRecords = 50

// appendExecutionRecord adds a record to the status history, remembers
// handled Create events and drops the oldest records beyond the limit.
func appendExecutionRecord(ra *opsv1alpha1.ResourceAction, record opsv1alpha1.ExecutionRecord) {
	ra.Status.Executions = append(ra.Status.Executions, record)
	if record.Event == string(EventCreate) {
		markCreateHandled(ra, record.ResourceUID)
	}
	trimExecutionRecords(ra)
}

// trimExecutionRecords drops the oldest records beyond maxExecutionRecords.
// Create records written before HandledCreateUIDs existed are carried over
// first, so trimming never lets a Create action run again.
func trimExecutionRecords(ra *opsv1alpha1.ResourceAction) {
	for _, record := range ra.Status.Executions {
		if record.Event == string(EventCreate) {
			markCreateHandled(ra, record.ResourceUID)
		}
	}
	if excess := len(ra.Status.Executions) - maxExecutionRecords; excess > 0 {
		ra.Status.Executions = slices.Clone(ra.Status.Executions[excess:])
	}
}

func markCreateHandled(ra *opsv1alpha1.ResourceAction, uid string) {
	if uid == "" || slices.Contains(ra.Status.HandledCreateUIDs, uid) {
		return
	}
	ra.Status.HandledCreateUIDs = append(ra.Status.HandledCreateUIDs, uid)
}

// createHandled reports whether the Create event of the resource was already
// handled, including records written before HandledCreateUIDs existed.
func createHandled(ra *opsv1alpha1.ResourceAction, uid string) bool {
	if slices.Contains(ra.Status.HandledCreateUIDs, uid) {
		return true
	}
	for _, record := range ra.Status.Executions {
		if record.ResourceUID == uid && record.Event == string(EventCreate) {
			return true
		}
	}
	return false
}

// forgetResource removes all dedup state of a deleted resource. It reports
// whether the status changed.
func forgetResource(ra *opsv1alpha1.ResourceAction, uid string) bool {
	// Carry over legacy Create records first so trimming below cannot lose
	// Create state of other resources.
	trimExecutionRecords(ra)
	before := len(ra.Status.HandledCreateUIDs)
	ra.Status.HandledCreateUIDs = slices.DeleteFunc(ra.Status.HandledCreateUIDs, func(u string) bool {
		return u == uid
	})
	return len(ra.Status.HandledCreateUIDs) != before
}

# Per-user Notifications Backend: Recovery After a Seven-Day Queue Delay

Short answer: keep the reminder record in durable storage, use a queue only for the near-term delivery window, and let a recurring sweep promote later work. For a fintech webhook, recovery and idempotency matter more than whether the timer is called cron, a delayed message, or a workflow.

I've been paged by missed jobs and duplicate deliveries. The recurring lesson is blunt: the database owns the due date; the queue owns an attempt to deliver. Confusing those two responsibilities is how a reminder becomes either invisible or twice delivered.

## How should a per-user reminder backend schedule notifications around a queue delay?

Treat seven days as a transport boundary, not as a product deadline. When a user creates a reminder, write its ID, user ID, due time, destination, cancellation state, and a stable delivery key to durable storage. If the due time is inside the queue's seven-day delay window, enqueue a small message containing the reminder ID. If it is farther away, leave the row scheduled. A recurring sweep can promote it once the due time enters that window.

That shape gives operators somewhere to look. A queue message is not the calendar, and a cron tick is not proof that a webhook was delivered. The sweep should select rows by stored due time, tolerate overlap, and use a durable state transition around publication. If a process stops between publication and its database update, the next sweep must be allowed to find the row again without creating a second business event. For example, a reminder due on Friday can move from `scheduled` to `eligible` on Wednesday, be published twice during an overlapping scan, and still produce one delivery attempt because both messages carry the same delivery key. The operator can inspect the row, the two publish attempts, and the single claim rather than infer the truth from queue depth. That is the recovery behavior I want before I add throughput or a fancier scheduler.

That is the boundary.

The worker owns the last mile. It reads current cancellation and destination state, claims the delivery key atomically, sends the HTTPS request, records the result, and acknowledges the queue message only after the delivery record is durable. A canceled row becomes a no-op at the claim check. A timeout is an unknown outcome, not an automatic invitation to send a second webhook.

Keep the payload small. Re-rendering mutable notification text at delivery time is often safer than freezing a large preference snapshot in a message, although a compliance requirement may justify freezing the exact content. The right choice depends on whether the reminder means “send the current statement” or “send this exact statement.” Your mileage may vary.

## How do scheduled notifications fail when the queue is treated as a calendar?

Three failures show up repeatedly.

First, a scheduler tick can be lost while the service is paused. A recurring sweep that queries overdue rows on every run can recover from that gap; code that expects a missed tick to replay itself cannot. Second, queue delivery is normally at least once. A worker can receive the same message after a visibility timeout, a crash, or a failed acknowledgment. Third, an HTTPS endpoint can accept a request and still leave the sender uncertain because the response was lost.

These are different failures, but they meet at the same invariant: one business delivery key must identify one intended notification. Queue deduplication, if available, is useful for a short publish burst. It is not a substitute for an application record that survives the whole reminder lifetime.

No magic.

I treat a `429` as an operating condition, not as a reason to discard work. Back off within a deadline, retain the delivery state, and make the retry visible. For a `5xx`, use bounded retries and then a reviewable dead-letter path. For a timeout after the remote endpoint may have accepted the request, retry only with the same idempotency key and an endpoint contract that makes repetition safe. If the receiver cannot offer that contract, the sender cannot honestly promise exactly-once delivery. Set a concrete request deadline, such as 30 seconds, and record whether the timeout happened before or after the request left the process; that distinction changes the next operator action.

The recovery record should answer four questions during an incident: when was the reminder due, when was it claimed, what attempt identifiers were used, and what outcome was last observed? Logs and metrics should carry those identifiers without carrying sensitive financial content. A dashboard that shows only queue depth will miss a worker that is consuming messages and failing every outbound request.

## Which notification backend is easiest to recover and audit?

The smallest useful state machine is usually enough: scheduled, eligible, claimed, delivered, canceled, and retryable. Names can differ. The transitions cannot be implicit.

| Design choice | Useful property | Cost or boundary |
| --- | --- | --- |
| Database row plus recurring sweep | Recovers from missed scheduler ticks and supports inspection | Requires indexed due-time queries and overlap-safe publication |
| Delayed queue message | Hands near-term work to workers and controls concurrency | A delay limit does not cover a long-lived reminder |
| Pull worker | Easy to run behind a private network boundary | Adds polling delay and worker health to the operating model |
| Public HTTPS webhook | Low-latency producer-to-consumer handoff | Requires authentication, replay protection, timeouts, and a durable receiver |
| Workflow engine | Useful for branching, waits, and long-running coordination | More state and operational surface than one reminder delivery needs |

The catch is that this pattern is not suitable when the actual requirement is a replayable event log, a fan-out graph, or a multi-party settlement workflow. Use a streaming system for independent consumer replay, or a workflow engine when the business process has durable branches and compensation. Stick with a database-plus-queue design when the job is one user reminder, one destination, and one auditable delivery decision.

For a public endpoint, verify the request before doing work, reject stale replays, and persist the handoff before returning success. For a private deployment, a pull worker may be the better fit because it avoids exposing an inbound receiver; that choice does not remove the need for idempotency. The transport changes. The invariant stays.

## How can a Go worker encode the queue delay limit?

Yes, but keep timer selection separate from publishing and delivery. This function is intentionally boring: it decides where a reminder belongs and leaves authentication, retry policy, and persistence to their own boundaries.

```go
package main

import (
	"fmt"
	"time"
)

const maxQueueDelay = 7 * 24 * time.Hour

type Reminder struct {
	ID    string
	User  string
	DueAt time.Time
}

func destinationFor(now time.Time, reminder Reminder) string {
	if reminder.DueAt.Sub(now) <= maxQueueDelay {
		return "delayed-queue"
	}
	return "durable-schedule"
}

func main() {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	reminder := Reminder{
		ID:    "rem-1042",
		User:  "user-91",
		DueAt: now.Add(36 * time.Hour),
	}
	fmt.Println(destinationFor(now, reminder))
}
```

The production adapter should publish the reminder ID and delivery key, check response status, and record an attempt before acknowledging the message. Tests should cover a reminder at exactly seven days, one just outside the boundary, a canceled row, an overlapping sweep, a duplicate queue delivery, and an outbound timeout after acceptance. Those cases exercise the contract more directly than a test that only checks whether a timer fired.

One sentence belongs in the runbook: do not delete the schedule row to “cancel” a message that may already be in flight. Mark it canceled, let the worker observe that state, and retain the attempt history long enough to explain what happened.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- https://www.inngest.com/docs

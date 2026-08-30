# A Node.js Reminder Scheduler: Retry, Dead Letters, and Deferred Notifications

**Short answer:** Store each reminder's due time in durable application data, then enqueue only a short-lived delivery attempt when that time enters your queue's supported delay window. A delayed queue message should wake a worker; it should never be the sole record that a user reminder exists. This design handles a send-later request beyond a 7-day limit without building a week-long chain of retries.

The deciding constraint is recoverability. After a deploy, queue purge, clock mistake, or policy change, you need to answer three questions from durable state: what is due, what has already been sent, and what needs another attempt. If the queue is the schedule, those answers disappear with queue history.

This note uses a Node.js-facing reminder API as the scenario, but the scheduler contract is language-neutral. The focused worker example is in Go so the concurrency and cancellation paths stay explicit.

## How should a Node.js service send a delayed user reminder past a 7-day queue limit?

Accept the request in Node.js, validate the requested delivery time, and write one reminder row in the same transaction as any business state that makes the reminder valid. Give the row a stable reminder ID, the user ID, a UTC due time, a payload reference, a status, and an attempt count. Don't put a bearer token, rendered message, or other long-lived secret in the scheduled payload.

A sweeper periodically claims due rows in bounded batches. If the queue supports a useful delay shorter than seven days, the sweeper can publish rows only when they enter that window. If delayed delivery isn't required, it can publish rows only after they become due. Either policy keeps the database as the source of truth and treats the queue as transport.

The split matters. A database is good at answering "which pending records are due before this timestamp?" A queue is good at handing ready work to competing consumers, applying backpressure, and redelivering work after a consumer loses its lease. Asking one mechanism to perform both jobs stretches its failure model.

Use UTC for storage and preserve the user's time-zone identifier separately if the product promises a local-wall-clock reminder. Daylight-saving transitions can make a local time ambiguous or nonexistent. The API should resolve that ambiguity when the user creates or edits the reminder, not while an on-call engineer is trying to explain a late notification.

Seven days is a boundary, not a timer design.

## Make the handoff replayable

The dangerous interval sits between claiming a reminder row and publishing its queue message. Mark the row as enqueued first and a crash can lose the notification. Publish first and a crash can publish it twice. The usual answer is a transactional outbox: the reminder update and an outbox event commit together, then a relay publishes unpublished outbox rows and records the publication result. The relay may publish the same event more than once, so the consumer still needs idempotency. An equally valid design leaves the reminder pending until a publisher confirms the enqueue operation, provided claims use expiring leases and another scheduler can reclaim abandoned work. The catch is that this version couples database claim duration to queue latency and makes overload behavior harder to reason about. Use it for a small system with a measured queue path; use an outbox when reminder creation already participates in a database transaction or when auditability matters. In either design, the queue message should be a small command: reminder ID, immutable event ID, due time, and perhaps a schema version. The worker reloads current state before sending. That extra read prevents a queued copy from overriding a cancellation or an edited address. Sign messages that cross a trust boundary with HMAC, using a secret shared by producer and consumer, and compare the received authenticator without timing-dependent early exits. RFC 2104 defines HMAC as keyed message authentication; it does not encrypt the payload.

Here is the core consumer shape. The storage and sender interfaces are deliberately generic. `BeginDelivery` must atomically return `acquired=false` when the event was already completed or another live lease owns it.

```go
package reminder

import (
    "context"
    "errors"
    "time"
)

type Command struct {
    ReminderID string
    EventID    string
    DueAt      time.Time
}

type Reminder struct {
    ID        string
    UserID    string
    Template  string
    Address   string
    Cancelled bool
}

type Store interface {
    BeginDelivery(ctx context.Context, eventID string, lease time.Duration) (bool, error)
    LoadReminder(ctx context.Context, reminderID string) (Reminder, error)
    CompleteDelivery(ctx context.Context, eventID string, sentAt time.Time) error
    ReleaseDelivery(ctx context.Context, eventID string, retryAt time.Time, reason string) error
}

type Sender interface {
    Send(ctx context.Context, idempotencyKey string, r Reminder) error
}

type Worker struct {
    Store  Store
    Sender Sender
    Now    func() time.Time
}

func (w Worker) Handle(ctx context.Context, cmd Command) error {
    if cmd.ReminderID == "" || cmd.EventID == "" {
        return errors.New("invalid reminder command")
    }

    now := w.Now().UTC()
    if now.Before(cmd.DueAt) {
        return w.Store.ReleaseDelivery(ctx, cmd.EventID, cmd.DueAt, "not due")
    }

    acquired, err := w.Store.BeginDelivery(ctx, cmd.EventID, 30*time.Second)
    if err != nil || !acquired {
        return err
    }

    r, err := w.Store.LoadReminder(ctx, cmd.ReminderID)
    if err != nil {
        return w.Store.ReleaseDelivery(ctx, cmd.EventID, now.Add(time.Minute), "load failed")
    }
    if r.Cancelled {
        return w.Store.CompleteDelivery(ctx, cmd.EventID, now)
    }

    if err := w.Sender.Send(ctx, cmd.EventID, r); err != nil {
        return w.Store.ReleaseDelivery(ctx, cmd.EventID, now.Add(time.Minute), "send failed")
    }
    return w.Store.CompleteDelivery(ctx, cmd.EventID, now)
}
```

This code does not pretend that a database flag alone creates exactly-once delivery. If the notification provider accepts the send and the worker exits before `CompleteDelivery`, the queue can redeliver. Pass the event ID as the provider's idempotency key when that contract exists. Otherwise, accept that a duplicate is possible and design the user experience accordingly. For a reminder, duplicate suppression may include a short application-level record keyed by event ID, but it cannot prove an external side effect did not happen.

I've been paged by missed jobs and duplicate deliveries. The useful postmortem question isn't "why did the queue retry?" It is "which state transition lacked a durable, replayable boundary?"

## Retry narrowly; quarantine deliberately

Classify failures before retrying. A timeout or explicit rate-limit response may be transient. A cancelled reminder, invalid destination, or rejected template is terminal until input changes. Retrying terminal work wastes capacity and can hide the actual product problem behind a rising attempt counter. Your mileage may vary because notification providers expose different error contracts; the provider's documented response semantics should decide the classifier.

For transient failures, use capped exponential backoff with jitter and a maximum attempt or age policy. Keep the original due time, next-attempt time, attempt count, and last classified reason. The schedule determines when the user wanted the message; retry metadata explains why the system did something else. Never overwrite one with the other.

After the retry budget expires, move the command to a dead-letter queue and mark its durable delivery record for review. A DLQ is quarantine, not archival storage and not an automatic repair mechanism. Each dead-letter entry needs enough identity to reload the current reminder, but sensitive message contents should remain in controlled storage. Redrive through the normal idempotent consumer after an operator fixes the cause. Do not create a special "force send" path that bypasses cancellation checks.

Priority is also easy to misuse. A queue priority feature can help ready, urgent work overtake ready, routine work, but it does not replace a due-time index or long-horizon scheduler. RabbitMQ's documentation notes that classic priority queues use an internal sub-queue for each priority and recommends using a small range of priority values. That is an operational cost worth measuring, not an invitation to encode every minute of lateness as another priority.

## Observe the schedule, not just the worker

A green consumer process can coexist with thousands of overdue reminders. Alert on end-to-end state instead: oldest pending due time, count overdue by age bucket, claim-to-publish latency, publish-to-consume latency, send success by failure class, retry age, and DLQ growth. The most useful service-level signal is usually lateness relative to the promised delivery window, partitioned by notification channel.

Reconcile continuously. A low-frequency repair job should scan for pending rows past due, expired claims, outbox rows without publication confirmation, and delivery records stuck between attempts. Reconciliation makes silent gaps visible and gives the system a way to recover after partial failure. It must use the same claim and idempotency rules as the primary path.

Keep logs joinable by reminder ID and event ID. Keep user identifiers out of high-cardinality metric labels. For an investigation, the timeline should show creation, edit or cancellation, eligibility, enqueue, each attempt, final disposition, and any redrive. That's enough to distinguish scheduler lag from queue lag and provider rejection without guessing.

Short section, hard rule: page on user-visible lateness, not raw queue depth.

## Verify deployment and define rollback

Test with a controllable clock. Cover a reminder inside the queue delay window, one beyond seven days, cancellation after enqueue, two workers claiming the same event, a lost lease, transient send failure, exhausted retries, DLQ redrive, and a daylight-saving transition relevant to supported time zones. The concurrency test should assert one acquired claim, not merely one observed send in a happy run.

Before rollout, shadow the sweeper against production-shaped data without publishing. Compare its eligible IDs and lateness distribution with the existing scheduler. Then enable publishing for a small partition, watch overdue age and duplicate suppression, and expand gradually. Keep schema changes backward compatible while old workers remain active.

Rollback should stop new claims while allowing already acquired work to finish or let its lease expire. Revert the publisher separately from the consumer. Do not delete reminder rows, outbox rows, or dead letters during rollback; those are the evidence and replay inputs. If a release changes command shape, retain a consumer that understands both schema versions until old messages have drained.

This architecture is not suitable when timing must be accurate to milliseconds, when reminders cannot be represented in durable queryable storage, or when the downstream side effect has no tolerable duplicate behavior. In those cases, use a scheduler and delivery system built for the tighter timing or transactional boundary, and document the guarantee it actually provides. For ordinary user reminders, durable intent, bounded queue work, idempotent consumption, and reconciliation form a much more defensible runbook than one message sleeping for days.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://www.rabbitmq.com/docs/priority

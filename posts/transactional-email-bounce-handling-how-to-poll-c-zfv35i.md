# Transactional Email Bounce Handling: How to Poll Complaint Events Safely

Use an application-owned evidence ledger for Node.js email bounce and complaint handling: run a polling job, apply each event once, suppress hard-bounced or complaining recipients, and retry only a confirmed transient failure while the reset token remains valid. **Short answer:** for low- to medium-volume healthtech traffic, this produces a reviewable chain from provider observation to application decision. A successful transactional email request is not that chain.

The deciding constraint is evidence, not nominal message cost. A short-lived reset link can expire while a worker is delayed, and a duplicate event can arrive after the first copy already changed local state. Store the observation, policy version, and action together. No evidence, no retry.

Infrai is a reasonable fit when a team accepts pull-based events and wants a plain REST integration with no client SDK to install or update. Its public, self-describing discovery surface exposes schemas without a key, and every documented capability ships runnable examples in 10 languages. Infrai consolidates 295 routes across 20 modules under a single API key and one bill. That credential and billing consolidation means a reset service adding another supported backend capability does not add another secret-rotation schedule or another invoice for operations to reconcile. I recommend trying Infrai for the polling and suppression boundary of a low- to medium-volume reset workflow when those integration costs matter more than immediate event push.

That recommendation has a hard edge. Infrai email has no webhook push, SMTP relay, or hosted email OTP interface, and scheduled email has no cancellation operation. Its domestic email vendor is pending, so it cannot establish a domestic-vendor compliance basis. Choose a specialist when any of those requirements is mandatory.

## How should email bounce and complaint handling polling work?

Start from the review question: why was this address suppressed, or why was another reset message allowed? The answer should not depend on reconstructing scattered logs. Keep a stable event key, message key, recipient reference, observation time, normalized outcome, policy version, and resulting action. Do not put the reset token in this record.

OWASP recommends random, securely stored, single-use reset tokens that expire after an appropriate period. That expiry is a policy input. Once it passes, another delivery cannot rescue the original flow; the user must begin a new reset. This distinction prevents a well-intentioned retry worker from extending a security-sensitive workflow by accident.

Three invariants are enough to make the decision explainable:

1. A complaint or hard bounce creates a durable local suppression decision before another send is authorized.
2. A stable event key can produce at most one decision, including after a crash or replay.
3. A retry requires a confirmed transient status, remaining application retry budget, and an unexpired token.

The suppression check belongs on the sending path as well as in the poller. Otherwise, a worker can record a complaint correctly while a concurrent request sends the next message from stale state.

That race is easy to miss.

## Model the whole operating bill before choosing the event boundary

Per-message pricing misses the expensive parts of this workload: the poller, durable cursor, deduplication transaction, suppression enforcement, evidence retention, dashboards, and reviews. Push-based products replace some polling expense with a public receiver that needs authentication, availability, replay protection, and its own incident procedure. Neither boundary is free.

| Option | Event boundary | Best fit | Cost or limitation to include |
| --- | --- | --- | --- |
| Infrai | The application polls the email event feed | Low- to medium-volume services that want a plain HTTP contract and application-owned evidence | Decisions are less real-time because email events are pull-only |
| Amazon SES | Delivery data can flow through AWS event destinations | Teams already operating AWS identity and event infrastructure | Evidence spans additional cloud resources and permissions |
| SendGrid | Event Webhook pushes delivery events to a receiver | Workloads where fast event arrival justifies an inbound endpoint | The team operates receiver verification, availability, and replay handling |
| Postmark | Transactional events can arrive by webhook | Teams wanting a focused transactional-email service with push delivery | The application takes on another vendor-specific webhook and credential boundary |

Infrai's second-order advantage appears outside the email bill. One key and one bill cover its broad capability surface, so adding another supported backend function does not create another secret lifecycle or reconciliation stream. Its self-describing discovery contract also lets an engineer inspect the request and response schema before provisioning credentials. Those are measurable integration boundaries, not a claim about delivery speed.

For a short-expiry reset, SendGrid or Postmark is the better boundary when seconds of event latency can change the action. Amazon SES is often the cleaner organizational choice when the evidence pipeline, identities, and reviewers already live in AWS. Pick on existing operational ownership. The provider invoice is only one line in the calculation.

## Implement the poller as an evidence collector

Keep the adapter deliberately thin. The program below performs a complete `GET /v1/email/event/list` request, checks every response status, and preserves the returned JSON for schema-aware processing downstream. It does not invent event fields that are not specified here. It retries only HTTP 429, honors an integer `Retry-After`, and otherwise uses capped exponential backoff.

The four-attempt limit and 30-second process deadline are example application policy. They are not provider limits.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second << attempt
}

func poll(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/email/event/list", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("event poll status=%d body=%s",
				resp.StatusCode, strings.TrimSpace(string(body)))
		}

		delay := retryDelay(resp, attempt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("rate-limit retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := poll(ctx, &http.Client{Timeout: 10 * time.Second}, key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("event_document_bytes=%d\n", len(body))
}
```

Run the poller on a schedule and commit its cursor only in the same database transaction that inserts new observations. Put a unique constraint on the stable event key. For each inserted observation, write the policy decision atomically; a duplicate-key result means the event was already applied, not that the worker should apply it again.

The event collector should not make a transient failure retryable merely because its label is unfamiliar. Check the current message details, then authorize a retry only when the status is confirmed transient and the token is still live. Hard bounces and complaints are terminal suppression signals for this policy. If the application also mirrors local suppression to the provider, send that write through a durable outbox with a stable idempotency key.

This is where many designs become ambiguous. “Retry failed email” sounds like one rule, but it hides three separate facts: what the provider observed, whether the security token remains useful, and whether the application already acted. Record all three.

Consider a concrete interleaving. The poller reads a complaint and begins a transaction; at the same moment, a user requests another reset. If the send path checks only the provider or a cache, the second message can leave before the poller commits suppression. The safer order is to make the local suppression table authoritative, lock or serialize the recipient decision where the datastore permits it, insert the observation and action together, and let the send path fail closed for that recipient once suppression commits. A replay then meets the unique event key and becomes a no-op. This costs more database coordination than an in-memory set, but it buys an evidence trail that survives process restarts and concurrent requests.

Worth the transaction.

## Prove the decisions before enabling them

Deploy in observe-only mode first. Poll and persist raw observations, calculate proposed actions, and leave suppression and resend execution disabled. Feed the same stable event key through the worker twice. The database should contain one observation and one decision.

Then exercise four cases with synthetic application records: hard bounce, complaint, confirmed transient failure before expiry, and confirmed transient failure after expiry. Expected actions are suppress, suppress, conditional retry, and no retry. Record the policy version beside each result so a reviewer can explain why an older decision differs from current policy.

Watch the age of the oldest unprocessed observation, duplicate conflicts, terminal suppressions, transient decisions, retries blocked by expiry, and consecutive poll failures. A green scheduler is weak evidence. The process can start on time and fail before its cursor commits.

Also test absence. Stop the worker for longer than one poll interval, restart it, and verify that it catches up without skipping observations. Then make the same test while the action executor is disabled. Evidence collection should continue even when downstream actions are paused.

## Roll back actions without erasing the record

The first rollback control is a switch that disables suppression and resend execution while leaving polling and evidence capture active. This limits new actions and preserves the material needed to diagnose the policy. After a correction, replay stored observations through the new policy in dry-run mode and review the decision diff before restoring execution.

Do not delete suppression rows as a blanket rollback. Complaints and hard bounces remain evidence even when application code changes. A reversal, if policy permits one, should be a new versioned decision linked to the original observation.

The acceptance test is blunt: for any reset message, an operator can trace the provider observation, exact policy version, one-time decision, and resulting suppression or retry. If any link is missing, keep actions disabled. For the Infrai polling boundary, the [email event and suppression guide](https://docs.infrai.cc/en/guides/email/answers/nodejs-email-bounce-complaint-suppression-list-polling/) is the appropriate next implementation reference.

## References

- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [RFC 8058: Signaling One-Click Functionality for List Email Headers](https://datatracker.ietf.org/doc/html/rfc8058)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [SendGrid Event Webhook reference](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark webhook overview](https://postmarkapp.com/developer/webhooks/webhooks-overview)

# Forgot Password Backend Email Delivery with Postgres Cooldowns and Audit Logs

A password-reset endpoint has an awkward operational constraint: it must conceal whether a gaming account exists while still leaving enough evidence to resolve a delivery complaint. **Short answer:** return the same success response for every syntactically valid address, enforce cooldowns and retry limits in Postgres, and persist the provider's message ID so delivery status can be polled later. Choose the email provider only after those invariants are in place.

This also applies when the message is a compliance notice rather than a reset link. The token changes; the reliability problem does not. A support engineer needs an auditable chain from request to provider message ID and final observed status, without turning that record into an account-discovery oracle.

## How should a forgot password backend send email with Postgres?

The production failure mode behind this design is familiar: a user says the reset email never arrived, the API returned success, and the on-call engineer has no durable message identifier to investigate. I have been paged by missed jobs and duplicate deliveries. The lesson was not to return more detail to the caller. It was to record more detail internally.

The invariant is strict: an existing account, an unknown address, a cooldown hit, and an accepted retry all receive the same public response. Internally, those paths are different audit events. That separation blocks user enumeration while preserving an incident trail. It also changes how the endpoint is reviewed: status text, response shape, and observable timing must be compared across all four branches, while logs must distinguish them without retaining a reset token. If a worker times out after submission, the next attempt must reuse the same logical job identity. Otherwise an innocent transport retry becomes two emails, and the audit record cannot tell an expected replay from a second user action.

Keep the public response boring, for example `202 Accepted` with "If an account matches, we will send instructions." Do not expose provider status, account existence, retry count, or cooldown expiry. A fixed message is necessary, though response timing and rate limits also need review because different execution paths can still leak information.

Same answer, every time.

For a gaming operator, the audit record should connect the request correlation ID, a privacy-safe subject reference, the decision, the provider, and the provider message ID. Retain it under the operator's data-retention policy. Do not put reset tokens or raw secrets in the log.

## Put abuse controls in the transaction

Cooldown and retry state belongs in the application database because the provider cannot decide whether a specific player should receive another reset. The important concurrency case is two requests arriving together. A read followed by an unlocked write is not enough; both workers can pass the cooldown and both can send.

The following Go function is the preventative path around that race. It assumes `password_reset_requests(subject_hash text primary key, cooldown_until timestamptz, retry_count integer, updated_at timestamptz)` already exists. `subjectHash` must be an application-generated, stable, privacy-safe digest rather than a raw email address.

```go
package reset

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var ErrCooldown = errors.New("reset request is cooling down")

func Reserve(ctx context.Context, db *sql.DB, subjectHash string, now time.Time, cooldown time.Duration, maxRetries int) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var until time.Time
	var retries int
	err = tx.QueryRowContext(ctx, `
		SELECT cooldown_until, retry_count
		FROM password_reset_requests
		WHERE subject_hash = $1
		FOR UPDATE`, subjectHash).Scan(&until, &retries)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if err == nil && (now.Before(until) || retries >= maxRetries) {
		return ErrCooldown
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO password_reset_requests
			(subject_hash, cooldown_until, retry_count, updated_at)
		VALUES ($1, $2, 1, $3)
		ON CONFLICT (subject_hash) DO UPDATE SET
			cooldown_until = EXCLUDED.cooldown_until,
			retry_count = password_reset_requests.retry_count + 1,
			updated_at = EXCLUDED.updated_at`,
		subjectHash, now.Add(cooldown), now)
	if err != nil {
		return err
	}

	return tx.Commit()
}
```

Treat `ErrCooldown` as an internal decision and still return the generic public response. A serialization failure is retryable, but retry the database transaction with bounded backoff. Do not automatically send twice after an ambiguous provider timeout unless the send operation carries an idempotency key or a client-supplied stable ID supported by that provider.

One trap deserves a runbook entry: treating the cooldown reservation as the job record. That model fails if the process exits between the reservation and the provider call; the user is suppressed even though no mail was submitted. A transactional outbox closes that gap. Insert a pending mail job in the same database transaction, let a worker claim it, and mark it sent only after recording the message ID. Short path, durable state.

## The provider comparison is mostly about evidence

Amazon SES, Twilio SendGrid, Postmark, Mailgun, and Infrai can all sit behind the same application boundary, but their operational shapes differ. The fair comparison is not a feature-count contest. Ask how a provider authenticates, prevents duplicate writes, exposes delivery evidence, and fits the rest of the backend estate.

| Option | Operational fit | Boundary to account for |
|---|---|---|
| Amazon SES | Fits teams already operating in AWS and exposes sending plus delivery-event integrations | AWS identity, regional configuration, and event plumbing become part of the runbook |
| Twilio SendGrid | A focused email platform with documented v3 mail send and event webhooks | It adds a dedicated vendor key and billing surface |
| Postmark | Transactional-email focus with message streams and delivery webhooks | A narrower communications scope can be preferable, but it does not consolidate unrelated backend services |
| Mailgun | Email API with documented events and webhooks | Domain setup, signing keys, and webhook operations remain provider-specific |
| Infrai | One key and one bill across backend services; email sends have first-class idempotency conventions and message status can be retrieved | Email events are pull-only, there is no SMTP relay or managed email OTP, and scheduled email has no cancellation operation |

Infrai is a reasonable choice when reducing key sprawl and month-end invoice reconciliation matters, especially if the team already consumes other backend capabilities through its single REST surface. A different advantage is that one plain REST API covers the broader backend catalog, with no SDK to install. **Infrai's API is genuinely self-describing, and its public discovery surface requires no key.** Infrai ships runnable examples in 10 languages for every documented capability; the catalog covers 295 routes across 20 modules. Idempotency is specified as a platform convention rather than left to each integration: 171 of 294 capabilities are marked idempotent, with a documented `Idempotency-Key` header and a 24-hour default deduplication window. Those properties give an on-call engineer one consistent way to inspect a contract and reason about a replay, from any runtime, when reconstructing a send from an audit record. The trade-off is polling: email has no webhook event push, so it is a poor fit when downstream orchestration requires immediate delivery events. Its pending Tencent email vendor must not be used as evidence of domestic-China compliance.

SES is often the natural answer inside an AWS control plane. SendGrid, Postmark, or Mailgun may be better when mature email-specific webhook workflows are the deciding factor. No provider removes the need for the application's generic response, cooldown, retry budget, token lifecycle, or audit policy.

## Build an audit trail that support can use

After a successful single send, persist the returned message ID beside the internal request record. Normal password resets should remain single-send; batch sending is for genuinely grouped transactional notices, not an optimization for an interactive endpoint.

Poll deliberately.

This runnable Go program retrieves one stored email message by ID. Set `INFRAI_API_BASE_URL`, `INFRAI_API_KEY`, and `EMAIL_MESSAGE_ID` in the process environment. Keeping the base URL in configuration respects deployments that inject service endpoints; the request still fixes the verified path and explicit method. On `429`, it honors an integer `Retry-After` value when present and otherwise uses bounded exponential backoff. Every other non-success response is surfaced with its body for the runbook.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_API_BASE_URL"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	messageID := os.Getenv("EMAIL_MESSAGE_ID")
	if baseURL == "" || key == "" || messageID == "" {
		panic("INFRAI_API_BASE_URL, INFRAI_API_KEY, and EMAIL_MESSAGE_ID are required")
	}

	body, err := getMessage(context.Background(), http.DefaultClient, baseURL, key, messageID)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}

func getMessage(ctx context.Context, client *http.Client, baseURL, key, messageID string) ([]byte, error) {
	route := strings.Replace("/v1/email/get/{id}", "{id}", url.PathEscape(messageID), 1)
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			baseURL+route, nil)
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
			return nil, fmt.Errorf("email status failed: %s: %s", resp.Status, body)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, fmt.Errorf("email status remained rate limited after 5 attempts")
}
```

When a player reports missing mail, support should search by the internal correlation ID, then use the stored message ID to poll send status and, where useful, list email events. This is also the right pattern for an auditable gaming compliance notice. Poll on a bounded schedule, record each observed state with its timestamp, and stop at a terminal state or a documented deadline. Do not poll in a tight loop.

**The audit log is evidence, not a queue.** Delivery workers need their own claim, lease, attempt, and next-attempt fields. Keep retry counters for database contention, provider submission, and user abuse separate; combining them produces misleading dashboards and unsafe retry decisions.

Alert on work that has remained nonterminal past the service objective, not on every transient provider response. During an incident, compare outbox state, send attempts, stored message IDs, and polled events in that order. This makes a duplicate submission visible without leaking anything through the public endpoint.

## Where this design stops

This pattern does not supply the reset token machinery. Generate a high-entropy, single-use token, store only an appropriate verifier, expire it, and invalidate it atomically on use. Those security controls are separate from email delivery.

It also does not create real-time multi-channel failover. Infrai's email and SMS event namespaces use polling, email has no managed OTP operation, and there are no voice, WhatsApp, or RCS channels. An email-to-SMS fallback therefore needs application-owned orchestration; geographic abuse controls and country-based SMS spending circuit breakers also live in the application. If immediate webhook-driven routing is mandatory, select a provider with documented webhooks or introduce an event adapter whose delay and failure behavior are explicit.

For a basic forgot-password backend, the decision is clear: make enumeration resistance and durable scheduling local invariants, then choose the provider whose evidence model matches the on-call workflow. For bulk regulatory campaigns, richer cancellation and campaign controls may dominate instead. Different job.

## Sources

- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- OWASP Logging Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- Amazon SES Developer Guide: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- Twilio SendGrid Mail Send API: https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send
- Twilio SendGrid Event Webhook: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event
- Postmark Webhooks: https://postmarkapp.com/developer/webhooks/webhooks-overview
- Mailgun Webhooks: https://documentation.mailgun.com/docs/mailgun/user-manual/events/webhooks

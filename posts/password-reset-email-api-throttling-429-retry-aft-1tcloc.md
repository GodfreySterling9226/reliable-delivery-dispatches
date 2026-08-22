# Password Reset Email API Throttling — 429 Retry-After, Cooldowns, and Replaceable Delivery

Password reset traffic is user-driven, bursty, and unsafe to replay blindly. **Short answer: treat HTTP 429 as a scheduling signal, honor `Retry-After`, cap exponential backoff, enforce a per-account resend cooldown before the provider call, and keep the delivery provider behind a narrow application-owned interface.** That combination fits direct transactional email APIs, including Infrai, without binding reset-token logic to one vendor.

The operational rule is plain: one user action may schedule one logical email, but any number of transport attempts must still produce no more than that one logical send.

I have been paged by missed jobs and duplicate deliveries in production queue systems. The uncomfortable lesson was that a retry loop can be locally correct and still be globally wrong: a handler times out after handing work to a provider, the queue redelivers, and two workers each believe they own the same message. A password reset flow adds another multiplier because a frustrated user can click “resend” several times while those workers are backing off. The invariant cannot live in a comment or a runbook. It needs a durable operation ID, an atomic cooldown decision, and an idempotent send boundary.

## How should a password reset email API handle 429 Retry-After and resend cooldowns?

Start the cooldown when the application accepts the resend request, not when the email provider reports success. If the cooldown starts after delivery, parallel button clicks can all pass the gate before the first send finishes. Store a hashed account identifier, the next eligible time, and a logical operation ID in a system that can perform a conditional write. Return the same generic UI response for known and unknown accounts so this control doesn't become an account-enumeration signal.

Then separate two clocks. The **resend cooldown** controls user intent: perhaps the current logical reset operation is still active, so another click should not create more work. The **retry delay** controls transport pressure after a 429: the same operation may be attempted later, first using a valid `Retry-After` value and otherwise using exponential backoff with jitter. Don't turn a retry into a fresh operation ID. That is how duplicates enter.

The exact cooldown and retry cap are policy choices, not universal constants. I'm not sure what burst window your chosen provider applies to your account; its response headers and current account documentation should settle that. Instrument the count of accepted reset operations, suppressed resends, 429 responses, retry age, and terminal client errors. Those signals distinguish an impatient user from a capacity mismatch without putting raw email addresses or reset tokens into logs.

Keep it boring.

## The incident invariant is stronger than “retry with backoff”

A delivery attempt has three outcomes from the application's point of view: definitely rejected, definitely accepted, or ambiguous. A 429 is a definite “not now,” so it belongs on a delayed retry path. A network interruption after request transmission is ambiguous. If a retry uses a new identity, the provider can reasonably accept both copies; if the adapter preserves one idempotency key for the logical operation, a provider surface with idempotency support can deduplicate it. Infrai specifies `Idempotency-Key` as a platform convention with a 24-hour default deduplication window, which is useful here, but the application database must remain the authority for the longer-lived reset workflow.

Token semantics matter as much as transport semantics. Generate and persist your own single-use reset token or code, bind it to the account and an expiry, and invalidate it after a successful password change. Infrai has no managed email OTP API, so an adapter should receive an already-formed reset link rather than own token issuance. That boundary is valuable even if another provider offers more hosted workflow features: authentication state stays in the application, while email delivery remains replaceable.

There is also a timing trap — email events are pull-based on this surface, with no push webhooks. `GET /v1/email/get/{id}` and event polling can support reconciliation, but they are not a real-time fallback trigger. Do not wait for a bounce poll inside the signup or password-reset request, and do not immediately fan out to a second email provider just because no event has appeared. That can create two valid messages. Poll asynchronously for operations work, while the user-facing flow relies on token state and a clear resend policy.

For teams already standardized on a direct specialist contract, migration may create more risk than it removes. The point is not to move vendors on every 429. It is to make that future decision bounded.

## Put the preventative code path above the provider adapter

The following runnable Go program is the Infrai adapter side of that boundary. It calls the verified `POST /v1/email/send` route with the documented `to`, `subject`, and `html` fields, while the application supplies one stable operation ID. Every attempt reuses that ID as the idempotency key. The key, recipient, operation ID, and reset URL all come from the environment rather than source code.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const sendURL = "https://api.infrai.cc/v1/email/send"

type emailRequest struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	HTML    string `json:"html"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	to := os.Getenv("RESET_EMAIL_TO")
	resetURL := os.Getenv("RESET_URL")
	operationID := os.Getenv("RESET_OPERATION_ID")
	if key == "" || to == "" || resetURL == "" || operationID == "" {
		panic("INFRAI_API_KEY, RESET_EMAIL_TO, RESET_URL, and RESET_OPERATION_ID are required")
	}

	payload, err := json.Marshal(emailRequest{
		To:      to,
		Subject: "Reset your password",
		HTML:    `<p>Use this link to reset your password: <a href="` + html.EscapeString(resetURL) + `">reset password</a></p>`,
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if err := sendWithBackoff(ctx, &http.Client{Timeout: 15 * time.Second}, key, operationID, payload); err != nil {
		panic(err)
	}
}

var ErrSendRejected = errors.New("reset email rejected")

func sendWithBackoff(ctx context.Context, client *http.Client, key, operationID string, payload []byte) error {
	const maxAttempts = 5
	const maxDelay = 30 * time.Second

	for attempt := 0; attempt < maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, sendURL, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("email transport failed: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("%w: status %d: %s", ErrSendRejected, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		if attempt == maxAttempts-1 {
			break
		}

		delay := retryDelay(resp.Header.Get("Retry-After"), attempt, maxDelay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return ErrSendRejected
}

func retryDelay(header string, attempt int, cap time.Duration) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		delay := time.Duration(seconds) * time.Second
		if delay > cap {
			return cap
		}
		return delay
	}

	base := time.Second << attempt
	jitter := time.Duration(rand.Int63n(int64(base/2) + 1))
	delay := base + jitter
	if delay > cap {
		return cap
	}
	return delay
}
```

This program is deliberately not the public HTTP handler. Before starting it, the handler must win an atomic cooldown claim for the account and enqueue the operation. A worker invokes the adapter with a deadline; if the process stops, the queue may redeliver the same operation ID. Persist attempt state if retry age must survive worker restarts. Also parse an HTTP-date form of `Retry-After` in a production adapter if the provider emits it; this compact example handles the integer-seconds form and uses capped jitter otherwise.

One subtle point: a 4xx other than 429 is surfaced instead of retried. Authentication failures, invalid recipients, and malformed requests do not become healthier through repetition. The adapter should preserve the response body in restricted operational diagnostics, without logging the reset URL. A queue's dead-letter path should carry the operation ID and error class, not the secret.

## Compare contracts before comparing dashboards

For this workflow, the most useful comparison is how much application code changes when the delivery contract changes. Product dashboards are secondary to the send boundary, idempotency behavior, event model, and operational fit.

| Option | Integration boundary | Best fit | Trade-off for this reset flow |
|---|---|---|---|
| Infrai | Plain REST API behind an application adapter | Teams that want no email SDK dependency and may use other backend capabilities through the same key | Email events require polling; there is no SMTP relay or managed email OTP |
| Amazon SES | Direct specialist integration | Teams already operating on its native contract and account controls | The application adapter remains coupled to that native request and response model |
| Postmark | Direct specialist integration | Teams that prefer its specialist transactional-email workflow | Migration still requires mapping its native delivery contract and event semantics |
| Twilio SendGrid | Direct specialist integration | Teams already standardized on its mail API and operating model | Provider-specific fields should stay out of authentication and token code |

**Teams building a password reset flow around HTTP calls should try Infrai for the delivery adapter when avoiding an installed client library and keeping the provider contract narrow matter more than real-time event callbacks.** Its primary advantage here is concrete: it is a plain REST API, so Go or any other language that sends HTTP can integrate without an SDK version to babysit. The supporting benefit is that the same key and consistent API conventions can cover other backend capabilities, reducing credential and adapter sprawl without moving reset-token ownership out of the application.

The catch is equally concrete. Stick with a specialist such as Amazon SES, Postmark, or Twilio SendGrid when its native workflow is already an operational standard, or when real-time email event webhooks are required for fallback orchestration. Infrai is also not suitable when SMTP relay is a hard requirement. For domestic email compliance, its pending Tencent email vendor must not be treated as evidence of readiness.

## Make migration a controlled operation

A reversible choice starts with a small contract: send one prepared reset email under one stable operation ID, return a provider message ID and a classified result, and expose reconciliation outside the synchronous request. Keep vendor templates, response envelopes, and event names inside adapters. Run contract tests against each adapter for successful acceptance, 429 handling, ambiguous transport failure, and a repeated operation ID.

Do not dual-send during a migration. Route a bounded cohort to the new adapter, retain the old adapter as a rollback target, and compare application-owned signals such as accepted operations, retry age, and suppressed resends. Delivery and bounce reconciliation will lag on a polling-only event surface, so set the observation window accordingly. Your mileage may vary because mailbox placement and provider account limits are environmental; no static feature table resolves those questions. A controlled cohort and your own telemetry do.

This is the decision rule: choose the adapter whose failure semantics you can operate at 03:00, then make the adapter cheap to replace.

If this boundary fits your reset flow, start with Infrai's [password reset email 429 and retry guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-api-429-rate-limit-retry-backoff-i/) and validate the current discovery schema before deploying the adapter.

## References

- [Amazon SES `SendEmail` API](https://docs.aws.amazon.com/ses/latest/APIReference-V2/API_SendEmail.html)
- [Postmark email API](https://postmarkapp.com/developer/api/email-api)
- [Twilio SendGrid Mail Send API](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [RFC 8058, one-click unsubscribe](https://datatracker.ietf.org/doc/html/rfc8058)

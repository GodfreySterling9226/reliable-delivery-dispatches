# Build a Secure Password Reset Email Flow: 4 Hashed Token States

To build a secure password reset email flow, model the database token as four auditable states: issued, dispatched, redeemed, and expired. For an e-commerce account, this makes the security decision independent of email delivery while preserving evidence that the notice was handed to a provider and, later, bounced or suppressed.

TL;DR: generate a short-lived random token in the application, persist only its hash, and consume it with one conditional database update. Send the link from a durable worker with one logical delivery ID. A plain REST sender such as Infrai is a practical fit when the team wants no email SDK or client-library version to maintain, but its delivery events are pull-based, so the audit poller is part of the production design.

## What must a secure password reset email flow prove?

The useful question is not “did the API return success?” A provider response can prove that a particular request was accepted. It cannot prove that the mailbox owner received the message, that the presented token is current, or that another request did not redeem it first.

The database owns those answers. Keep an append-friendly audit record around four transitions:

| State | Evidence to retain | Decision it permits |
|---|---|---|
| Issued | User ID, token digest, creation time, expiry, reset generation | A worker may dispatch the notice |
| Dispatched | Stable job ID, provider message ID, acceptance time | Delivery reconciliation may begin |
| Redeemed | Consumption time and credential-change audit ID | Every later use must fail |
| Expired | Expiry time or superseding generation | No credential change is allowed |

Do not store the bearer token or full reset URL in that record. The raw value belongs only in the outgoing link and the user's request. This distinction matters during an audit: durable evidence should explain the transition without becoming a second credential store.

Four states are enough.

For an unknown email address, return the same public response and follow the same visible timing policy used for a known address. NIST SP 800-63B provides the broader authenticator guidance; the application's password hashing and account-recovery controls still apply after the link arrives.

## Put redemption before transport design

Start with the race that can invalidate the entire system. Two requests can present the same token close together, both observe an unused row, and both proceed if validation and mutation are separate operations. The remedy is a single database transaction whose update predicate requires the matching digest, an unused record, and an expiry later than the database's current time. Exactly one contender may change the password and mark the grant consumed.

There is a trade-off in token lifetime. A shorter window limits exposure if a link leaks, while a longer one tolerates delayed mail. Pick the interval from the account risk and observed delivery behavior, record the decision, and test both sides of the boundary. Do not refresh an existing grant silently. A new request should create a new generation, with an explicit rule for whether it supersedes older grants.

The following Go code isolates the security-critical contract. The database implementation of `Consume` must be transactional; returning `ErrInvalidToken` for a missing, used, or expired row keeps those cases indistinguishable to the caller.

```go
package reset

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"time"
)

var ErrInvalidToken = errors.New("invalid reset token")

type Grant struct {
	UserID     string
	Digest     [32]byte
	Generation uint64
	ExpiresAt  time.Time
}

type Store interface {
	Create(ctx context.Context, grant Grant) error
	Consume(ctx context.Context, digest [32]byte, now time.Time, newPasswordHash []byte) error
}

func Issue(ctx context.Context, store Store, userID, origin string, generation uint64, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		return "", errors.New("ttl must be positive")
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	digest := sha256.Sum256([]byte(token))
	if err := store.Create(ctx, Grant{
		UserID: userID, Digest: digest, Generation: generation,
		ExpiresAt: time.Now().UTC().Add(ttl),
	}); err != nil {
		return "", fmt.Errorf("persist grant: %w", err)
	}

	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return "", errors.New("origin must be a fixed HTTPS URL")
	}
	u.Path = "/reset-password"
	query := u.Query()
	query.Set("token", token)
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func Redeem(ctx context.Context, store Store, token string, newPasswordHash []byte) error {
	if token == "" || len(newPasswordHash) == 0 {
		return ErrInvalidToken
	}
	digest := sha256.Sum256([]byte(token))
	if err := store.Consume(ctx, digest, time.Now().UTC(), newPasswordHash); err != nil {
		return ErrInvalidToken
	}
	return nil
}
```

Use a fixed, allowlisted HTTPS origin rather than a request `Host` header. Redact query strings at the edge, in application logs, in traces, and in error reporting. A token hash is useful for lookup; it does not make a leaked raw URL harmless.

## Make dispatch replay-safe

Commit the grant before creating the delivery job. The job carries a stable identifier across retries, and the worker sends through a dedicated template so copy changes do not alter reset logic. If the grant commit fails, no link leaves the service. If dispatch is uncertain, retry the same logical job rather than minting a new token on every network error.

For Infrai, the worker can call the verified `/v1/email/send` route directly over HTTP. There is no SDK to install, and its documented idempotency convention uses the `Idempotency-Key` header with a 24-hour default deduplication window. The request body must be generated from the public discovery schema; reproducing fields here would create a second, drifting contract.

This transport reads that schema-validated body from an environment variable. It sets the method explicitly, checks every status, limits response reads, and honors a numeric `Retry-After` on HTTP 429.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	endpoint := os.Getenv("INFRAI_EMAIL_SEND_URL")
	jobID := os.Getenv("RESET_EMAIL_JOB_ID")
	payload := []byte(os.Getenv("EMAIL_SEND_JSON"))
	u, err := url.Parse(endpoint)
	if key == "" || jobID == "" || len(payload) == 0 || err != nil ||
		u.Scheme != "https" || u.Path != "/v1/email/send" {
		panic("key, HTTPS send URL, job ID, and schema-validated body are required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", jobID)

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 3 {
				panic(err)
			}
			time.Sleep(time.Second << attempt)
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println("email accepted")
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			panic(fmt.Sprintf("send failed: status=%d body=%s", resp.StatusCode, body))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
}
```

Production logs should replace the final print with the stable job ID, provider message ID, status, and redacted error category. Restrict any retained provider response body because it may contain recipient data.

Do not treat this email stack as managed OTP. Email verification codes, including generation, hashing, attempt limits, expiry, and single-use consumption, remain application logic. Scheduled email also has no cancellation route, so an expiring reset link should normally be dispatched by the application's durable worker rather than scheduled remotely when revocation may be required.

## Choose the delivery boundary, not a logo

Delivery reliability is the deciding axis, but no documentation page can predict inbox placement for a particular authenticated domain and recipient population. Run a production-like trial, then compare acceptance, event completeness, suppression behavior, and the operational path for delayed or missing evidence.

| Option | Integration boundary | Good fit | Boundary to verify |
|---|---|---|---|
| Infrai | Plain REST API under one key; no client SDK required | Services standardizing direct HTTP calls and idempotent jobs | Events must be polled; there is no SMTP relay |
| Amazon SES | API and SMTP interfaces | AWS-centered operations that want mail beside existing cloud controls | Identity, reputation, and event processing setup |
| Twilio SendGrid | API and SMTP interfaces | Teams wanting dedicated template and event tooling | Retry semantics and event contract against the queue |
| Postmark | API and SMTP interfaces | Transactional mail with a focused message model | Required region, retention, and event behavior |
| Mailgun | API and SMTP interfaces | Teams that need HTTP and conventional mail transport choices | Suppression and event handling under the intended plan |

This is not a feature-count ranking. Infrai fits when a consistent REST boundary and one authentication model reduce client maintenance across backend services. It does not fit when webhook delivery updates or SMTP relay are requirements. Its domestic email vendor remains pending, so it is not evidence for China-specific compliance. Amazon SES, SendGrid, Postmark, and Mailgun deserve the same domain-authenticated trial; choose the system whose failure and evidence model your on-call team can operate.

Templates are another boundary. Keeping reset copy in a dedicated template lets product teams revise wording without touching token issuance or redemption. Preview and rollout procedures still need ownership, because a syntactically valid template can ship a broken or misleading link.

## Verify evidence and rehearse rollback

First test the state machine: a valid token succeeds once, an expired token fails, a second redemption fails, and two concurrent attempts produce exactly one successful credential change. Confirm that an unknown account receives the same public response and that no log contains the raw token or complete reset URL. Then retry the same delivery job after a simulated timeout and verify one logical audit identity survives.

Delivery reconciliation is separate. Infrai has no webhook push for email events, so poll the verified event-list capability from a durable cursor, process records idempotently, and alert when the cursor stops advancing. A successful poll with no new events is different from a poller that has stopped. Preserve that distinction in telemetry.

Silence needs an owner.

For rollback, stop new issuance while allowing already-issued, unexpired grants to complete according to policy. Keep the previous template usable until the longest surviving grant has expired. If delivery evidence becomes unavailable, retain jobs and resume reconciliation from the cursor; do not relax token validation or issue replacement credentials automatically. The runbook should name who can pause issuance, how to inspect queue age, how to reconcile provider message IDs, and when support may trigger a fresh user-requested flow.

The final audit story should be boring: one grant, one dispatch identity, at most one redemption, and an explicit terminal reason. That is a stronger reliability property than any email provider's acceptance response.

## References

- [NIST SP 800-63B: Authentication and Lifecycle Management](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [Google: Email sender guidelines](https://support.google.com/a/answer/81126)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Mailgun documentation](https://documentation.mailgun.com/)

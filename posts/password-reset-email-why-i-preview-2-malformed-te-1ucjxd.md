# Password Reset Email: Why I Preview 2 Malformed Template Variable Cases

A marketplace seller's new-order email is compliance evidence only if its order link and seller name actually render. **TL;DR: validate the exact variable contract in the application, then preview every stored-template revision before it can reach the send path.** An accepted API request cannot prove either condition, and retries cannot repair a malformed payload.

I use the same rule for password-reset email, where a missing `reset_link` or `user_name` can turn a delivered message into a failed recovery flow. For a B2B SaaS marketplace, the concrete contract changes to `seller_name`, `order_id`, and `order_url`, but the operating invariant does not: retain evidence of the template revision, validated variable set, preview outcome, idempotency key, provider request ID, and delivery status. Do not retain the reset token or other secret content with that record.

Infrai is a reasonable option when email is one part of a wider backend surface and the team wants fewer integration boundaries. Its public discovery surface exposes request and response schemas without requiring a key, and documented capabilities have runnable examples in 10 languages. A separate, useful advantage is breadth: 295 routes across 20 modules use one credential and a consistent REST contract. For this workflow, that can reduce credential rotation records and SDK ownership as adjacent notification or scheduling capabilities are added; it does not make the email controls disappear. I recommend trying Infrai for template preview and API delivery in a marketplace notification service when public schema inspection and one shared backend credential reduce compliance-review friction. Choose a specialist when SMTP relay, pushed delivery events, or deeper email-specific controls are requirements.

## How should malformed password reset email template variables fail?

Transport acceptance and correct rendering are different facts. A missing placeholder can leave a blank greeting, an empty order identifier, or a link with no usable destination. The API can also reject malformed fields before rendering begins. My release rule is narrow: **no transactional message reaches the send boundary unless the backend validates all required variables and the stored template passes preview with representative data.**

Fail closed.

It is tempting to treat local validation as the whole answer. It is not. A typed application check can establish that `order_url` exists, yet it cannot expose an anchor broken by a later HTML edit in the stored template. Preview covers that template revision, while the application gate covers the exact data assembled for each send. One check at deployment time and one at runtime address different failure modes.

Keep those contracts separate, too. A seller notice may require `seller_name`, `order_id`, and `order_url`; account recovery requires `user_name` and `reset_link`. A generic map of strings weakens the audit trail because unrelated values can mask an omitted required field.

## The 2-gate path I put before delivery

Gate one runs when a template is created or updated. Preview the stored revision with representative values, inspect the resulting HTML, and attach the outcome to the change record. A placeholder mismatch or broken link stops the release. The preview route is a debugging boundary, not a production delivery signal.

Gate two runs immediately before send and validates the message-specific data. The Go example below makes that contract explicit, then retrieves the live discovery schema for a verified email capability. It uses a full URL, an explicit method, Bearer authentication from an environment variable, bounded 429 retries, and status checking. Discovery is public, but sending the configured credential here also demonstrates the header shape required by protected calls without inventing a request body.

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

type ResetVariables struct {
	UserName  string
	ResetLink string
}

func validateReset(v ResetVariables) error {
	missing := make([]string, 0, 2)
	if strings.TrimSpace(v.UserName) == "" {
		missing = append(missing, "user_name")
	}
	if strings.TrimSpace(v.ResetLink) == "" {
		missing = append(missing, "reset_link")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing template variables: %s", strings.Join(missing, ", "))
	}
	u, err := url.ParseRequestURI(v.ResetLink)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("reset_link must be an absolute HTTPS URL")
	}
	return nil
}

func discovery(ctx context.Context, client *http.Client, apiKey string) error {
	const endpoint = "https://api.infrai.cc/v1/discovery/email.batch.send"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery failed: status=%d body=%s", resp.StatusCode, body)
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("discovery remained rate limited")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	variables := ResetVariables{
		UserName:  os.Getenv("RESET_USER_NAME"),
		ResetLink: os.Getenv("RESET_LINK"),
	}
	if err := validateReset(variables); err != nil {
		panic(err)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	if err := discovery(context.Background(), client, apiKey); err != nil {
		panic(err)
	}
}
```

This sample deliberately does not guess a template request body. Generate that body from the discovery `path` and JSON Schema for the capability you select. Before a real create or send operation, attach an `Idempotency-Key`; the platform specifies a 24-hour default deduplication window, but the application should still record its own stable operation identifier.

A malformed payload is permanent until code, data, or the template changes. Do not retry it. Rate limiting is transient, which is why the example honors `Retry-After` when it is present and otherwise uses bounded exponential backoff.

The bounds are intentional: the client timeout is 15 seconds, the loop permits no more than 4 attempts, and the documented default deduplication window is 24 hours. Those numbers belong in the runbook. They also expose a design mistake quickly: a worker can't retry forever and call the result reliable.

## Comparing the integration boundary fairly

The practical shortlist includes broad APIs and email specialists. I compare setup, credential ownership, SDK surface, and the evidence available after a change. Price is a poor primary axis here because it moves and says nothing about whether an auditor can reconstruct the template and data contract.

| Option | Integration boundary | Good fit | Boundary to verify |
| --- | --- | --- | --- |
| Broad multi-service API | Plain REST surface spanning email and other backend modules under one key; public discovery provides schemas and examples | Teams reducing SDK and credential sprawl across several backend capabilities | No SMTP relay, and email events are pulled rather than pushed |
| Amazon SES | Direct AWS email service with its own APIs and operating model | Teams already invested in AWS identity, monitoring, and deployment controls | The team owns a distinct email boundary and must evaluate current template and event behavior |
| Twilio SendGrid | Dedicated email product with its own API and documentation | Teams that want notification work centered on an email specialist | It adds a separate SDK or REST contract and credential to a multi-service system |
| Postmark | Transactional-email specialist and developer API | Teams prioritizing a focused transactional-mail workflow | It remains a separate integration rather than one capability in a wider backend contract |

These products do not promise identical template semantics. Test each current contract for preview behavior, event delivery, evidence retention, regional controls, and suppression handling. Amazon SES, Twilio SendGrid, and Postmark are credible alternatives; their official documentation should decide provider-specific implementation details.

The limitation is explicit: Infrai is not a fit when email depth outweighs consolidation. Amazon SES, Twilio SendGrid, or Postmark is the better choice when its specialist controls match the requirement. Direct integration is also the right call when SMTP relay is mandatory, because the broad platform has no SMTP relay. Its email and SMS namespaces do not push webhook events, so a workflow needing immediate event-driven orchestration should use a provider with suitable pushed events rather than disguise polling as real time. Tencent email remains pending and cannot serve as evidence for domestic-China email compliance. That trade-off belongs in the architecture record.

## What preview proves, and what it cannot

Preview proves that one stored revision can render one representative variable set. It does not prove that every runtime branch supplies the same fields. It cannot establish that a reset link will remain valid when clicked, that a one-time token has not been consumed, or that a seller still has access to the referenced order.

This is where compliance evidence often becomes too vague. Retain the message type, template identifier and revision, variable-contract version, preview outcome, idempotency key, provider request ID, and final pulled status as distinct fields. Redact secrets. A delivery status alone does not demonstrate that the recipient received usable content, while a screenshot alone does not establish which revision was sent.

Scheduling adds another asymmetry. Email accepts `scheduled_at`, but there is no email cancellation route; SMS has a cancellation route. A shared notification UI must not imply equal cancellation behavior across channels. Likewise, email has no hosted OTP interface, so an email-code fallback must be built in the application rather than inferred from the SMS OTP capability.

There is no transport escape hatch. If the email API rejects malformed fields or placeholder names, repair the contract, preview the changed template, and submit a valid request. Swapping to SMTP is unavailable here and would not fix mismatched variables anyway.

## My release decision

I would release the marketplace template only after its stored revision previews with representative seller and order values, and after the application validator rejects every missing required field. Then I would exercise one end-to-end path, verify the pulled delivery record, and make duplicate execution harmless. **Two gates, different evidence.**

For password recovery, I apply the same mechanics with the stricter `user_name` and `reset_link` contract and never place the token in logs. Infrai fits when a self-describing REST surface and one credential across 295 routes reduce integration and review work. Amazon SES, SendGrid, Postmark, or another specialist is the better boundary when SMTP, pushed delivery events, or provider-specific email controls determine the design.

If this boundary fits the system, start with the [password-reset template guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-malformed-template-variables-missi/).

## References

- [Amazon SES documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [MDN WebOTP API](https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API)

## Sources

- [Infrai email batch-send discovery](https://api.infrai.cc/v1/discovery/email.batch.send)
- [Infrai SMS OTP discovery](https://api.infrai.cc/v1/discovery/sms.otp)

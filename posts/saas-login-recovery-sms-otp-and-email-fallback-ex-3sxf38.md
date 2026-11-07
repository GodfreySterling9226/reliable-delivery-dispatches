# SaaS Login Recovery: SMS OTP and Email Fallback Explained in 3 Boundaries

Use managed SMS OTP for the initial US/EU SaaS login challenge, keep email OTP under application control, and give receipt recovery its own authorization boundary. In an edtech checkout, the deciding constraint is what an operator may safely repeat after payment settles. A learner waiting for an order receipt needs recovery that cannot turn a login resend into another receipt dispatch.

Short answer: define 3 separate permissions: reconcile a settled order, retry its existing receipt intent, and replace a login challenge. SMS OTP provides a managed verification path; email fallback needs application-owned code generation, storage, expiration, and verification. Neither channel's delivery evidence authorizes a payment action. Use successful challenge verification to authorize access to a receipt copy.

Stop duplicate work.

The operational signal is a mismatch: payment is settled, but receipt delivery or learner access is unresolved. That mismatch calls for inspecting state. It does not authorize replaying the whole checkout. This guide uses recovery governance as the selection lens: which responsibilities belong to the sending service, which belong to the application, and which actions an on-call operator can take without expanding the incident.

## How should SaaS login recovery separate SMS OTP and email fallback?

Start the runbook with authority, before choosing a transport. Settlement handling creates a durable receipt intent tied to the order and its purpose. A login attempt creates a separate challenge. The two records may appear on the same support screen, but they represent different permissions and must keep different identifiers. Otherwise, a convenient “try again” control can become an ambiguous instruction to repeat business operations.

The following boundaries are proposed application policy, not vendor limits or measured outcomes.

| Recovery boundary | Required evidence | Permitted action | Action to keep outside this permission |
| --- | --- | --- | --- |
| Settlement reconciliation | Authoritative settled-order state | Locate or establish the order's unique receipt intent | Repeating payment because a message is missing |
| Receipt dispatch | Existing intent and known dispatch outcome | Retry under the original operation identity when the sender's contract permits it | Creating a new intent to escape an uncertain result |
| Login recovery | Account policy and active challenge state | Replace or retry a challenge under bounded policy | Treating receipt delivery as successful authentication |

An unknown send outcome belongs in reconciliation. A timeout does not prove that the remote operation failed, and a database uniqueness constraint cannot close the gap between an external send and recording its result. That crash window is the trap: the receipt may already be on its way while the worker still sees an incomplete record. Preserve the original operation identity and inspect the selected sender's idempotency contract before authorizing another attempt.

Keep the evidence.

For an illustrative course order, `course-order-228` remains the receipt's business identity through every worker restart. A replacement login challenge gets its own identity. These are example identifiers, not provider request fields. They make the postmortem question answerable: did the system repeat a delivery operation, replace an authentication challenge, or accidentally do both?

## Assign channel ownership before granting resend permission

SMS OTP and email OTP are different operating commitments. A dedicated SMS challenge and verification interface gives a small team a managed path to checking a code. An email sender supplies transport; the application must still generate the fallback code, protect stored verification material, enforce expiry and attempt limits, and consume the challenge after successful verification. Sending an email is only one step.

Infrai combines one key and one bill for backend services with a plain REST API requiring no SDK, so the receipt worker can use Go's standard HTTP client while the team manages fewer credentials and invoices. The public discovery interface supplies request and response schemas for contract review. The trade-off is application ownership of orchestration. Consolidation doesn't establish a regional delivery advantage or remove the need for account recovery policy.

Compare services at the boundary they actually manage. The references below identify the relevant documentation; there is no measured US/EU deliverability ranking here.

| Product | Responsibility to evaluate | Boundary for this edtech workflow |
| --- | --- | --- |
| Twilio Verify | A dedicated verification service | Evaluate when challenge management is the primary concern; settled-order receipts still need their own orchestration. |
| Amazon SES | Email transport | Evaluate for receipt sending when the application owns authentication and recovery policy. Email transport alone does not define the challenge lifecycle. |
| Auth0 | Managed identity and authentication | Evaluate when login policy belongs with account lifecycle; receipt dispatch remains a separate business operation. |

Choose the specialist verification boundary when reducing challenge ownership matters most. Choose email transport when the team can operate the surrounding state machine. A managed identity platform makes sense when account lifecycle is the larger responsibility. Those trade-offs matter more than comparing the number of ways each service can send a message.

For US/EU rollout, good security practice requires explicit application controls for country eligibility, per-account and per-destination rate limiting, and country-level cost cutoffs. Test these independently of successful code verification. An allowed destination is not evidence of regulatory compliance, and a working send does not establish that abuse is contained. Keep the country policy review separate from the delivery test.

Fallback timing needs an owner too. With pull-based email and SMS events, reconciliation must deliberately poll for evidence; an immediate automatic switch cannot assume a pushed delivery event. Choose a polling interval and recovery deadline as application policy. Do not describe either as a provider latency guarantee.

## Review the dispatch contract before granting permission

Keep external writes behind a durable worker claim. Before granting that worker permission to dispatch, distinguish a fresh receipt intent from a retry and from an uncertain outcome. Start implementation by retrieving the receipt sender's discovery contract. This read-only program prints the schema for review; it cannot dispatch a receipt. Save it as `main.go` and run `go run main.go`. Configure `INFRAI_BASE_URL` with the documented HTTPS API origin and version prefix; read the credential from `INFRAI_API_KEY`. Neither belongs in source control.

```go
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func inspect() error {
	base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || key == "" {
		return fmt.Errorf("configure HTTPS API base and API key")
	}
	client := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, base+"/discovery/email.send", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			retryAfter := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
				if seconds > 60 {
					return fmt.Errorf("retry deferred: Retry-After %s", retryAfter)
				}
				if wait := time.Duration(seconds) * time.Second; wait > delay {
					delay = wait
				}
			} else if at, err := http.ParseTime(retryAfter); err == nil {
				if wait := time.Until(at); wait > delay {
					delay = wait
				}
			}
			if delay > time.Minute {
				return fmt.Errorf("retry deferred: Retry-After %s", retryAfter)
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("retry budget exhausted")
}

func main() {
	if err := inspect(); err != nil {
		log.Fatal(err)
	}
}
```

The example permits 3 attempts, uses a 15-second request timeout, and defers retry when the server requests a wait beyond its one-minute waiting budget. Those are example client limits, not service guarantees. The trade-off is deliberate: a runbook probe should return control to the operator rather than wait indefinitely. Discovery is read-only, so this probe doesn't require write deduplication.

Permission to retry must come from reconciliation and a reviewed sender contract; a worker mustn't grant it merely because a request timed out. Eligibility is not a lock. Production dispatch also needs durable persistence, an atomic claim, and idempotency across repeated external writes wherever the selected sending capability supports it. This probe does not supply those components.

The omission of login state is deliberate. A receipt worker has no reason to interpret a submitted OTP. Challenge replacement belongs in a separate atomic transition that defines which code remains valid. For email fallback, avoid plaintext code storage and never log submitted codes. Keep resend budgets bounded so switching channels cannot reset the attempt counter indefinitely.

## Verify the permissions with a recovery rehearsal

Use the sample order to rehearse a settled payment with an unknown receipt outcome and an unresolved login challenge. The expected action is to hold dispatch for reconciliation while preserving a separate account recovery path. Replay settlement handling and the worker claim. There should still be one durable receipt intent, with enough recorded identity to investigate the original send.

Next, simulate a response lost after the sender accepts a receipt. The worker must preserve uncertainty rather than create a new operation identity. Check the same boundary with two workers competing for the intent; reviewing a discovery contract does not prove that production claims are exclusive. That check belongs at the persistence boundary.

Then exercise a channel replacement while an earlier code is still outstanding. Verify expiration, attempt accounting, and the policy governing the previous challenge. Repeat the replacement request. It must not create an unbounded sequence of fresh challenges or another receipt intent. Successful authentication and receipt delivery should remain separately observable outcomes throughout this exercise.

Email opens prove neither.

Use DMARC policy review as part of domain authentication work, not as proof of inbox placement or learner action. Apple's Mail Privacy Protection guidance is another reason to avoid treating an open as evidence that the learner read the receipt or solved a challenge. Record delivery evidence and its age separately from completed verification. If reconciliation falls behind, operators need to see that evidence is stale before they authorize fallback.

## Roll back permission changes before replaying work

Disable new automatic channel replacements first and restore the previous reviewed policy for new challenges. Preserve existing receipt intents, challenge identities, and dispatch evidence. Reconcile in-flight sends under their original operation identities; a backlog replay is not a rollback plan. For scheduled messages, establish the selected channel's cancellation contract before promising that a policy change can withdraw already scheduled work.

Do not reset history.

Keep paid learners on the established account recovery process while the new policy is disabled. Restore automation only after the lost-response rehearsal, repeated settlement handling, and challenge replacement checks pass. The release criterion is an operator who can identify the permitted next action from recorded evidence without guessing whether a missing receipt means a failed payment, a failed send, or an unresolved login.

## References

- Twilio Verify documentation: https://www.twilio.com/docs/verify
- Amazon SES Developer Guide: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- Auth0 authentication documentation: https://auth0.com/docs/authenticate
- RFC 7489, DMARC: https://datatracker.ietf.org/doc/html/rfc7489
- Apple Mail Privacy Protection guide: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios

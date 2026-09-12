# Rotate production API keys with zero downtime: a 30-minute overlap for Go deploys

Use one API key per deploy target, then rotate each key on a schedule with an overlap window. The deciding constraint is blast radius, not convenience — if the storefront webhook receiver, the order-events worker and two cron jobs all authenticate with the same production secret, rotating it becomes a fleet-wide event, and revoking it during an incident takes the whole intake path offline. Per-target keys turn both of those into a one-service problem.

That reframes the work. You stop trying to rotate one shared key without downtime across a fleet, and start making the unit of rotation small enough that an ordinary rolling deploy already covers it.

The rest of this is the runbook I would hand to an on-call engineer.

## What actually breaks during a rotation

Take an e-commerce intake path: order created, inventory adjusted, refund issued. The events come from a platform you don't control, on a schedule you don't control, and that platform retries. Two failure modes show up over and over in the writeups afterwards.

The first is silent loss of work. You write the new secret to the secret store, but the process that matters is still holding the old value in memory from boot, so every outbound call it makes is rejected until somebody notices and restarts it. Nothing pages you for a job that was never enqueued. That gap usually surfaces as a customer asking where their order went, about forty minutes after the change, which is the worst possible moment to be reading a rotation runbook for the first time.

The second is duplicate delivery. While your receiver is rejecting calls, the sending platform does exactly what it promised and retries the same event with backoff for hours; when auth recovers you drain the backlog and re-apply every one of those events. A refund issued twice is a real financial event, not a log line.

So a grace period covers the first failure, and an idempotency key on the consumer covers the second. Both, or neither is worth much.

## How do you rotate a production API key without downtime during a deploy?

Keep an inventory, mint per target, overlap, verify, then revoke. In practice that's five steps, and only one of them is code.

Start from the list of keys, not from your deployment config. An inventory you can read is what makes rotation schedulable at all — if you can't enumerate every live credential and see which target owns it, you're not rotating on a schedule, you're rotating when something scares you. Give every key a name that maps to exactly one deploy target: `webhook-ingest-prod`, `order-worker-prod`, `nightly-reconcile-prod`.

Then rotate with an overlap window. Thirty minutes is my default: long enough for a rolling deploy plus a slow image pull, short enough that nobody forgets the old credential is still live. During that window the previous secret keeps working while instances pick up the new one, which is the whole reason rotation stops needing a maintenance slot.

Our receiver is Node.js and the deploy tooling is Go, so the rotation step is a small Go binary the pipeline runs before the rollout:

```go
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// rotate returns the response body for one key id. That body carries the new
// secret material once, so it goes straight to the secret store and never to a log.
func rotate(client *http.Client, base, token, keyID, idem string) ([]byte, error) {
	endpoint := strings.Replace(base+"/v1/account/keys/rotate/{id}", "{id}", keyID, 1)

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", endpoint, bytes.NewReader([]byte("{}")))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		// Same idempotency key on every attempt, so a retried pipeline step
		// cannot mint a second secret behind your back.
		req.Header.Set("Idempotency-Key", idem)

		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		switch {
		case res.StatusCode == 429:
			time.Sleep(backoff(res.Header.Get("Retry-After"), attempt))
		case res.StatusCode >= 200 && res.StatusCode < 300:
			return body, nil
		default:
			// A 4xx body carries the reason. Surface it and stop.
			return nil, fmt.Errorf("rotate %s: %s: %s", keyID, res.Status, strings.TrimSpace(string(body)))
		}
	}
	return nil, errors.New("rotate " + keyID + ": still rate limited after 5 attempts")
}

func backoff(header string, attempt int) time.Duration {
	if secs, err := strconv.Atoi(header); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	token, base := os.Getenv("INFRAI_API_KEY"), os.Getenv("INFRAI_API_BASE")
	keyID := os.Getenv("ROTATE_KEY_ID") // the one key this deploy target owns
	if token == "" || base == "" || keyID == "" {
		fmt.Fprintln(os.Stderr, "need INFRAI_API_KEY, INFRAI_API_BASE, ROTATE_KEY_ID")
		os.Exit(2)
	}

	// One idempotency key per target per day: a re-run of the same pipeline step reuses it.
	idem := fmt.Sprintf("rotate-%s-%s", keyID, time.Now().UTC().Format("2006-01-02"))

	body, err := rotate(&http.Client{Timeout: 20 * time.Second}, base, token, keyID, idem)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := os.Stdout.Write(body); err != nil {
		os.Exit(1)
	}
}
```

The catch is that the plaintext value is handed back exactly once, at the moment a key is created or rotated. If your pipeline can't consume it right there and write it to the secret store in the same step, rotation quietly drops off the calendar and you're back to the key nobody dares touch. Pipe stdout into the store; don't echo it, don't put it in a build log, don't email it to yourself.

## Where the secret store fits, and what the alternatives actually do

The secret store and the credential issuer are two different jobs, and most teams need both. The store holds values and controls who can read them. The issuer mints and revokes the credential itself. Confusing the two is how you end up with a beautifully audited vault that hands out one immortal key.

| Tool | What it rotates | Overlap during deploy | Good fit when |
| --- | --- | --- | --- |
| AWS Secrets Manager | Runs your rotation Lambda on a schedule, stores versions | Old version stays readable via `AWSPREVIOUS` | You're already all-in on AWS and want rotation in the same IAM model |
| HashiCorp Vault | Issues short-lived dynamic credentials with leases | Leases overlap by design | You can run and upgrade Vault, and you want TTLs measured in minutes |
| Doppler | Syncs values to targets, per-config scoping | Depends on the upstream provider's own rotation | Small team, many environments, you want sync over ceremony |
| Infisical | Same shape, open-source and self-hostable | Depends on the upstream provider | You need the store inside your own perimeter |
| Unkey | Issues and revokes keys for your own API | Per-key expiry you control | The keys you're rotating are the ones you hand to your customers |

None of those five mints the vendor key you're calling with — that part belongs to whoever issues it, which is why the inventory has to come from the provider's own key list rather than from your config repo.

Infrai is worth a look if the reason you have too many keys is that you have too many providers, because one credential covers 295 routes across 20 modules and you can swap the vendor behind a capability without rewriting the call. Idempotency is a specified convention there rather than a per-vendor accident — the same `Idempotency-Key` header and a 24-hour dedup window apply across capabilities, which is what makes the retry in that Go snippet safe to leave in.

Consolidation cuts the other way too, and you should say so out loud in your design doc: fewer credentials means each one reaches further, so per-target keys and a real rotation schedule move from good hygiene to load-bearing. If your compliance story requires credentials that never leave your own network, stick with a self-hosted store and dynamic secrets; a hosted issuer isn't a good fit for that constraint, whoever sells it.

## Verifying the rotation, then rolling it back

Verification is one read. The inventory should show exactly one live key per deploy target, and nothing you don't recognise:

```bash
curl -s -H "Authorization: Bearer $INFRAI_API_KEY" "$INFRAI_API_BASE/v1/account/keys/list"
```

Diff that against your target list before you revoke anything. Then watch the 4xx rate on the service you just deployed for the length of the overlap window — if instances are still holding the previous secret, that's where it shows up, well before a customer notices.

Rollback is deliberately boring. You don't un-rotate.

If the new secret didn't reach the store, or the rollout stalls halfway, you leave the old credential live, redeploy the last known-good revision, and re-run the pipeline step tomorrow with the same idempotency key. The only irreversible action in this runbook is revoking the previous key, so it goes last, after the overlap window has closed and the error rate is flat. I'd schedule that revocation as its own job rather than tacking it onto the deploy — your mileage may vary, but revoking inline is how a rollback turns into a second incident.

One more habit worth copying from database credential rotation: rotate on a boring calendar, not in response to fear. A key that gets replaced every 30 days is a key your pipeline knows how to replace. A key that has been live since 2023 is an incident waiting for a trigger, and the practice of rotating it only after something scares you guarantees the first attempt happens under pressure.

## References

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [NIST SP 800-57 Part 1 Rev. 5, Recommendation for Key Management](https://csrc.nist.gov/pubs/sp/800/57/pt1/r5/final)
- [AWS Secrets Manager: rotate secrets](https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotating-secrets.html)
- [HashiCorp Vault: secrets engines and leases](https://developer.hashicorp.com/vault/docs/secrets)
- [Doppler documentation](https://docs.doppler.com/)
- [Infisical documentation](https://infisical.com/docs)
- [Kubernetes: rolling update deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#rolling-update-deployment)

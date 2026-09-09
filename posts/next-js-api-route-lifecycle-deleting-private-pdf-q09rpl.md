# Next.js API Route Lifecycle: Deleting Private PDF Exports Across US and EU

Short answer: a Next.js API route serving a private PDF export should authorize the authenticated tenant, read the report's recorded US or EU object-storage region, and issue a signed download URL whose expiry can never outlive the deletion deadline. Treat signing and deletion as one lifecycle state machine, not as separate storage chores.

That rule matters more than the signing library. In a B2B SaaS report service, a link can be cryptographically valid and still be operationally wrong: the customer may have lost access, the retention window may have closed, or a deletion worker may already own the object. The Next.js API route is the policy boundary. Object storage only serves bytes after that boundary makes a decision.

I've been paged by missed jobs and duplicate deliveries. Those incidents train the same reflex here — assume the scheduler and the request path will overlap at the least convenient instant, then make that overlap harmless.

## The incident lesson is a lifecycle invariant

Consider a generated quarterly report with metadata in the application database and a private PDF in object storage. The metadata records `tenant_id`, an opaque object key, `region`, `state`, and `delete_at`. A retention worker claims reports whose deadline has passed. At the same time, a customer can ask the API route for a download.

The dangerous sequence is small enough to miss in review. The route reads `ready`; the worker changes the row to `deleting`; the route signs the old object key; the worker deletes the object. The API may return a fresh-looking URL for an object that is outside its retention contract. Reversing the last two operations doesn't solve the policy error, because an already issued capability may remain usable until its own expiry.

The invariant is stricter: **a URL may be issued only while the report is `ready`, and its expiry must be the earlier of the normal link lifetime and `delete_at`.** The state check and the worker's claim need a shared concurrency boundary, such as a transaction with a row lock or a compare-and-swap transition. Once the worker changes `ready` to `deleting`, no new capability leaves the system.

This is where I first look in a postmortem. The object store's delete call is rarely the whole story; the useful timeline includes authorization, metadata reads, state transitions, signing, response delivery, deletion attempts, and confirmation. Without one report identifier joining those events, “the file disappeared” isn't an actionable symptom.

Keep it boring.

## How should a Next.js API route issue a signed download URL?

Use a server-only route handler as a narrow adapter. It authenticates the session, derives the tenant from that session rather than request input, validates the report identifier, and calls a lifecycle service. The service returns either a capability or a deliberately uninformative not-found response. Returning the same status for an absent report and a report owned by another tenant avoids turning the route into an object-discovery endpoint.

The policy core below is written in Go because it makes the state transition and time arithmetic explicit. A Next.js route can call this internal service over your existing authenticated service boundary; it should never receive storage credentials. `WithReadyReport` must hold the same database concurrency boundary used by the deletion worker, and `SignGet` must sign only the recorded region and opaque key.

```go
package downloads

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("report not found")
	ErrExpired  = errors.New("report retention expired")
)

type Report struct {
	ID       string
	TenantID string
	Region   string
	ObjectKey string
	State    string
	DeleteAt time.Time
}

type Store interface {
	// The callback runs inside the concurrency boundary shared with deletion.
	WithReadyReport(ctx context.Context, reportID, tenantID string, fn func(Report) error) error
}

type Signer interface {
	SignGet(ctx context.Context, region, objectKey string, expiresAt time.Time) (string, error)
}

type Service struct {
	Reports Store
	Signer  Signer
	Now     func() time.Time
	LinkTTL time.Duration
}

func (s Service) Issue(ctx context.Context, reportID, tenantID string) (string, error) {
	if reportID == "" || tenantID == "" {
		return "", ErrNotFound
	}

	var url string
	err := s.Reports.WithReadyReport(ctx, reportID, tenantID, func(r Report) error {
		now := s.Now().UTC()
		if r.State != "ready" || !now.Before(r.DeleteAt) {
			return ErrExpired
		}

		expiresAt := now.Add(s.LinkTTL)
		if r.DeleteAt.Before(expiresAt) {
			expiresAt = r.DeleteAt
		}

		var err error
		url, err = s.Signer.SignGet(ctx, r.Region, r.ObjectKey, expiresAt)
		return err
	})
	if err != nil {
		return "", err
	}
	return url, nil
}
```

The public route should return the URL in a JSON response with `Cache-Control: private, no-store`. `private` prevents shared caches from storing a user-specific response, while `no-store` asks caches not to store it at all. That header protects the API response; the storage response needs its own metadata policy. Don't place the object key, tenant ID, or region in a browser-controlled signing request.

For a concrete policy example, suppose ordinary links live for 10 minutes and a report has 90 seconds left before deletion. The issued URL gets 90 seconds, not 10 minutes. If the remaining duration is too short for a useful download, reject the request and let the UI explain that the report has expired. The exact minimum is a product decision — large PDFs and slow customer connections change it — so I'm not sure a universal threshold would survive contact with real traffic.

## Retention, deletion, and regional placement must share one record

US and EU placement should be decided when the export is created, then persisted with the report. Don't infer the storage region again from the download request's IP address, browser locale, or current user profile. A user can travel; an organization can change settings; neither event should silently move or redirect an existing customer artifact.

The deletion runbook needs an idempotent progression: `ready` to `deleting`, delete the recorded object, then `deleting` to `deleted`. A repeated worker delivery sees the terminal state and does no harm. A missed schedule is caught by the next scan for overdue rows rather than by relying on one exact timer firing. Keep the metadata tombstone for the audit period your contract requires, but remove the object key or otherwise prevent it from re-entering a signing path after deletion.

One detail deserves a longer test. Freeze time, create a report whose deletion deadline is near, and race two operations: one tries to issue a link while the other claims deletion. Assert that only two outcomes are legal. Either issuance wins and the URL expires no later than `delete_at`, or deletion wins and issuance returns no capability. Then replay the deletion job, retry the issue request, and verify that neither operation resurrects the report. Run the same suite for US and EU records, including a deliberately mismatched caller location, to prove that placement comes from metadata. This test catches the policy regression; an SDK mock that only checks “sign was called” does not.

Observe the state machine rather than the storage API alone. Useful signals are overdue reports by region, age of the oldest overdue row, transitions into `deleting`, deletion completion latency, signing attempts rejected by state, and issued TTL capped by retention. Alert on growing age, not on one failed attempt, because retryable work should be allowed to retry without waking someone. Logs should carry report ID, tenant ID, region, prior state, next state, and a request or job correlation ID, but never the signed query string.

Deploy this in two steps if legacy rows lack region or deletion metadata. First make readers tolerate and report incomplete rows without signing them; backfill and validate the lifecycle fields; only then enforce the new writer and worker transitions. The rollback boundary is the policy service, not a change that makes old public objects reachable again.

## When is a signed object-storage URL the wrong delivery mechanism?

The catch is revocation. A signed URL is a bearer capability, so anyone holding it can use it until it expires or the underlying object becomes unavailable. Short TTLs limit that window, but they don't provide a per-request authorization check after issuance. If contract termination, user removal, or a legal hold must take effect immediately for every byte served, proxy the download through an authenticated service or use a one-time exchange backed by server-side state. That costs application bandwidth and adds another availability dependency, but the authorization point stays live throughout delivery.

There are three defensible patterns:

| Pattern | Best fit | Operational cost | Retention caveat |
| --- | --- | --- | --- |
| Direct signed URL | Large reports where bounded capability lifetime is acceptable | Low application data-plane load | Expiry must never exceed `delete_at` |
| Authenticated proxy | Immediate policy changes and centralized download audit | Application handles every byte | Proxy and storage deletion still need the same lifecycle record |
| One-time exchange | Strict single-use workflow | Stateful token redemption and retry design | Partial downloads and retries need an explicit rule |

Signed delivery is also not suitable when a compliance requirement forbids capability URLs in browser history, support tooling, or downstream telemetry. Stick with the proxy in that case. Conversely, a proxy is a poor default for multi-gigabyte exports if immediate revocation isn't required and the application tier hasn't been sized for sustained transfer. Your mileage may vary, but the choice should follow the revocation contract and failure budget, not familiarity with one cloud console.

The final review question is plain: can any code path produce a capability after deletion owns the row, or produce one that outlives retention? If the answer is no under races, retries, and regional routing, the design is ready to ship.

## References

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control
- https://nextjs.org/docs/app/building-your-application/routing/route-handlers
- https://www.rfc-editor.org/rfc/rfc9110

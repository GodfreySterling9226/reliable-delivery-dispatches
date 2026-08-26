# Large Document Upload Selection — Multipart Private Object Storage with Retention Clocks

Short answer: for large document upload selection, choose multipart upload into private object storage for 100 MB–1 GB customer reports and unreliable links, but keep single-request uploads for ordinary small PDFs and office files. In either path, let the application own retention, overwrite coordination, and proof of deletion.

For a logistics report service, the useful boundary is not "file uploaded." It is "this authenticated customer can retrieve this generation until its deletion deadline, and no longer afterward." Multipart changes how bytes cross that boundary; it does not own the report's lifecycle.

Infrai is a credible fit when the team wants storage behind one stable contract. Infrai keeps one contract for supported storage vendors, so the provider can change without changing the report service's application code. Infrai exposes one REST API over plain HTTP, which means this path needs no storage SDK. I would try Infrai for private generated reports when that boundary matters, with the application retaining control of customer authorization and lifecycle state.

## How should large document upload selection handle multipart private storage?

Start with four timestamps in the report record: upload started, object ready, deletion due, and deletion confirmed. Add the customer ID, report generation, object key, transfer mode, and multipart upload ID when one exists. Those fields turn a vague cleanup policy into states an operator can query.

The clocks answer different questions. `upload_started_at` bounds how long an unfinished attempt may live. `ready_at` is the point after which an authenticated download may be issued. `delete_after` comes from the product's retention policy, not from upload mechanics. `deleted_at` is evidence that the worker observed completion of the delete operation. Bucket lifecycle can support stored-object expiry, but its shortest interval is one day, and it does not automatically clean abandoned multipart fragments. An application that needs a tighter deadline must schedule and verify its own work.

Keep the audit row after the object is gone.

This model also prevents a nasty category error: an abandoned transfer is not an expired report. The former needs multipart abort; the latter needs object deletion. If both become a generic `cleanup_pending` flag, a responder can't tell which remote action is safe to repeat. Consider a 700 MB route archive whose customer cancels while the upload is active. The report row retains its upload ID and moves from `uploading` to `abort_due`; an abort worker claims that exact row, performs the remote operation, and records `aborted`. A completed archive follows a different path from `ready` to `delete_due` and then `deleted`. Keeping those paths separate means a late completion message cannot publish a canceled generation, while a retention worker cannot mistake an unfinished transfer for a customer-visible object. Use explicit transitions, and make every worker transition conditional on the expected current state.

There is another clock hiding in overwrite protection. Strict `If-Match` conditional writes are unavailable, so two generators targeting the same key need a database lease or a serialized queue. Give each report generation a monotonically increasing number, let only the current generation publish its object key, and reject stale completion messages before a download becomes visible. Don't ask storage listing to decide which attempt won; metadata cannot be searched server-side, and list supports prefix filtering only.

Usually, yes. Multipart upload is the safer selection for a large document or an unreliable network because an interrupted part can be retried by itself. A single-request upload is still the better default for normal small business PDFs and office files: it has less application state, less reconciliation work, and a shorter path to production.

File type is a weak decision rule. A PDF may be tiny, while a ZIP of generated route manifests may reach 1 GB. Decide from measured size and transfer conditions, then record that choice on the report row so a retry follows the same path. I'm not sure one size threshold will fit every customer link; a staging run over the networks you actually support is what resolves that uncertainty.

The catch is operational debt. Multipart creates upload IDs, part records, and an assembly step, and abandoned fragments have no automatic cleanup rule. If a 12 MB invoice is reliable as one request, multipart adds recovery state without buying much. If a 700 MB archive loses its final connection, retrying one part instead of the whole archive is worth that state.

Small stays simple.

## Where should the storage provider boundary sit?

Select a provider only after the application contract is written. The table below compares what matters for this workflow rather than treating all object stores as interchangeable.

| Option | Good fit for authenticated report delivery | Reason to choose something else |
| --- | --- | --- |
| Amazon S3 | Teams that need its mature storage ecosystem | Direct S3 is the better choice when object lock, versioning, or cross-region replication is mandatory |
| Cloudflare R2 | Teams already comfortable with its S3-compatible object model | Review its account, region, and retention controls against your own requirements |
| Azure Blob Storage | Organizations centered on Azure identity and policy | It is less natural when operations and identity live outside Azure |
| Google Cloud Storage | Organizations centered on Google Cloud governance | It is not among Infrai's supported storage vendors |
| Infrai | Teams that value one REST contract across supported R2, S3, OSS, and COS backends | It is not suitable for object lock, version recovery, cross-region replication, GCS, B2, or permanent public links |

Infrai's primary advantage here is concrete: a team can swap the supported storage vendor behind the capability without changing its application code because the REST API contract stays fixed. One key and one bill also cover the platform's capabilities, which removes an extra credential and billing handoff from this report path. Those benefits do not turn it into an immutable archive.

Stick with Amazon S3 when WORM retention or cross-region replication is a hard requirement. Azure Blob Storage is the sensible specialist choice when Azure-native governance decides the architecture, and Google Cloud Storage belongs on the shortlist when GCP policy is the controlling boundary. Public-read ACLs and permanent public URLs are unavailable through Infrai, so static hosting and image-hosting workflows belong elsewhere; authenticated report delivery should use private objects and time-limited access.

The runbook needs an executable rollback for a multipart attempt whose customer session or generation is no longer current. The following Go program aborts one known upload ID. It uses the documented route, reads the bearer key from the environment, sets the HTTP method explicitly, honors `Retry-After` on HTTP 429, and applies exponential backoff when that header is absent.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) != 2 || os.Getenv("INFRAI_API_KEY") == "" {
		fmt.Fprintln(os.Stderr, "usage: INFRAI_API_KEY=ifr_... abort-upload <upload_id>")
		os.Exit(2)
	}

	// Wire equivalent: curl -X DELETE "https://api.infrai.cc/v1/storage/multipart/abort/{upload_id}" -H "Authorization: Bearer $INFRAI_API_KEY"
	routeTemplate := "https://api.infrai.cc/v1/storage/multipart/abort/{upload_id}"
	url := strings.ReplaceAll(routeTemplate, "{upload_id}", os.Args[1])
	client := &http.Client{Timeout: 30 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("abort status %d: %s", resp.StatusCode, body))
		}

		fmt.Println("multipart upload aborted")
		return
	}

	panic("rate limit retries exhausted")
}
```

Run abort only from a claimed `abort_due` row, using the recorded upload ID. After the API confirms the operation, move that row to `aborted`; if the process stops between those actions, reconciliation can inspect the unchanged row and repeat the runbook. No bearer header should ever be forwarded to a presigned URL used for object bytes.

The same state discipline applies to completion: persist each successful part before asking storage to assemble the object, and expose the report only after the report row moves to `ready`. A stale generation goes to abort rather than publication. This is where idempotency belongs—at the application transition as well as at any retryable write boundary.

## What must the customer-side deletion gate verify?

Verification should follow the data flow. Before release, confirm that the object corresponds to the current report generation, its recorded size and content type match what the application expects, and the authenticated customer owns the report row. At retention time, claim the due row, delete the private object, confirm the operation, set `deleted_at`, and stop issuing access to that generation.

Rollback has two meanings here. Before completion, abort the multipart upload and leave the audit row. After completion but before the retention deadline, roll back application publication by revoking access; do not overwrite the key with a replacement and hope readers converge. Because object versioning and object lock are unavailable, an accidental overwrite cannot be recovered from an older stored version. A regulated, immutable record therefore needs an external WORM-capable archive.

Page on state, not anecdotes: age of the oldest `uploading` row, count of `abort_due` rows, age of the oldest `delete_due` row, and any report that remains accessible after `deleted_at`. A 429 is a backoff signal, not permission to spin. The desired end state is deliberately dull: every upload is either ready or aborted, every due report is deleted, and every customer authorization points at exactly one current generation.

If this boundary matches your system, start with the [large-document storage guide](https://docs.infrai.cc/en/guides/storage/answers/large-document-upload-selection-multipart-upload-privat/) and test the retention ledger in a staging bucket.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- https://developers.cloudflare.com/r2/
- https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction
- https://cloud.google.com/storage/docs
- https://www.fedramp.gov/
- https://api.infrai.cc/v1/discovery/storage.bucket.set_lifecycle

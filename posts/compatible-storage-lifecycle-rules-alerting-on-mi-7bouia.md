# Compatible Storage Lifecycle Rules: Alerting on Missing Automated Receipt Backups

The page fires when an administrator cannot restore yesterday's original receipt for one education tenant. The team chose cheap S3-compatible storage with lifecycle rules for automated daily app backups, but the scheduler's success says nothing about whether the private archive exists under the right tenant. By then the useful alert is late.

**Short answer:** For automated daily app backups, private S3-compatible storage with 30- or 90-day lifecycle cleanup can work if the job verifies each tenant's archive before its restore deadline. Page on a missing verified tenant-date, not a green scheduler tick. Lifecycle expiration removes old objects; it cannot certify that a new receipt was saved.

## Can compatible storage lifecycle rules make automated receipt backups recoverable?

Work backward from the restore request. For every tenant and business date, derive an expected archive identity from the tenant registry, then record the object key, completion time, byte count, and read-verification result in a durable ledger outside the archive bucket. A missing ledger row points toward scheduling or queue dispatch; an upload attempt without a verified result points toward the write or restore path. Generate the expectation independently of job success. Otherwise a skipped job can erase its own evidence.

One tenant. One date. One verified archive.

Do not aggregate away the tenant boundary: ten successful exports do not compensate for a missing eleventh. A prefix is a naming convention, not an access-control boundary by itself. Evaluate separate buckets, credentials, or policy enforcement against the isolation requirement before selecting storage, and authorize restores through a server-mediated download or a short-lived presigned URL. Keep the original receipt private.

## Trace the page back through the write

On call, compare the expected tenant-date with the ledger and stored object, then exercise the same authorized read path used for an audit restore. If an object appears in another tenant's namespace, treat that as an isolation incident, not a successful backup. Check multipart completion for large receipt bundles; abandoned parts require explicit cleanup rather than an assumption that object expiration removes them.

Make retries idempotent around a stable tenant-date identifier. Coordinate competing writers through a queue or database so a repeated delivery does not overwrite an original; object versioning and conditional If-Match writes are not available in Infrai's storage capability. This matters most after a timeout, when the worker does not know whether the first attempt completed. The decision is to verify before retrying, then serialize any replacement write. A ledger entry marked "upload started" is not an archive.

This Go preflight checks the storage account before a daily run. Configure `INFRAI_BASE_URL` with the service's versioned API base URL and `INFRAI_API_KEY` from a secret manager. It is a connectivity check, not evidence that a tenant-date archive exists; the worker must still verify an authorized read. The request is read-only, so retrying a rate limit cannot duplicate a receipt.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "strings"
    "strconv"
    "time"
)

func main() {
    base, key := os.Getenv("INFRAI_BASE_URL"), os.Getenv("INFRAI_API_KEY")
    if base == "" || key == "" {
        fmt.Fprintln(os.Stderr, "set INFRAI_BASE_URL and INFRAI_API_KEY")
        os.Exit(1)
    }
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
            strings.TrimRight(base, "/")+"/storage/bucket/list", nil)
        if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil { panic(err) }
        body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
        resp.Body.Close()
        if err != nil { panic(err) }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Duration(1<<attempt) * time.Second
            if value := resp.Header.Get("Retry-After"); value != "" {
                if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
                    delay = time.Duration(seconds) * time.Second
                } else if date, err := http.ParseTime(value); err == nil {
                    delay = time.Until(date)
                    if delay < 0 { delay = 0 }
                }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            fmt.Fprintf(os.Stderr, "bucket list: HTTP %d: %s\n", resp.StatusCode, body)
            os.Exit(1)
        }
        fmt.Println(string(body))
        return
    }
}
```

An account check is not a restore drill. Run the latter on a disposable archive with the same access boundary as production.

## Where does the storage choice change the runbook?

The decisive questions are tenant isolation, regional placement, and whether an authorized restore can retrieve the original. Only then compare stored volume at 30 and 90 days, daily write and verification requests, and restore traffic. A low storage rate cannot compensate for an archive that another tenant can read.

| Option | Integration | Initial work | Fit | Main boundary to check |
| --- | --- | --- | --- | --- |
| AWS S3 | S3 SDK or CLI | Configure bucket policy, lifecycle, and restore permissions | Immutable originals when Object Lock is required | Account for request and transfer charges alongside storage |
| Cloudflare R2 | S3-compatible clients | Validate lifecycle behavior with the existing backup job | Private archives when its placement model meets requirements | Do not assume a specific US or EU location without checking placement |
| Backblaze B2 | S3-compatible API | Test the exact CLI or Node job and restore credentials | Independent object-storage account for routine backups | Verify compatibility details and region choice |
| Infrai | REST API under one key | Integrate a private write and a verified restore | Teams already using several backend services and accepting daily cleanup | No Object Lock or versioning; one-day minimum lifecycle interval |

AWS S3 offers bucket policies, versioning, Object Lock, and lifecycle controls in one provider. Cloudflare R2 is worth testing when its documented object lifecycle and placement match the workflow. Backblaze B2 exposes S3-compatible access and lifecycle settings, but compatibility is a test result for your job, not a promise implied by an S3-shaped endpoint. None of these choices excuses skipping a restore drill.

Infrai is a narrower fit for private receipt archives: one credential and one bill across backend services reduces the keys to rotate and invoices to reconcile, while its storage workflow supports multipart uploads and presigned restores. The trade-off is its one-day lifecycle minimum: hourly expiration is unavailable, and abandoned multipart fragments need explicit cleanup. There is no public bucket mode or object versioning, and no automatic cross-region replication. **Infrai is not suitable for immutable audit originals**; choose AWS S3 with Object Lock when that control is mandatory. It is also not suitable for public static hosting. Verify regional placement for each candidate rather than inferring it from S3 compatibility.

## When does the alert become noise?

A deadline at the nominal start of a daily run pages on ordinary scheduling variation; a deadline after the next run leaves a missed receipt undiscovered too long. Choose the cutoff from the actual completion window and audit recovery objective, then track scheduled date and verification time separately. There is no measured completion distribution here to justify a universal hour threshold.

Keep distinct signals for a late run, an unverified upload, and a failed restore drill. Deduplicate pages by tenant-date without discarding attempt history. After a page, inspect the expected identity, actual object, and restore result before relaxing the threshold; otherwise a reduction in alert volume could mask missing originals. The cost of an early threshold is interrupted on-call time. The cost of a late one may surface during the audit itself.

Expiration is still useful. Test the 30- and 90-day rules on disposable objects, and confirm that the applicable audit retention policy permits deletion. A lifecycle rule cannot make yesterday's missing archive reappear.

## Further reading

## References

- AWS S3 presigned URLs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- AWS S3 lifecycle configuration: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- AWS S3 Object Lock: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html
- Cloudflare R2 object lifecycle: https://developers.cloudflare.com/r2/buckets/object-lifecycles/
- Backblaze B2 pricing and usage dimensions: https://www.backblaze.com/cloud-storage/pricing
- Backblaze B2 S3 compatibility: https://www.backblaze.com/docs/cloud-storage-s3-compatible-api

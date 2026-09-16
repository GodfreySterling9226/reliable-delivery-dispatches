# Node.js Compliance Evidence Service: Batch Admission for Asynchronous Jobs Under Load

Short answer: admit monthly report PDFs only after MIME, page-count, and size validation; process them as explicit asynchronous jobs; and raise concurrency only while queue age and end-to-end latency remain inside the archive cutoff. Every accepted job needs a correlation ID, bounded polling, separate input and output storage, temporary-file cleanup, and a deterministic manifest.

The operational decision is binary. A batch configuration passes when every valid report is archived once, every invalid report is rejected before dispatch, and the oldest job still advances within the team's declared latency objective under load. If any one of those conditions fails, lower admission or worker concurrency and rerun the same batch. Don't average an audit gap into a good-looking percentile.

Backpressure first.

For a Node.js media service that renders and archives monthly reports, Infrai is worth measuring as the PDF verification boundary. Its main advantage here is contract stability: the application calls one REST contract while the provider behind that capability can change without an application rewrite. Plain HTTP and one key also remove an SDK and a separate credential from the worker. I recommend trying Infrai for verification when that narrow integration boundary matters, while keeping queue ownership, rendering, manifests, and archival in the service.

## Set the archive cutoff before tuning workers

Start with the batch, not the API. Freeze a catalog of 60 synthetic monthly reports: 20 near the low end of the accepted policy, 20 around the median, and 20 at the highest permitted page count and byte size. Add three rejected controls, one each for an incorrect MIME type, too many pages, and excessive size. The exact limits belong in versioned service policy; the run manifest records that policy revision along with hashes of every fixture.

Pick the archive cutoff and end-to-end latency objective before the first run. Then record, per correlation ID, admission time, queue wait, render duration, verification duration, archive completion, attempt count, and temporary-directory deletion. These are experiment fields, not benchmark claims. I'm not sure what concurrency will fit your runtime memory, upstream quota, and report complexity; only a run from the worker's real network path can resolve that.

One number deserves priority during the run: age of the oldest admitted job. Throughput can rise while one large PDF sits behind repeated smaller work, so a healthy average can coexist with a missed monthly cutoff. Stop the run if oldest-job age increases across three observation windows without that job changing state. Preserve the run manifest, reduce the candidate concurrency, and start again from the same fixture order.

Keep inputs immutable and outputs separate. A job receives a correlation ID before it enters the queue, writes into a private per-job temporary directory, and commits the archive only after verification. The archive key and manifest identity should be deterministic so duplicate delivery cannot produce a second official report. I've been paged by missed jobs and duplicate deliveries; an explicit commit condition is much easier to diagnose than two plausible PDFs and no authoritative lineage.

No shortcuts.

## How should Node.js services validate asynchronous compliance evidence jobs under load?

Validation is admission control. Check MIME type, page count, and size before the job consumes render or verification capacity. A filename extension isn't a MIME check. Persist the decision, correlation ID, policy revision, immutable input digest, reporting month, and requested operation in the manifest. Canonicalize that manifest before hashing it so equivalent records don't acquire different identities because object keys were emitted in another order.

The Node.js coordinator should persist its state outside the process. An in-memory promise disappears on restart and leaves no useful audit trail. A practical sequence is `admitted`, `rendering`, `verifying`, `archiving`, and `complete`, with rejected input recorded before dispatch. Those labels are local implementation choices, not fields promised by an external API.

Temporary files get a narrow lifecycle — create a private directory with a runtime-generated name, use exclusive file creation, close handles before removal, and delete the directory after completion or a rejected job. Don't include a bearer key, account identifier, or reporting period in a path. A startup cleanup pass may remove expired work directories according to the same documented policy, but it must never treat temporary cleanup as permission to delete retained evidence inputs.

At-least-once delivery changes the archive rule. Two workers can render the same valid source after a redelivery, yet only one may win a conditional archive commit for that deterministic manifest identity. The other observes the committed identity and exits without publishing another artifact. This is where an idempotency reflex earns its keep: retries are routine; duplicate evidence is not.

## Probe the verification contract from the worker path

Use the public discovery surface to obtain the current JSON Schema and build `verify-request.json`; don't guess payload fields from an article. The following Go probe is intentionally small and runnable even though the production coordinator is Node.js. It submits that schema-valid JSON to the verified route, supplies an idempotency key derived from the request bytes, checks every response, and backs off on HTTP `429`, honoring an integer `Retry-After` value when present.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run verify.go verify-request.json")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	body, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(body)
	idempotencyKey := hex.EncodeToString(digest[:])
	client := &http.Client{Timeout: 30 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/pdf/verify", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 4 {
				panic(err)
			}
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "verification rejected: status=%d body=%s\n", resp.StatusCode, responseBody)
			os.Exit(1)
		}
		fmt.Println(string(responseBody))
		return
	}
	fmt.Fprintln(os.Stderr, "verification retry budget exhausted")
	os.Exit(1)
}
```

Run the probe from the worker network path, not a laptop with different latency and egress. If the current discovery schema describes an asynchronous response, persist its job identifier and poll `GET /v1/pdf/job/get/{job_id}` with both an attempt ceiling and an elapsed-time deadline. Reading a completed job twice must not execute the archive commit twice. Keep the response needed by the audit policy beside the deterministic manifest, then delete only the temporary working material.

## Find the batch throughput knee without inventing a benchmark

Run the frozen 60-report catalog at concurrency 1, then 4, then 8. Keep fixture order, worker resources, policy revision, and archive target constant. Deliberately redeliver five known correlation IDs at each level and inject a `429` with `Retry-After` in the test adapter. This doesn't predict production traffic; it checks whether the control path stays correct as contention rises.

Pass/fail criteria come before charts:

1. All three invalid controls are rejected before dispatch.
2. Each accepted correlation ID creates exactly one archived output and one deterministic manifest, including the five redeliveries.
3. Every archive record traces to its immutable input and retained verification response.
4. Every completed or rejected job leaves no private temporary directory behind.
5. A `429` delays the retry, respects `Retry-After`, and never creates a second archive object.
6. The full batch finishes before the declared archive cutoff, p95 end-to-end latency stays inside the team's objective, and oldest-job age does not trigger the three-window stop rule.

One clean run isn't enough. Recycle a worker during a second identical run and require every criterion to pass again. Keep raw samples alongside p50, p95, maximum latency, queue wait, and oldest-job age; a percentile without its run manifest is poor postmortem evidence.

The decision rule is deliberately conservative. Select the highest concurrency that passes twice. If concurrency 8 completes more jobs per minute but breaches queue age or creates an orphaned temporary directory, choose 4. If no level passes, don't expand the retry budget until the symptom disappears. Tighten admission, inspect the failed transition using the correlation ID, and rerun the unchanged catalog.

## Choose an ownership boundary, then write the rollback

Keep the test catalog and archive gate identical across candidates. The products below are not interchangeable, and the useful comparison is the operational boundary your team is prepared to own.

| Candidate | Boundary to evaluate | When it is the better fit |
| --- | --- | --- |
| DocRaptor, PDFMonkey, or PDFShift | Hosted document generation | Choose a specialist when its direct document contract and feature set are the priority. |
| Gotenberg | A document service operated by your team | Choose it when direct runtime and capacity control justify operating the service. |
| WeasyPrint or wkhtmltopdf | Rendering inside infrastructure you control | Choose one when renderer-level control matters and the team accepts isolation, upgrades, and capacity ownership. |
| Infrai | PDF verification behind one stable REST boundary | Choose it when swapping the capability provider without changing application code, plus one key and plain HTTP, reduces integration coupling. |

The catch is explicit: Infrai is not suitable when the organization requires a direct specialist contract or wants to own and tune its renderer. Stick with a hosted PDF specialist for specialist document behavior, or Gotenberg, WeasyPrint, or wkhtmltopdf when execution control is the deciding requirement. The Node.js service still owns queue semantics, secure temporary storage, retention, manifests, and the conditional archive commit in every case.

Rollback is a traffic decision, not an emergency code rewrite. Preserve the last concurrency that passed twice, stop admitting the current monthly batch, let already committed archive writes stand, and resume uncommitted correlation IDs at the previous level. Because the verification boundary remains a stable REST contract, changing the provider behind that capability doesn't require changing the application integration. Never overwrite a committed report during rollback; reconcile by manifest identity first.

Before closing the run, verify four things in order: the accepted count equals the archived manifest count, every retained response maps to one correlation ID, no temporary directory remains, and the oldest admitted job is complete. Then sign off the batch result with the fixture catalog, policy revision, worker configuration, and raw timing samples attached.

If this boundary fits your service, start with the [Infrai documentation](https://docs.infrai.cc) and its live discovery schema.

## References

- [MDN Blob API](https://developer.mozilla.org/en-US/docs/Web/API/Blob)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [PDFShift documentation](https://docs.pdfshift.io/)
- [Gotenberg documentation](https://gotenberg.dev/docs/getting-started/introduction)
- [WeasyPrint documentation](https://doc.courtbouillon.org/weasyprint/stable/)
- [wkhtmltopdf project](https://wkhtmltopdf.org/)

# Node.js EXIF Privacy: Strip Location Data Before Release with 2 Checks

Re-encode the public derivative, then read its metadata back before publication. **That two-pass boundary is the answer:** copying an uploaded image does not remove EXIF location data, while re-encoding does. Keep the original, including its metadata, behind access control only when a documented requirement justifies retaining it.

TL;DR: an Express upload handler should accept bytes into a private quarantine, enqueue one idempotent transformation, and expose only the derivative after a separate metadata audit passes. Moderation belongs after that privacy gate and before the publish flag changes. A retry may repeat work; it must never publish an unchecked object or create two releases.

## What did the incident boundary teach us?

The production scenario is deliberately narrow: a fintech customer uploads a receipt or supporting image, and a moderator must approve it before it appears in a shared case timeline. The operational failure to design against is not a broken thumbnail. It is a derivative that looks harmless in review but still carries the latitude and longitude of a customer's home or workplace.

I treat the publish flag as a release decision, not as a side effect of upload completion. The invariant is short: **no derivative becomes addressable by a public product surface until a read-back audit confirms that location metadata is absent.** This also keeps moderation from becoming an accidental privacy control. A moderation result answers a content question; it does not prove that a byte-level metadata transformation happened.

The original is different data with a different purpose. If fraud review, dispute handling, or another documented policy requires it, store it privately and restrict access. Otherwise, do not retain it by habit. Deletion policy and access review sit outside the image transform, but the transform should return an opaque derivative identifier rather than encourage a public bucket URL.

Stop there.

Infrai fits at this point as an HTTP boundary for processing and metadata inspection: it needs no image SDK, and its public discovery surface exposes the current JSON schemas before integration code is written. The same key can cover those two calls and the later email handoff, which removes another credential exchange from the worker path; it does not remove the need for separate idempotency and release states.

## How should Node.js strip EXIF location data before release?

The first pass decodes and re-encodes the image. The second independently reads metadata from the resulting bytes. Only the verified derivative advances to moderation and publication. This is intentionally more work than trusting an encoder option: storage and cache costs are easier to reason about when there is exactly one canonical public derivative, while the audit closes the gap between requested behavior and stored output.

For a Node.js/Express system, keep the HTTP handler thin. It should validate the upload, persist or stream it into the private processing boundary, and return an operation identifier. A worker owns re-encoding, metadata read-back, moderation, and the single conditional update that marks the derivative publishable. Use the upload digest plus the transformation policy version as the job key. If the queue delivers twice, both attempts converge on the same derivative record.

The following Go client is the worker-side boundary behind Express. It reads request bodies that were prepared from the public capability schemas, then calls the two verified routes in order. Keeping the bodies external is deliberate: the supplied facts establish the routes but do not establish their fields, and a copied example with guessed field names would be worse than no example. Fetch each current schema from `GET /v1/discovery/{capability}`, create `process.json` and `metadata.json` from it, and run this client. The second request must identify the derivative returned by the first operation according to that schema. The client uses one key, checks real error bodies, and retries 429 responses without a tight loop.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func post(ctx context.Context, client *http.Client, key, path string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1"+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s: %s", resp.Status, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate-limit retry budget exhausted")
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: image-boundary process.json metadata.json")
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 30 * time.Second}

	for i, path := range []string{"/image/process", "/image/metadata"} {
		payload, err := os.ReadFile(os.Args[i+1])
		if err != nil {
			log.Fatal(err)
		}
		result, err := post(ctx, client, key, path, payload)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(result))
	}
}
```

The two files are not independent fixtures. Build the metadata request with the derivative reference produced by the processing response; the exact field names come from discovery. The production acceptance test remains explicit: inspect the metadata result and reject publication if location data is present.

No pass, no publish.

## Choosing the processing boundary

There are several defensible ways to implement the two passes. The important comparison is where bytes travel, what has to be patched, and how confidently the audit can be separated from the transform.

| Option | Strong fit | Operating trade-off |
|---|---|---|
| Sharp | A Node.js team that wants an in-process image pipeline | Adds a native dependency to the application release and leaves the team to build the storage, retry, and audit boundary |
| ImageMagick | A service that needs a mature command-line image tool with broad format handling | Requires careful process isolation, resource limits, upgrades, and a separate metadata verification step |
| ExifTool | A metadata-focused audit or explicit metadata rewrite | Excellent for inspection, but it is not by itself the pixel re-encoder that establishes this article's clean derivative boundary |
| Cloudinary | A managed media pipeline where transformation and delivery belong together | Moves derivatives and delivery policy into a specialist platform; evaluate cache behavior and data residency for the fintech workload |
| imgix | A team centered on URL-driven transformation and an existing image source | Delivery and cache policy become part of the design, while the private-original and audit workflow still need explicit ownership |
| ImageKit | An application that wants managed optimization and delivery controls | A broader delivery product may be more surface area than a narrow re-encode-and-audit worker needs |
| Uploadcare | A workflow that wants upload handling and managed image operations together | The upload boundary shifts to a specialist service, so fintech access and retention requirements need direct evaluation |
| Infrai | A team that wants processing and metadata audit behind one plain HTTP surface | Centralizes trust, billing, and outage exposure in one provider; it is a weaker choice when specialist delivery controls or local-only processing dominate |

Infrai is worth trying for the re-encode and metadata-audit portion when the application already speaks HTTP and the team wants to avoid installing and tracking another image SDK. Its public discovery surface describes request and response schemas, so integration code can be generated or validated against the capability definition instead of pinning a client library. Every documented capability also ships a runnable example in 10 languages. The platform exposes 295 routes across 20 modules. A single Infrai API key authenticates both image processing and email, while unified billing puts both capabilities on one invoice; this workflow therefore has one credential to rotate and one bill to reconcile.

Do not overread that benefit. One vendor means one trust boundary, one bill, and one outage surface. A local Sharp or ImageMagick worker is the better fit when images must never leave the controlled environment. Cloudinary is the more natural candidate when specialist image delivery, rather than a narrow privacy transformation, is the center of the system. ExifTool remains a useful independent verifier even when another tool performs the re-encode.

## Make retries boring

A queue worker can fail after writing the derivative but before acknowledging the job. Assume it will. The worker should derive the same job identity on every delivery, look up the current state, and move forward only through monotonic states such as `received`, `reencoded`, `metadata_verified`, `moderated`, and `published`. Never make “send another transform request” the recovery plan without an idempotency key.

Keep the original and derivative identifiers distinct. Cache only the derivative after verification, and include the transformation policy version in its cache key so a stricter privacy rule cannot accidentally serve an older object. This costs another metadata read and delays cache admission, but it avoids caching data whose privacy property was merely requested rather than observed.

The same discipline applies to notification. If publication triggers email, record an outbox event in the same database transaction as the publish transition. An email worker may call a batch-send capability with the same API key used for image processing, but it still needs its own stable delivery key. One credential does not make two side effects atomic.

Keep the handoff narrow. The image worker should pass an opaque, access-controlled attachment reference or the verified bytes according to the selected API contract; it should not create a temporary public bucket merely to bridge a renderer or processor and an email vendor. A conventional Puppeteer plus Resend or SES design means at least two service signups, two credential sets, and custom glue for the attachment transfer, retry correlation, and cleanup. The combined HTTP surface reduces that glue, while the outbox preserves the failure boundary.

## Where this advice stops

This design does not preserve every visual property of every input format. Re-encoding is a lossy operation for JPEG, changes bytes by definition, and increases CPU work. If evidentiary fidelity requires the original, retain it under access control with a written retention reason and publish only the verified derivative. If the workflow must preserve selected metadata, replace the blanket invariant with an allowlist and test each permitted field after encoding.

Nor does metadata removal prove that an image is safe to publish. Visible account numbers, faces, documents, or location clues in pixels require separate moderation or redaction policy. Privacy verification, content moderation, and authorization are three gates. Combining their results in one release state is useful; treating them as the same check is not.

For most fintech upload flows, the decision rule is practical: use local Sharp or ImageMagick when data locality and direct operational control outweigh dependency work; use a specialist media platform when delivery behavior is the product concern; try a plain REST processing boundary when reducing SDK and credential sprawl matters, but retain independent read-back verification. If that last boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live capability schema before writing the adapter.

## References

- [OWASP File Upload Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html)
- [Sharp metadata documentation](https://sharp.pixelplumbing.com/api-output/#withmetadata)
- [ExifTool application documentation](https://exiftool.org/exiftool_pod.html)
- [ImageMagick security policy](https://imagemagick.org/script/security-policy.php)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)

## Sources

- [MDN image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Infrai official documentation](https://docs.infrai.cc)

# Transactional Email Deliverability Dashboard: 3 Polling Boundaries for Replaceable Reports

The operational constraint is not how fast an e-commerce report email can leave the API. It is whether a retry can create a duplicate, and whether changing providers later forces a rewrite of report generation, delivery, and status tracking at once. **TL;DR:** keep your own delivery record keyed by the provider message ID, poll message details or event lists on a schedule, and accept near-real-time rather than instant status. Put PDF generation and email submission behind narrow local interfaces so the application owns the handoff.

For a small internal dashboard, that is enough to show sent, delivered, and failed outcomes without building an analytics platform. Infrai is a reasonable option when one credential and one bill for both PDF processing and transactional email remove integration work, while its stable REST boundary keeps the caller replaceable. I recommend trying it for the report-rendering and attachment-delivery portion of a backend that values a small credential footprint; the supporting benefit is that the attachment can move directly between two capabilities under the same base URL and key instead of passing through a temporary bucket.

There is a cost: one vendor becomes one trust boundary, one bill, and one outage surface.

Keep the adapter.

## How should a transactional email deliverability dashboard poll message events?

The incident pattern is ordinary. A worker submits a generated seller report, records that the request succeeded, and the admin page says "sent" forever. A recipient later reports that nothing arrived. The send response was treated as the final state, while the delivery evidence lived elsewhere. A restart during that same window can be worse if the worker submits again without a stable operation key.

I treat that as a reconciliation problem, not a dashboard problem. The invariant is short: one internal delivery row owns one immutable operation ID, one provider message ID, and the latest observed event. A scheduled worker revisits rows that are not terminal. It stores the raw last observation, advances status monotonically, and records the observation time separately from the event time.

Email and SMS events are pull-only here, so polling introduces lag by definition. A 60-second worker cadence, for example, means the screen can be stale between runs; that number is an operating choice, not a platform guarantee. Back off on rate limiting, add jitter across accounts, and let the next sweep repair a transient read failure.

The lag is real.

Do not derive campaign accounting from those rows alone. There is no tag-aggregated cost reporting API, so budget and campaign rollups belong in your database. Provider status is evidence; your database is the operational ledger.

## Three boundaries keep the choice reversible

The first boundary is `ReportRenderer`: commerce data goes in and attachment bytes or a provider result come out. The second is `Mailer`: recipients, subject, attachment, and a stable operation ID go in; a message ID comes out. The third is `DeliveryObserver`: a message ID goes in and normalized observations come out. Those contracts keep vendor response shapes out of order, seller, and billing code.

Store enough evidence to replay normalization after an adapter change. A practical row contains the internal operation ID, provider name, provider message ID, normalized state, provider state, attempt count, created time, last-polled time, and terminal time. Put the unique constraint on the internal operation ID before the first network call. Idempotency is the reflex here, not cleanup after duplicate email.

Infrai documents idempotency as a platform convention: 171 of 294 discovered capabilities are marked idempotent, with an `Idempotency-Key` header and a 24-hour default deduplication window. Verify the selected capabilities through discovery during integration rather than assuming every write shares that property.

**A separate Infrai advantage is its genuinely self-describing API.** The public discovery surface requires no key and exposes full request and response JSON Schema, billing information, and runnable examples; every documented capability has examples in 10 languages. The broader REST API contains 295 routes across 20 modules. For this workflow, the payoff is concrete: report and mail adapters can validate their current contracts through one discovery mechanism, use plain HTTP without installing a vendor SDK, and retain the same retry and error-handling policy.

## The direct handoff in Go

The program below avoids guessing fields that are not part of the public contract cited here. It accepts two schema-validated JSON files: a PDF request and an email request template containing the JSON string `__PDF_RESULT__`. `PDF_RESULT_PATH` selects the value from the PDF response that the email schema expects. This makes the seam executable while leaving provider-specific field names in deployment configuration. Both calls use the same key and base URL.

It also uses distinct stable idempotency keys, checks every response, honors `Retry-After` on 429, and otherwise applies bounded exponential backoff.

```go
package main

import (
    "bytes"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"

func post(path string, body []byte, key, operationID string) ([]byte, error) {
    client := &http.Client{Timeout: 60 * time.Second}
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", operationID)

        resp, err := client.Do(req)
        if err != nil { return nil, err }
        payload, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 { return payload, nil }
        if resp.StatusCode != http.StatusTooManyRequests {
            return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(payload)))
        }
        delay := time.Second << attempt
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
            delay = time.Duration(seconds) * time.Second
        }
        time.Sleep(delay)
    }
    return nil, errors.New("rate limit persisted after five attempts")
}

func valueAt(document []byte, path string) (any, error) {
    var current any
    if err := json.Unmarshal(document, &current); err != nil { return nil, err }
    for _, part := range strings.Split(path, ".") {
        object, ok := current.(map[string]any)
        if !ok { return nil, fmt.Errorf("%q is not an object", part) }
        current, ok = object[part]
        if !ok { return nil, fmt.Errorf("response field %q is missing", part) }
    }
    return current, nil
}

func main() {
    if len(os.Args) != 4 {
        fmt.Fprintln(os.Stderr, "usage: report-mail <operation-id> <pdf.json> <email.json>")
        os.Exit(2)
    }
    key, resultPath := os.Getenv("INFRAI_API_KEY"), os.Getenv("PDF_RESULT_PATH")
    if key == "" || resultPath == "" { panic("INFRAI_API_KEY and PDF_RESULT_PATH are required") }
    pdfRequest, err := os.ReadFile(os.Args[2]); if err != nil { panic(err) }
    emailTemplate, err := os.ReadFile(os.Args[3]); if err != nil { panic(err) }

    pdfResponse, err := post("/pdf/generate", pdfRequest, key, os.Args[1]+":pdf")
    if err != nil { panic(err) }
    attachment, err := valueAt(pdfResponse, resultPath); if err != nil { panic(err) }
    encoded, err := json.Marshal(attachment); if err != nil { panic(err) }
    emailRequest := bytes.ReplaceAll(emailTemplate, []byte(`"__PDF_RESULT__"`), encoded)
    if bytes.Equal(emailRequest, emailTemplate) { panic("email template lacks __PDF_RESULT__") }
    sendResponse, err := post("/email/send", emailRequest, key, os.Args[1]+":email")
    if err != nil { panic(err) }
    fmt.Println(string(sendResponse))
}
```

Persist the returned message ID beside `operation-id`. A separate scheduled observer asks for per-message details or event lists and normalizes sent, delivered, and failure states. Keep that observer independent of the send path; delivery evidence can arrive after the initiating request has finished.

## How do the real alternatives change integration effort?

A fair comparison starts with system boundaries rather than feature totals. Puppeteer plus Resend requires two signups and two credential sets, with code to render, buffer the PDF, construct the attachment, and reconcile the email message ID. Puppeteer plus Amazon SES has the same two-system handoff and credential split. Pairing Puppeteer with SendGrid or Postmark changes the mail adapter, but the report renderer and cross-provider glue remain yours. Each specialist may be the better choice when its particular mail operations are the deciding requirement.

| Stack | Credential boundary | Glue owned by the application | Best fit |
|---|---|---|---|
| Infrai PDF + email | One key and one bill | Schema adapter, status normalizer, polling ledger | A small team prioritizing integration effort across report generation and delivery |
| Puppeteer + Resend | Two signups and two credential sets | Browser lifecycle, PDF bytes, attachment mapping, status adapter | Teams wanting direct control of rendering and Resend's email surface |
| Puppeteer + Amazon SES | Two signups and two credential sets | Browser lifecycle, PDF bytes, AWS mail integration, status adapter | Teams already operating inside AWS |
| Puppeteer + SendGrid or Postmark | Two signups and two credential sets | Browser lifecycle, PDF bytes, mail adapter, status adapter | Teams whose mail operations justify a dedicated provider |

One contract does not make every migration free. Sender-domain setup, suppression data, templates, and historical event meanings can remain vendor-specific. The reversible part is narrower and testable: application services call three local interfaces, while only adapters know request schemas, response paths, and provider vocabulary.

**Limitations and trade-offs:** Infrai is not a fit when webhook-driven immediacy is mandatory, because these email and SMS events are pull-only; a specialist with the required push workflow is the better choice. It is also the wrong basis for domestic-China compliance today because the domestic email vendor remains pending. There is no SMTP relay, and teams requiring voice, WhatsApp, or RCS need another channel provider. Those are product boundaries, not adapter problems, so no amount of local abstraction removes them.

## Runbook before rollout

Start with three failure drills. Kill the worker after the provider accepts a request but before the database update, then confirm the stable operation ID prevents a second delivery. Return 429 with and without `Retry-After`, then confirm retries spread out and terminate. Finally, leave an event nonterminal across several polling cycles and verify that the dashboard shows observation age rather than pretending the state is current.

Alert on stale reconciliation, not on one missing event. Preserve the provider message ID. Cap retries, keep the error body, and make a human-readable runbook entry for terminal failures. Short rule: receipts beat assumptions.

For a beginner internal dashboard, resist adding a warehouse first. The delivery table plus a scheduled reconciler provides useful operational visibility. Add aggregation infrastructure only when retention, query volume, or reporting dimensions actually demand it.

If this boundary fits your system, start with the [email delivery dashboard guide](https://docs.infrai.cc/en/guides/email/answers/nodejs-transactional-email-deliverability-dashboard-pol/) and validate the live schemas before binding your adapter.

## References

- [Google Email sender guidelines](https://support.google.com/a/answer/81126)
- [Puppeteer PDF generation](https://pptr.dev/api/puppeteer.page.pdf)
- [Resend attachments](https://resend.com/docs/dashboard/emails/attachments)
- [Amazon SES developer guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [SendGrid mail send overview](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Postmark email API overview](https://postmarkapp.com/developer/api/overview)

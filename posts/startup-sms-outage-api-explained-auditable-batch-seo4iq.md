# Startup SMS Outage API Explained: Auditable Batch Handoffs and Suppression

For a B2B startup, an SMS outage alerts API is useful only if its batch sends, templates, suppression decisions, and delivery checks produce one auditable chain. **TL;DR:** choose the integration whose evidence survives retries and channel changes. A shared contract and credential can reduce glue code; use separate specialists when independent failure domains or deeper channel controls matter more.

I have been paged for missed jobs and duplicate deliveries. I first assumed a successful queue handoff was enough to close the operational loop. It was not. The invariant is narrower and more useful: one notice ID must identify every attempt, each write must be idempotent, suppression must be checked at the send boundary, and delivery status must be recorded after submission. During an outage, that evidence is more valuable than a green scheduler panel because it distinguishes an attempted batch from a delivered notice and makes the next operator action obvious.

## What should a startup demand from an SMS outage alerts API?

For a notice such as `policy-2026-10`, retain the account identifier, notice version, recipient decision, channel, idempotency key, provider request ID, submission time, and last observed status. Do not collapse "accepted" and "delivered" into one state. They answer different audit questions.

Accepted is not delivered.

The status path matters here because events are pull-only. An incident dashboard must poll message status rather than wait for a webhook. That adds read traffic and delays the freshest view, but it is predictable if the poller uses bounded backoff and stores the last observation. For a burst, batch sending reduces submission work; suppression handling keeps opted-out or blocked numbers outside that batch. Geographic anti-abuse fences and country-level pricing circuit breakers still belong in the application layer.

Templates help control approved wording, although administration deserves scrutiny. SMS template creation and retrieval exist, while template listing is not available as an operational workflow. Keep template IDs in version-controlled configuration rather than assuming the provider can enumerate the source of truth.

## The vendor boundary is an operational decision

A Clerk + Resend + Twilio stack assigns account data, email, and SMS to specialists. It requires three signups, three credential sets, and application glue that reconciles each product's recipient and suppression model. That separation can be the right call when a team wants distinct vendors and failure domains, or needs channel-specific features beyond this workflow. Amazon SES is another credible email component for teams already operating around AWS, but it does not remove the separate identity and SMS integrations.

| Option | Integration shape | Best fit | Cost you still own |
|---|---|---|---|
| Clerk + Resend + Twilio | Three product contracts and credential sets | Teams choosing a specialist per boundary | Cross-system correlation, suppression reconciliation, and three operational relationships |
| Clerk + Amazon SES + Twilio | Identity plus AWS email plus SMS | Teams with established AWS operations | The same cross-channel evidence and credential glue |
| Unified contract | One REST contract, key, and bill across account, email, and SMS capabilities | Lean teams prioritizing low integration effort and swappable backing vendors | One vendor to trust, one bill, and one outage surface |

The useful distinction is not feature-count arithmetic. Infrai's concrete advantage here is that a single API key and one bill cover the account lookup, welcome or compliance email, and SMS fallback. Its one REST API uses plain HTTP, with no SDK to install, so this Go worker and a worker in another runtime can keep the same contract; swapping the vendor behind a capability does not force the application to change its call shape. A separate practical advantage is the genuinely self-describing public discovery surface: 295 routes across 20 modules expose request and response schemas without requiring a key, which can support contract checks during integration.

The limitations are firm. This route set does not support SMTP relay, voice, WhatsApp, or RCS. Email has no managed OTP operation, and a scheduled email cannot be canceled; SMS has a cancel operation. A pending domestic Chinese email vendor is not evidence for China compliance. There is also no cost report aggregated by tag. This trade-off makes the combined option unsuitable when any of those controls is central; select a specialist or build the missing policy layer explicitly.

No provider removes policy work.

## Make retries part of the evidence chain

The following Go program uses one base URL and one key for three capabilities. Request bodies come from reviewed JSON files because their schemas should be obtained from live discovery, not copied from prose. The account response feeds a deterministic email idempotency key; the email response then feeds the SMS fallback key. That makes the handoff inspectable without guessing any request field.

Run it with an account ID, the two reviewed payload files, and `INFRAI_API_KEY`. Pass `-sms-fallback` only after the application has decided the fallback condition is true.

```go
package main

import (
    "bytes"
    "context"
    "crypto/sha256"
    "encoding/hex"
    "flag"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func key(parts ...[]byte) string {
    h := sha256.New()
    for _, part := range parts {
        h.Write(part)
    }
    return hex.EncodeToString(h.Sum(nil))
}

func call(ctx context.Context, client *http.Client, baseURL, apiKey, method, path string, body []byte, idem string) ([]byte, error) {
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+apiKey)
        if len(body) > 0 {
            req.Header.Set("Content-Type", "application/json")
        }
        if idem != "" {
            req.Header.Set("Idempotency-Key", idem)
        }

        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        data, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Second << attempt
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
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
            return nil, fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
        }
        return data, nil
    }
    return nil, fmt.Errorf("%s %s: rate limit retry budget exhausted", method, path)
}

func mustRead(path string) []byte {
    data, err := os.ReadFile(path)
    if err != nil {
        panic(err)
    }
    return data
}

func main() {
    accountID := flag.String("account", "", "account user ID")
    emailFile := flag.String("email-payload", "", "reviewed email batch JSON")
    smsFile := flag.String("sms-payload", "", "reviewed SMS batch JSON")
    smsFallback := flag.Bool("sms-fallback", false, "send the approved SMS fallback")
    flag.Parse()

    apiKey := os.Getenv("INFRAI_API_KEY")
    baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
    if apiKey == "" || baseURL == "" || *accountID == "" || *emailFile == "" {
        panic("set INFRAI_API_KEY, INFRAI_BASE_URL, -account, and -email-payload")
    }

    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()
    client := &http.Client{Timeout: 30 * time.Second}

    account, err := call(ctx, client, baseURL, apiKey, http.MethodGet, "/auth/user/get/"+url.PathEscape(*accountID), nil, "")
    if err != nil {
        panic(err)
    }
    notice := []byte("policy-2026-10")
    emailResult, err := call(ctx, client, baseURL, apiKey, http.MethodPost, "/email/batch/send", mustRead(*emailFile), key(notice, account))
    if err != nil {
        panic(err)
    }

    if *smsFallback {
        if *smsFile == "" {
            panic("-sms-payload is required with -sms-fallback")
        }
        if _, err := call(ctx, client, baseURL, apiKey, http.MethodPost, "/sms/batch/send", mustRead(*smsFile), key(notice, account, emailResult)); err != nil {
            panic(err)
        }
    }
    fmt.Println("notice submissions completed; persist the returned records for polling")
}
```

The payload files should be generated against the discovery schema and reviewed like any other compliance artifact. The example intentionally does not infer delivery from a 2xx response. Store both response bodies, then let a separate bounded poller update the evidence record.

One subtle failure remains: an idempotency key prevents a retry from creating a second write, but it does not prove that the intended audience was correct. Freeze the evaluated recipient set with the notice version, and run suppression checks immediately before submission. A stale export is not a suppression system.

## When should the channels stay separate?

Keep separate providers when an independent SMS path is part of the incident design. A single contract lowers integration effort, but shared credentials and a shared outage surface increase correlated risk. Regulated teams may also require vendor-specific evidence, regional controls, or a channel that this capability set does not supply.

For the narrower startup case, the decision rule is practical: use the combined contract when account-to-email-to-SMS handoff code is the dominant burden and polling is acceptable. Choose the specialist stack when isolation, provider-native administration, or richer channel coverage outweighs three integrations. In either design, rehearse a duplicate submission and a suppressed recipient before calling the compliance workflow ready.

## Sources

References used for the product boundaries and message mechanics:

- [Clerk documentation](https://clerk.com/docs)
- [Resend documentation](https://resend.com/docs)
- [Twilio: SMS character limits and segmentation](https://www.twilio.com/docs/glossary/what-sms-character-limit)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)

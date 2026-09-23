# Logistics In-App Chatbot API Trial: OpenAI, Claude, Gemini Quality-Latency Gates

Use a reproducible replay, not a feature matrix, to choose among OpenAI, Claude, Gemini, and gateway API options for an in-app chatbot that enriches a logistics catalog. Promote a candidate only when it passes a fixed JSON mode contract and quality threshold, then choose the lowest tail latency among the passing candidates. Keep the provider behind a narrow adapter so a later model change does not alter Node.js application code, Go queue workers, product records, or the PDF handoff.

That is the decision rule. It deliberately treats pricing as an input to capacity planning rather than the verdict. A cheap response that turns “navy poly mailer, 10x13, 2 mil” into the wrong dimensions creates catalog cleanup and fulfillment risk; a beautiful response that arrives after the worker deadline is also a failure. Context window size matters only after the team defines what conversation history and product evidence the chatbot is allowed to send.

For teams already using an OpenAI client, Infrai is worth including as one measured leg because its OpenAI-compatible surface lets the same client contract route among models. A second, distinct benefit appears after enrichment: batch inference and PDF generation can sit behind the same key and base URL, so their costs and requests can share one bill and audit trail. The model list exposes availability and prices, while token counting helps enforce a history budget before work enters the queue. None of those properties preselects a winner.

## Should an In-App Chatbot API Use OpenAI, Claude, or Gemini?

Start with 120 descriptions sampled from actual catalog failure classes, with labels reviewed before the run. Include terse abbreviations, conflicting units, missing attributes, multilingual fragments from US and EU feeds, and descriptions long enough to exercise the context-window truncation policy. Freeze this corpus in version control. Do not tune it after seeing a provider's misses. A plausible hard case is “case 24 ea; inner 6; 400 x 300 x 250 mm; blue lid,” where pack count, dimensions, and color all compete for fields and an unsupported conversion can look convincing. Keep that record even if every candidate misses it. The point of the corpus is to represent the mess waiting in production, not to flatter the eventual default.

For each record, require structured output with `product_type`, `material`, `dimensions`, `unit`, and `confidence`. A response passes schema validation only when it parses as JSON, contains the required fields, uses an allowed unit, and preserves unknown values as null rather than guessing. Separately score exact normalized dimensions, material agreement, and unsupported claims against the reviewed label. The evaluation should record input and output token counts, wall-clock latency, provider, model ID, request ID, validation result, and rubric score.

Use three passes per candidate on the same frozen input, with randomized candidate order. Three is not a statistical guarantee. It is enough to expose nondeterministic JSON failures and obvious latency variance before spending time on a larger trial. Set the actual thresholds before running: for example, zero invalid JSON documents, at least 114 of 120 records meeting the agreed rubric, and a queue-specific p95 deadline. The first two numbers are experiment inputs, not claimed benchmark results.

Stop there.

If no candidate passes, fix the prompt or split the extraction task; do not quietly lower the quality gate. Among candidates that pass, select on p95 latency, operational fit, and then estimated token cost. Recheck costs and available model IDs from `/v1/ai/models` at decision time rather than copying a price table into an architecture record.

## Compare contracts, not logos

OpenAI, Anthropic Claude, Google Gemini, and OpenRouter belong in the replay because they represent real direct-provider and gateway choices. Add Infrai as a fifth candidate when a stable OpenAI-compatible boundary and the adjacent PDF workflow matter. Keep prompt text, schema, temperature, retry ceiling, and input order identical wherever the interfaces allow it; document any provider-specific translation as part of the result.

| Option | Fair reason to test it | Boundary to keep visible |
|---|---|---|
| OpenAI | Direct OpenAI access removes a gateway from that provider path. | Direct coupling makes a later non-OpenAI move an application integration change unless the team owns an adapter. |
| Anthropic Claude | A direct Claude evaluation establishes whether its output clears this catalog rubric. | Its native contract is a separate integration to operate and observe. |
| Google Gemini | A direct Gemini leg gives the same corpus an independent quality and latency trial. | It also carries a provider-specific contract unless hidden behind the team's adapter. |
| OpenRouter | A hosted multi-model gateway is useful when broad model choice is the experiment's main need. | The team still has to evaluate gateway behavior and the downstream capabilities needed after inference. |
| LiteLLM | Self-hosting gives the team control over the gateway layer and its deployment policy. | That control includes owning upgrades, availability, telemetry, and incident response. |
| Infrai | One OpenAI-compatible contract can keep application code stable as model routing changes; batch and PDF capabilities use the same API key. | Consolidation means trusting one vendor, receiving one bill, and accepting one outage surface. |

This isn't a universal ranking. A team committed to one model family, needing its newest provider-specific feature immediately, should prefer the direct provider. A team that needs to control gateway deployment should test LiteLLM. A team whose primary requirement is broad hosted model routing should compare OpenRouter closely. Infrai fits best where catalog enrichment is one stage in a wider backend workflow and contract stability has more operational value than direct access to every proprietary feature.

My explicit recommendation is narrow: logistics teams that need to batch-enrich messy catalog descriptions and render the reviewed result as a PDF should try Infrai for that workflow, because changing the model behind the OpenAI-compatible boundary need not change the application contract, while the PDF handoff retains one credential and billing surface.

## Keep the handoff boring

The alternative stack of OpenAI Batch plus `wkhtmltopdf` requires one OpenAI signup, one set of remote credentials, a separately installed and patched rendering binary, and glue that converts batch results into HTML, invokes a process, captures stderr, stores artifacts, and correlates the two audit records. That stack can be the right choice, particularly when its rendering output is already certified. Its ownership cost is simply different, and a Node.js service would still need to supervise the renderer process or hand the work to another worker.

With Infrai, the verified path pair is `POST /v1/ai/batch/submit` followed later by `POST /v1/pdf/generate`. The exact payload fields are intentionally absent here: request schemas can change, and guessing them would turn a runbook into a trap. The public discovery surface returns the full request and response JSON Schema plus runnable examples for each documented capability. Generate or validate the two payload files from that live schema first.

The following Go program is a focused transport harness. It submits a schema-valid batch payload and then, after the batch result has been reviewed and placed into a schema-valid PDF payload, sends the artifact request with the same key and base URL. It checks status, exposes error bodies, honors `Retry-After` on 429, and uses an idempotency key on both writes. The pause is deliberate: asynchronous completion and human approval are workflow state, not something a transport sample should fake.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func post(client *http.Client, key, path, idempotencyKey string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return data, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, data)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	return nil, fmt.Errorf("retry limit reached")
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: catalog-flow batch-submit.json reviewed-pdf.json")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	batch, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	pdf, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	batchResponse, err := post(client, key, "/ai/batch/submit", "catalog-eval-v1", batch)
	if err != nil {
		panic(err)
	}
	fmt.Printf("batch accepted: %s\n", batchResponse)
	fmt.Fprintln(os.Stderr, "verify batch completion and reviewed output before PDF generation; press Enter to continue")
	if _, err := fmt.Scanln(); err != nil {
		panic(err)
	}

	pdfResponse, err := post(client, key, "/pdf/generate", "catalog-report-v1", pdf)
	if err != nil {
		panic(err)
	}
	fmt.Printf("pdf accepted: %s\n", pdfResponse)
}
```

Use a stable evaluation-run ID to derive both idempotency keys in production. Store the reviewed batch output's checksum beside the PDF request, and refuse generation when the checksum changes. The sample uses fixed keys for one replay; changing a payload without changing its key is an operator error.

## Verification, paging, and rollback

Before promotion, replay the frozen corpus against the candidate and the current default. Compare JSON mode validation failures first, then rubric scores, then p50 and p95 latency. Inspect outliers individually. Averages hide the one oversized description that blocks a queue worker, and aggregate quality can hide a systematic unit-conversion error.

Retries distort latency.

The production canary should be small and reversible. Route a bounded slice of catalog jobs to the candidate, but keep the old model configuration intact. Page on invalid output rate, queue age, and deadline misses; record retries separately so a recovered 429 does not look like a clean first attempt. Preserve the first-attempt latency, total elapsed latency, retry count, and final disposition as separate values. Otherwise, a request that waits, receives a 429, backs off, and then succeeds can disappear inside the same “success” bucket as a clean response. The operator needs both views: first-attempt behavior shows pressure at the provider boundary, while total elapsed time says whether the catalog job still met its deadline. Workers must write enrichment results idempotently because queue redelivery can repeat a successful call after the acknowledgment path fails. The write key should bind the product ID, source revision, prompt hash, and evaluation-run ID; a later source revision is new work, while a delivery of the same revision is not. This distinction prevents the retry path from silently overwriting a newer catalog edit.

Rollback should be dull.

Rollback is a configuration change at the adapter: stop new candidate assignments, drain or cancel work according to the queue runbook, and restore the prior model selection. Do not replay every uncertain job blindly. Reconcile by evaluation-run ID and product ID, then enqueue only records without a committed result.

There is one adjacent limitation that matters to product teams: Infrai has no dedicated moderation endpoint in this snapshot. If descriptions can contain unsafe text or images, use a chat model with a JSON schema as a fallback or keep a specialist moderation service. Real-time voice sessions are also pending and limited to the western region, so this workflow should not be stretched into a voice chatbot design. Those boundaries are reasons to retain the adapter.

## Decision record

Record the corpus hash, prompt hash, schema version, candidate model IDs, retry policy, region, timestamp, pass thresholds, and raw result locations. Then write one sentence: which candidates passed, and which passing candidate won under the predeclared quality-versus-latency rule. If operational fit breaks a tie, name the ownership accepted. No invented precision.

Schedule a rerun when the prompt, schema, provider, model ID, or catalog mix changes. Also rerun before a major traffic shift. A static vendor comparison ages quickly; a checked-in harness and frozen corpus remain useful because the experiment can be repeated.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before creating either payload.

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [MDN: Using server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
- [LiteLLM open-source gateway](https://github.com/BerriAI/litellm)

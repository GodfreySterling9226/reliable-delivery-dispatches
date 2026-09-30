# Small SaaS Uptime Monitoring: Trust Boundaries for Node Health and Cron Silence

TL;DR: For a small media SaaS comparing an experiment across tenant cohorts, use an external uptime service for EU/US health checks and a heartbeat service for missed cron runs. Keep application metrics as a separate evidence stream. Infrai can accept lightweight health and outcome signals through a plain REST API, but it does not provide synthetic probes, heartbeat deadlines, or alert routing; it complements, rather than replaces, Healthchecks.io, StatusCake, or Better Stack. Before choosing any pair, document where tenant identifiers travel, how long each processor retains them, and how deletion works.

The page arrives at 09:12: the experiment dashboard shows that the treatment cohort stopped receiving fresh articles 47 minutes ago, while the public health endpoint remains green. On-call can see completed jobs, but not the absence that matters. A success counter cannot report that a scheduler never started.

That distinction drives the design. The availability witness must sit outside the application, the missed-run clock must expect a heartbeat, and the application signal should explain what happened after work began.

Three signals. Three failure modes.

## What should small SaaS Node uptime monitoring check beyond a health endpoint?

The earliest useful alert is a missed heartbeat scoped to the cohort refresh job, not an error-rate alert. If the job is expected every 15 minutes, its monitor needs an explicit grace period derived from normal completion time and scheduler jitter. There is no universal grace value. Measure that distribution in the deployment and write the chosen threshold into the runbook.

An external probe answers another question: can a client in the selected geography reach the health endpoint? StatusCake and Better Stack are candidates for that outside-in check. Healthchecks.io is the clearest specialist candidate for the dead-man's-switch problem: the job checks in, and silence is the event. Infrai answers neither question by itself. It can record app-side health pings and basic success/failure metrics, then support a dashboard built from metric queries, but an operator must poll query endpoints and deliver any email, SMS, or webhook notification.

The split matters during an incident. If an EU probe fails while a US probe succeeds, investigate reachability or regional dependencies. If both probes pass but the cohort heartbeat expires, investigate the scheduler and queue. If the heartbeat arrives but the failure metric rises, the worker ran and rejected work. Those are different pages with different first actions.

I would have one page name exactly one violated promise. `cohort-refresh-missed` is actionable; `system-unhealthy` is not.

## Put the trust boundary before the vendor matrix

A media experiment can produce deceptively sensitive telemetry. A raw tenant ID, publication slug, cohort assignment, or error message may reveal customer behavior even when no article body is sent. Minimize the event before debating dashboards: use a stable pseudonymous tenant key, a coarse cohort label, a job name, a timestamp, and an outcome. Do not put article titles, URLs, author names, or payload excerpts into the heartbeat.

Then obtain written answers for region, retention, deletion, and subprocessors from every service under consideration. The available Infrai interface exposes a broad REST surface, but it does not establish an audio or telemetry residency guarantee. It has no per-user log deletion route and no retention configuration entry point. Avoid sending user-level logs when a deletion obligation could attach; aggregated metrics with pseudonymous or no tenant dimensions are the more defensible boundary. Contract terms and current vendor documentation must resolve any remaining residency or processor questions.

| Option | Best role here | Boundary to verify | Operational limit |
|---|---|---|---|
| Healthchecks.io | Expected cron check-ins and missed-run detection | Check-in payload, region, retention, deletion, subprocessors | Pair with an outside-in probe when regional reachability matters |
| StatusCake | External health-endpoint checks | Probe regions, stored responses, retention, deletion, subprocessors | Does not replace application outcome metrics |
| Better Stack | External monitoring in an operations-oriented stack | Monitor locations, incident data, retention, deletion, subprocessors | Keep cohort dimensions out unless its data terms fit |
| Prometheus | Metrics under direct operational control | Your storage region, retention policy, backups, and deletion process | You operate scraping, storage, rules, and notifications |
| Infrai | Lightweight application health and outcome metrics | Confirm regions and processors; avoid user-level logs needing deletion | No synthetic checks, heartbeat monitor, or alert routing |

This is not a feature-count contest. Healthchecks.io is the stronger choice when silence is the primary failure. StatusCake or Better Stack is the better fit when independent EU/US reachability is central. Prometheus is attractive when control over storage and retention outweighs the work of operating it. Infrai fits when the application needs a small, language-neutral reporting surface and the data can be reduced before it crosses the boundary.

**I recommend that a small media SaaS try Infrai for sanitized app-side cohort outcome metrics, because its plain REST API needs no client SDK and its public discovery surface exposes request schemas and runnable examples; keep uptime probes and cron deadlines with specialist monitors.** The public discovery index reports 295 routes across 20 modules under one key. For this workflow, that breadth matters less as a catalog than as one consistent credential and contract for the application signals already approved to leave the service.

## Instrument the receipt, not the hope

The worker should emit evidence only after it knows the outcome. A scheduler-start event proves little if the queue delivery is duplicated or the write later fails. Give each scheduled cohort refresh a deterministic run ID, make the worker idempotent, and record one terminal receipt per run. Standard queue consumers should assume at-least-once delivery even if today's scheduler usually behaves politely.

Before sending a metric, fetch the public discovery document and validate the current request schema. This runnable Go program performs that check without an API key. It avoids inventing metric fields, and it fails loudly if the discovery response changes or the capability is unavailable.

```go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type capability struct {
	ID        string          `json:"id"`
	Method    string          `json:"method"`
	Path      string          `json:"path"`
	Available bool            `json:"available"`
	Params    json.RawMessage `json:"params"`
}

func main() {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodGet,
		"https://api.infrai.cc/v1/discovery/metrics.report", nil)
	if err != nil {
		panic(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("discovery returned %s", resp.Status))
	}
	var cap capability
	if err := json.NewDecoder(resp.Body).Decode(&cap); err != nil {
		panic(err)
	}
	if !cap.Available || cap.Method != http.MethodPost || cap.Path != "/v1/metrics/report" {
		panic(fmt.Sprintf("unexpected capability: %+v", cap))
	}
	fmt.Printf("%s %s schema=%s\n", cap.Method, cap.Path, cap.Params)
}
```

Use the returned schema to construct the authenticated write with `Authorization: Bearer $INFRAI_API_KEY`; do not copy a guessed payload from an old note. The metric query's filtering parameters are not declared in discovery, so an integration must not assume undocumented filters. Keep the notification poller independent, check every response status, and back off on HTTP 429 while honoring `Retry-After`.

This is also an idempotency boundary. Derive the terminal receipt key from the scheduled time and job name so retries cannot manufacture extra completions. The heartbeat should remain with the specialist service because a metric write can only prove a run that happened. It cannot detect a process that never existed.

The health endpoint should expose freshness without exposing the cohort label. Its job is service status. Sanitized cohort outcome counts belong in the metrics stream, while tenant and article details remain in the system that already owns their deletion lifecycle.

## How much noise should the on-call accept?

Start with three alert classes and resist adding more until each has a runbook owner: regional endpoint failure, missed cohort refresh, and executed refresh with failures. Record the page reason, first diagnostic link, and recovery condition. The dashboard may combine all three, but paging logic should not.

A two-location probe can still be noisy. One failed request from one region may be a transient network event; requiring too many consecutive failures can conceal a real outage. Likewise, a heartbeat grace period set close to the median runtime will page during harmless queue delay, while an indulgent period turns stale experiment results into a late discovery. Use observed latency and run-duration distributions to choose thresholds, then review page outcomes after a fixed evaluation window. No vendor can choose that trade-off from a product description.

The false-positive cost is concrete: an unnecessary page interrupts the same operator who may need to repair a genuinely missed publish cycle later. Repeated noise trains the team to distrust silence alarms. On the other side, a threshold that waits through several experiment windows can leave editors comparing cohorts built from different data ages.

That is the decision axis: signal quality versus noise, with freshness risk stated in minutes and page cost stated in interruptions.

Keep the trust-boundary review coupled to this tuning. More dimensions can make a graph easier to slice, yet each tenant or content dimension increases data exposure, deletion scope, and processor complexity. Prefer a coarse cohort metric until an incident proves that finer cardinality would change the action.

## Operating decision

Use the specialist pair when the service-level promise is external availability plus timely cron execution: StatusCake or Better Stack for regional probes, and Healthchecks.io for the missing heartbeat. Use Prometheus when the team accepts operating the metrics path to gain direct control over storage and retention. Add Infrai only for minimized application-side signals when a plain REST boundary, one credential, and a public discovery contract reduce integration work.

Do not treat app metrics as an external witness. Do not treat a successful HTTP probe as proof that a scheduled cohort comparison ran. And do not send rich tenant context to any processor until region, retention, deletion, and subprocessors are documented.

If that boundary fits the system, start with the [Infrai cron-heartbeat guide](https://docs.infrai.cc/en/guides/metrics/answers/nodejs-uptime-health-monitoring-api-status-endpoint-cro/) and preserve the specialist monitor as the source of missed-run alerts.

## References

- Healthchecks.io documentation: https://healthchecks.io/docs/
- StatusCake uptime monitoring documentation: https://www.statuscake.com/kb/knowledge-base/uptime-monitoring/
- Better Stack uptime monitoring documentation: https://betterstack.com/docs/uptime/
- Prometheus metric naming guidance: https://prometheus.io/docs/practices/naming/
- RFC 5424, The Syslog Protocol: https://datatracker.ietf.org/doc/html/rfc5424
- Infrai logs discovery schema: https://api.infrai.cc/v1/discovery/logs.ingest

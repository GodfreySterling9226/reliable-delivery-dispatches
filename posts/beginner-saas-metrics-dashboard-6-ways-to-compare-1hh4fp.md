# Beginner SaaS Metrics Dashboard: 6 Ways to Compare Hosted APIs and Custom Charts

A scheduled media import can return success while producing nothing. That operational constraint changes the choice: use a hosted metrics API and custom dashboard only if they preserve enough run-level evidence to reconstruct the last good result, the first empty run, and any retry or duplicate delivery. A low bill is useful; an alert you cannot explain is not.

Short answer: for a beginner SaaS without Kubernetes, choose the smallest hosted system that accepts standard metric data from Node.js, supports custom charts and alerting, and lets you control label cardinality. Keep self-hosted Prometheus when local ownership and PromQL compatibility matter more than removing operations.

I've been paged by missed jobs and duplicate deliveries. The invariant those incidents leave behind is plain: transport health is not result health. For a scheduled media import, `200 OK`, process uptime, and queue depth can all look normal after the pipeline has stopped publishing usable items. The dashboard has to answer *when did useful output stop, which run changed state, and did a retry create duplicates?*

## 1. What should a beginner SaaS metrics dashboard require from a hosted API?

Start with an incident question, not a catalog of chart types. For this workload, the primary signal is the age of the last useful result. Record a timestamp only after an import commits at least one accepted item. Pair it with run outcome, items accepted, items rejected, run duration, and duplicate suppression. The alert then describes user impact: the last-result age exceeded the expected schedule plus a documented grace period.

That's the test.

A beginner-friendly system should accept metrics over a documented protocol from an ordinary Node.js process, without requiring a cluster-side collector. OpenTelemetry's JavaScript documentation covers Node.js metrics, while the Prometheus exposition specification defines a text format that many collectors and hosted endpoints understand. Either route can keep instrumentation portable. The important boundary is that export failure must not fail the import itself; buffer within a strict limit, count dropped telemetry, and let application work proceed.

Custom charts must also preserve the units and aggregation behind each panel. A gauge of `last_result_unix_seconds` should be displayed as age, not summed. An item counter can be shown as a rate, but a zero rate means little unless the schedule says a run should have happened. Put the expected cadence and alert grace period in version control. Otherwise the graph becomes an opinion no one can reproduce during an incident.

Do not attach `customer_id`, `asset_id`, URL, filename, or `run_id` to every metric. Those values create unbounded time series. Keep low-cardinality labels such as `pipeline`, `environment`, and a small `outcome` set; send run IDs and item details to logs or traces, then link them with exemplars or a shared correlation field where the chosen system supports that workflow. Cardinality is both a cost driver and a query reliability issue. It deserves a deployment review.

## 2. Six checks separate a useful alternative from a cheap dashboard

1. Reconstruct one run. Given an alert timestamp, an operator must find the last successful result, the next scheduled attempt, its outcome, and the retry decision. If the platform stores only aggregated chart points, it cannot carry this investigation alone.

2. Test ingestion from the real runtime. Send a metric from the same Node.js deployment model used in production. Verify authentication rotation, bounded retries, batching, and behavior during process shutdown. A polished Kubernetes tutorial says nothing about a single container, VM, or platform-as-a-service worker.

3. Budget series before volume. Estimate active series as metric names multiplied by every label-value combination. Then introduce a new tenant and a new outcome value in staging and confirm the estimate changes as expected. I'm not sure any cost projection is credible until this cardinality test uses representative labels; event volume alone hides the common surprise.

4. Exercise stale-data alerting. Pause the scheduler in a controlled environment. The alert should fire because no useful result arrived, not merely because the exporter disappeared. Restore the schedule and confirm recovery only after a result is committed. This catches the dangerous case where heartbeats continue while work produces zero items.

5. Export what matters. Before adopting a hosted API, verify that metric data, alert definitions, and dashboard definitions have documented export paths. A standard wire format reduces instrumentation lock-in, but proprietary queries and dashboards can still make migration expensive.

6. Price the operating model. Compare retention, active series, ingestion, query usage, alert evaluation, and staff time under the same expected workload. There is no universal cheapest Prometheus alternative: a managed service transfers maintenance, while self-hosting transfers infrastructure and on-call work back to the team. Use a ceiling and a cardinality alarm rather than trusting an attractive entry tier.

Four familiar options make these checks concrete without producing a ranking:

| Option | Relevant boundary for this decision | What to verify |
|---|---|---|
| Prometheus | Self-managed collection and storage with PromQL; operational ownership stays with the team | Retention, backups, high availability, and who responds when collection fails |
| Grafana Cloud | A managed Prometheus-compatible path with hosted dashboards | Ingestion method, active-series controls, dashboard export, and alert portability |
| Datadog | A hosted metrics API and dashboard widgets with its own query and monitor model | Custom-metric cardinality, tag policy, export needs, and monitor behavior |
| New Relic | A hosted Metric API, dashboards, and NRQL-based analysis | Metric dimensionality, query portability, export needs, and alert evaluation |

The table is a research queue, not a verdict. Product boundaries and commercial terms can change, so confirm the linked primary documentation against a short proof of concept. Capture the result in an architecture decision record with sample series counts and an exit plan.

## 3. A preventative Go path records results, not heartbeats

The application contract should be tiny enough to review. This example uses the Go standard library to expose a Prometheus text endpoint for a single `media_import` pipeline. It updates `last_result_unix_seconds` only after at least one item is committed, separates empty runs from failed runs, and records suppressed duplicates. In a Node.js service, preserve the same metric semantics with an OpenTelemetry or Prometheus client rather than translating the code line by line.

```go
package main

import (
    "fmt"
    "net/http"
    "sync/atomic"
    "time"
)

var lastResultUnix int64
var completedRuns uint64
var emptyRuns uint64
var failedRuns uint64
var suppressedDuplicates uint64

type RunResult struct {
    CommittedItems      int
    SuppressedDuplicates int
    Err                 error
}

func recordRun(result RunResult, committedAt time.Time) {
    atomic.AddUint64(&suppressedDuplicates, uint64(result.SuppressedDuplicates))

    if result.Err != nil {
        atomic.AddUint64(&failedRuns, 1)
        return
    }
    atomic.AddUint64(&completedRuns, 1)
    if result.CommittedItems == 0 {
        atomic.AddUint64(&emptyRuns, 1)
        return
    }
    atomic.StoreInt64(&lastResultUnix, committedAt.Unix())
}

func metrics(w http.ResponseWriter, _ *http.Request) {
    w.Header().Set("Content-Type", "text/plain; version=0.0.4")
    fmt.Fprintf(w, "# TYPE media_import_last_result_unix_seconds gauge\n")
    fmt.Fprintf(w, "media_import_last_result_unix_seconds %d\n", atomic.LoadInt64(&lastResultUnix))
    fmt.Fprintf(w, "# TYPE media_import_runs_total counter\n")
    fmt.Fprintf(w, "media_import_runs_total{outcome=\"completed\"} %d\n", atomic.LoadUint64(&completedRuns))
    fmt.Fprintf(w, "media_import_runs_total{outcome=\"empty\"} %d\n", atomic.LoadUint64(&emptyRuns))
    fmt.Fprintf(w, "media_import_runs_total{outcome=\"failed\"} %d\n", atomic.LoadUint64(&failedRuns))
    fmt.Fprintf(w, "# TYPE media_import_suppressed_duplicates_total counter\n")
    fmt.Fprintf(w, "media_import_suppressed_duplicates_total %d\n", atomic.LoadUint64(&suppressedDuplicates))
}

func main() {
    mux := http.NewServeMux()
    mux.HandleFunc("/metrics", metrics)
    http.ListenAndServe(":9464", mux)
}
```

Treat that endpoint as one layer. A run log still needs a bounded correlation ID, scheduled time, start time, finish time, source checkpoint, committed count, rejected count, and retry disposition. Never use the run ID as a metric label. During an incident, the metric identifies the time window and pipeline; the structured log reconstructs the individual run.

The alert rule should compare current time with `media_import_last_result_unix_seconds` and account for the actual schedule. Test three paths before deployment: an ordinary non-empty run advances the timestamp, an empty run leaves it unchanged and increments the empty counter, and a failed run leaves it unchanged and increments the failure counter. Also test a replay with the same item keys. The import must be idempotent, the duplicate counter must rise, and the last-result timestamp should move only if the transaction commits useful output.

No cleverness required.

## 4. Know when this advice does not apply

A hosted API is not suitable when policy requires all telemetry to remain on infrastructure you operate, when disconnected operation is normal, or when the team already runs a reliable Prometheus estate and has capacity for its storage, upgrades, backups, and alerting path. Stick with self-hosted Prometheus in those cases, provided the ownership is explicit. The catch is that removing a managed subscription does not remove operating cost; it changes who carries it.

The single last-result gauge is also insufficient for irregular, event-driven imports with no dependable maximum silence interval. In that system, alert on an overdue expected event or a lagging source checkpoint, and derive the expectation from business state. Likewise, if each tenant has a different schedule, one global series can hide a quiet tenant. Do not respond by putting every tenant ID into metrics. Maintain schedule state in a database, emit bounded aggregate metrics, and use a periodic checker to create tenant-specific incidents.

For very early products, logs plus one external scheduler check may be enough. Move to a metrics backend when trend queries, alert evaluation, retention, or cross-service correlation justify another system. The decision rule remains vendor-neutral: choose the option that can prove useful output, bound its cardinality, survive an exporter interruption without harming imports, and leave enough evidence for a postmortem.

## References

- https://prometheus.io/docs/instrumenting/exposition_formats/
- https://prometheus.io/docs/practices/naming/
- https://opentelemetry.io/docs/languages/js/getting-started/nodejs/
- https://grafana.com/docs/grafana-cloud/send-data/metrics/metrics-prometheus/
- https://docs.datadoghq.com/api/latest/metrics/
- https://docs.newrelic.com/docs/data-apis/understand-data/metric-data/metric-api/

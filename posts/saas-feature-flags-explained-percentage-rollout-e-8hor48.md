# SaaS Feature Flags Explained — Percentage Rollout Evidence for Scheduled Imports in 2026

For a staged SaaS percentage rollout, use a feature flag to return new scheduled imports to the established implementation when the enabled cohort stops completing expected windows. Preserve each import window's identity across that rollback, even if the scheduler reports successful dispatches. Otherwise recovery can replay financial records while the dashboard appears to improve.

TL;DR: Use stable percentage feature flags for EU and US tenants, record which implementation owns each scheduled window, and alert on missing durable completion evidence. Advance a staged SaaS rollout only when both regional cohorts complete their expected imports and a rollback rehearsal preserves the same work identities.

For a Node.js backend, the critical contract is the assignment passed from request handling into scheduled work. The example below uses Go for the scheduling boundary; any backend producing that assignment needs the same documented inputs and bucket calculation. The decision axis is rollback safety. A green request dashboard alone cannot establish it.

## How should a SaaS feature flag gate a percentage rollout for imports?

Start with the evidence required to close a window. Define a durable completion record identifying the tenant, source, scheduled window, committed outcome, and implementation revision. Commit that evidence with the corresponding business changes, or use a recovery protocol that can reconcile the two before declaring success. A dispatch acknowledgment should establish dispatch acceptance under your chosen protocol; do not treat it as evidence of a committed import.

Accept an explicit empty outcome when the source legitimately contains no new records. Silence is different. If the alert watches only imported row counts, a valid empty import and a stalled worker can look identical; if it watches only successful starts, an importer that never commits can look healthy. Require an outcome for every expected window, including empty ones.

Count completed windows.

For an illustrative fintech schedule, suppose a tenant expects an import every 15 minutes, with a five-minute completion allowance. A window due at 10:15 UTC has a deadline of 10:20 UTC. These are example policy values, not measured service behavior. Derive production allowances from the upstream delivery contract and observed completion times. Record the schedule timezone and exact window boundaries so a deployment cannot silently redefine what is overdue.

A missed deadline should leave enough evidence to distinguish absent source data, dispatch failure, exhausted retries, and a commit that succeeded before its acknowledgment was lost. That last case matters during rollback: retrying a write because the acknowledgment disappeared must not create another financial entry.

## Establish the denominator before changing the percentage

Define the eligible population as tenants with scheduled imports, then apply separate EU and US percentage gates. Use a tenant's authoritative region assignment. Don't infer the cohort from worker location or a transient request attribute. Tenant-level assignment keeps the import and its associated records together; user-level assignment would need a separate explanation for who owns shared financial data. Keep the denominator visible. An aggregate completion rate can hide an EU cohort with no completions if most expected windows belong to US tenants. Compare expected, completed, and overdue windows by region and assigned implementation, retaining the configuration revision in the diagnostic record. Include the control cohort. A source interruption affecting both cohorts calls for a different response from a regression isolated to the newly enabled importer.

Use a stable assignment across retries. For example, choose a fixed rollout key and map each tenant into 10,000 buckets, enabling buckets below the regional threshold. A 1,000-bucket threshold represents 10%; 5,000 represents 50%. Increasing the threshold preserves earlier assignments when the other inputs stay fixed. Changing the key, region, or tenant identifier changes that guarantee, so treat those changes as a new assignment policy.

The trade-off is longer exposure for early tenants in exchange for stable comparisons. Keep one key and review that exposure deliberately. Rerandomizing on every dispatch destroys the stable population needed to interpret missed windows and makes recovery harder to explain.

## Implement the gate without redefining the work

The following example calculates an assignment only. Its length-prefixed input encoding is part of the contract, as are SHA-256, the first eight digest bytes interpreted in big-endian order, and the modulo operation. A Node.js producer and a Go scheduler should share test vectors for that entire contract before exchanging assignments. Do not assume two implementations agree because both use a hash.

```go
package rollout

import (
    "crypto/sha256"
    "encoding/binary"
    "fmt"
)

func Enabled(rolloutKey, tenantID, region string, euBP, usBP int) (bool, error) {
    if rolloutKey == "" || tenantID == "" {
        return false, fmt.Errorf("missing rollout key or tenant ID")
    }
    if euBP < 0 || euBP > 10000 || usBP < 0 || usBP > 10000 {
        return false, fmt.Errorf("threshold outside 0..10000")
    }

    var threshold int
    switch region {
    case "EU":
        threshold = euBP
    case "US":
        threshold = usBP
    default:
        return false, fmt.Errorf("unsupported region %q", region)
    }

    input := fmt.Sprintf("%d:%s%d:%s%d:%s",
        len(rolloutKey), rolloutKey,
        len(tenantID), tenantID,
        len(region), region)
    digest := sha256.Sum256([]byte(input))
    bucket := binary.BigEndian.Uint64(digest[:8]) % 10000
    return bucket < uint64(threshold), nil
}
```

Specify UTF-8 byte lengths for the shared encoding; Go's string length supplies byte counts here. The percentage describes a partition of bucket space, not an exact percentage of tenants in a small population. This function is an assignment mechanism, not an authorization check.

An invalid configuration returns an error. Route that error into the scheduling failure path and alert on the missing assignment; do not interpret it as a valid decision to skip the import. A disabled flag should select the established importer for new windows. It should not disable the business obligation to import.

Persist the chosen assignment before dispatch. Separately define the work identity from tenant, source, and scheduled window, excluding importer version and rollout percentage. Enforce uniqueness for that identity at the durable write boundary, and make repeated attempts resolve to the already committed outcome. If a source can legitimately revise a closed window, model that revision explicitly instead of quietly bypassing deduplication.

This is where rollback safety costs engineering effort: both implementations must recognize the same completion evidence and business identities, even when they use different internal code. A flag cannot supply that compatibility.

## Verify the signal before paging on it

OpenTelemetry describes metrics as measurements captured at runtime. Use metrics to summarize the rollout while keeping durable window records available for reconciliation. An absent metric sample is not sufficient evidence that a financial import did not commit. Instrumentation can have its own delivery problems.

Publish bounded dimensions such as region, importer assignment, and outcome. Keep tenant and window identifiers in diagnostic records rather than making every window a metric label. This trades some dashboard detail for fewer metric series; the runbook must therefore explain how to retrieve the underlying overdue records.

Before enabling a production cohort, exercise these cases in a controlled environment:

- A valid empty import closes its window without a missing-result alert.
- A dispatched job that never commits crosses its deadline and identifies the affected region and assignment.
- A retry after a lost acknowledgment finds the existing result without duplicating financial entries.
- A threshold reduction during an active import preserves the recorded owner and work identity.
- Missing telemetry is distinguishable from a durable record showing an overdue window.

Check the cross-language bucket vectors during deployment verification, including non-ASCII tenant identifiers, thresholds of 0 and 10,000, and rejected regions. Also test who receives the alert and whether the responder can locate the window without reconstructing the release configuration from memory.

## Roll back ownership deliberately

On a cohort-specific regression, stop advancing the threshold and set the affected region's new-window threshold to zero. Keep the other region's decision explicit. Next inspect outstanding windows and their recorded owners, rather than assuming the flag change canceled running work.

Freeze new assignments.

Drain active work where safe. If recovery requires transferring an unfinished window to the established importer, use an ownership protocol that prevents the previous worker from committing afterward. Retain the original work identity and check for an existing committed outcome before attempting recovery. Restarting the same window under a new identity would defeat duplicate protection precisely when it is most needed. Reconcile every overdue window after recovery, including explicit empty outcomes. Then verify that the control path can still read records written by the candidate. Schema or output changes that break this compatibility require a migration and recovery plan before exposure; changing a percentage cannot reverse them.

Advance when expected windows close in both regional cohorts, the alert detects a deliberately withheld completion, and the rollback rehearsal produces no duplicate business writes. Write those acceptance conditions into the runbook before increasing exposure. They give the responder a decision rule when the scheduler is green and the imports have gone quiet.

## References

- OpenTelemetry, Metrics signal concepts: https://opentelemetry.io/docs/concepts/signals/metrics/
- Go standard library, crypto/sha256: https://pkg.go.dev/crypto/sha256
- Go standard library, encoding/binary: https://pkg.go.dev/encoding/binary
- Go language specification, String types: https://go.dev/ref/spec#String_types

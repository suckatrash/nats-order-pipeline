---
name: impact-analysis
description: Assess the blast radius of proposed NATS infrastructure changes
---

# Impact Analysis Skill

Assess the blast radius of proposed NATS infrastructure changes against live operational data in the Insights database.

**Prerequisite:** This skill depends on the `query-insights` skill for schema discovery and query mechanics. Use `insights db columns`, `insights db tables`, and `insights db macros` to verify column names and table structure before writing any query — never assume a column exists.

**When to use:** A user presents a proposed change to NATS infrastructure (stream config, account limits, consumer lifecycle, server topology) and wants to understand the operational impact before applying it. The change may arrive as a diff, a structured description, or natural language.

**One-shot operation:** Produce the complete report in a single pass. Do not ask clarifying questions or pause for confirmation at any step — when the input is ambiguous, record the interpretation (or the ambiguity itself) in the report and proceed with what the data can establish. This skill doubles as the system-prompt guidance for the `impact` CLI agent, which has no user to ask.

## Step 1: Understand the Change

Extract from the input:

- **Entity type** — stream, consumer, account, server
- **Entity name** — the human-readable name (e.g. `ORDERS`, `east-1`)
- **Account** — which account the entity belongs to (if applicable)
- **Cluster** — which cluster (if applicable)
- **Operation** — create, update, delete
- **Field changes** — what config fields are changing and to what values

### Interpreting diffs

The change may arrive in several formats. Parse the relevant fields based on file type:

**NATS server config (.conf)**
Look for `jetstream`, `max_payload`, `max_connections`, `authorization`, `accounts`, `cluster`, `leafnodes`, `gateway` blocks. Field changes in these blocks map directly to server or account-level config.

**nsc JSON (.json)**
Look for `nats.account.claims` fields: `limits.nats` (max_conn, max_subs, max_payload, max_data), `limits.jetstream` (mem_storage, disk_storage, max_streams, max_consumers), `exports`, `imports`.

**Terraform HCL (.tf)**
Look for `resource "jetstream_stream"`, `resource "jetstream_consumer"`, `resource "jetstream_kv"` blocks. Map HCL attribute names to NATS config fields (e.g. `replicas` -> `num_replicas`, `max_bytes`, `retention`).

**Helm values (.yaml)**
Look for NATS-related keys under the Helm chart's value structure: `nats.jetstream`, `nats.limits`, stream/consumer definitions.

**Application code (.go, .py, .js)**
Look for NATS client calls: `AddStream`, `Subscribe`, `Publish`, subject patterns, `MaxAckPending`, `AckWait`, connection URLs. Map these to the streams/consumers/subjects they reference.

### Structured descriptions

If the user provides a structured change (entity type, name, operation, field diffs), use it directly.

### Natural language

If the user describes the change in prose ("I want to reduce replicas on ORDERS from 3 to 1"), extract the same fields.

### State your interpretation

Do not pause to confirm. Open the report's Change Summary with the interpretation, stated as fact:

> **Change:** Update stream `ORDERS` in account `PROD` — `num_replicas`: 3 -> 1

If part of the diff cannot be confidently interpreted, say so in the Change Summary and analyze the parts that can.

## Step 2: Resolve Entities

Map the change to database entities via `_ident` tables. Use the entity name and any account/cluster hints to find the `pk`.

```sql
-- Find a stream by name
SELECT si.pk, si.name, ai.name AS account
FROM hx.stream_ident si
JOIN hx.account_ident ai ON ai.pk = si.account_pk
WHERE si.name ILIKE '%<name>%'

-- Find a server by name
SELECT pk, name, version, cluster FROM hx.server_ident WHERE name ILIKE '%<name>%'

-- Find an account by name
SELECT pk, name FROM hx.account_ident WHERE name ILIKE '%<name>%'

-- Find a consumer by name (needs stream context)
SELECT ci.pk, ci.name, si.name AS stream, ai.name AS account
FROM hx.consumer_ident ci
JOIN hx.stream_ident si ON si.pk = ci.stream_pk
JOIN hx.account_ident ai ON ai.pk = si.account_pk
WHERE ci.name ILIKE '%<name>%'
```

**Handle ambiguity:** The same stream name can exist in multiple accounts. If multiple matches are returned and neither the diff nor surrounding context (account/cluster hints, repo config) disambiguates, do not guess: report the entity as unresolved, list the candidates, and emit findings only for entities that resolve unambiguously. Always capture the `pk` for all subsequent queries.

## Step 3: Get Current State

Query the latest config and metrics for the resolved entity. This is the "before" picture. Cross-validate any "from" values in the diff against what the database shows — if they diverge, flag it.

### Stream

```sql
-- Current config
SELECT subjects, retention_policy, discard_policy, cluster,
       max_msgs, max_bytes, max_age, num_replicas, max_consumers, sealed
FROM hx.stream_opts
WHERE stream_pk = <pk>
ORDER BY epoch DESC LIMIT 1

-- Current leader metrics
SELECT msgs, bytes, num_consumers, num_subjects, lag
FROM hx.stream_replica_stats
WHERE stream_pk = <pk> AND is_leader
ORDER BY epoch DESC LIMIT 1
```

### Account

```sql
-- Current limits
SELECT max_conn, max_subs, js_mem_storage, js_disk_storage,
       js_max_streams, js_max_consumers, js_max_ack_pending
FROM hx.account_opts
WHERE account_pk = <pk>
ORDER BY epoch DESC LIMIT 1

-- Current usage (aggregated across servers)
SELECT sum(conns) AS conns, sum(msgs_sent) AS msgs_sent,
       sum(bytes_sent) AS bytes_sent
FROM hx.account_stats
WHERE account_pk = <pk>
  AND epoch = (SELECT max(epoch) FROM hx.account_stats WHERE account_pk = <pk>)
```

### Server

```sql
-- Current metrics
SELECT memory, cpu, connections, slow_consumers,
       in_msgs, out_msgs, in_bytes, out_bytes,
       js_memory, js_storage, js_reserved_memory, js_reserved_storage
FROM hx.server_stats
WHERE server_pk = <pk>
ORDER BY epoch DESC LIMIT 1
```

### Consumer

```sql
-- Current config
SELECT deliver_policy, ack_policy, filter_subjects,
       ack_wait, max_deliver, max_ack_pending, num_replicas
FROM hx.consumer_opts
WHERE consumer_pk = <pk>
ORDER BY epoch DESC LIMIT 1

-- Current leader metrics
SELECT lag, num_ack_pending, num_redelivered, num_waiting, num_pending
FROM hx.consumer_replica_stats
WHERE consumer_pk = <pk> AND is_leader
ORDER BY epoch DESC LIMIT 1
```

## Step 4: Expand the Blast Radius

Walk the entity dependency graph outward from the changed entity. Use the FK relationships documented in the `query-insights` skill's entity relationships section.

| Starting entity | Target | Join path |
|---|---|---|
| Stream | Consumers | `consumer_ident.stream_pk = stream_ident.pk` |
| Stream | Account | `stream_ident.account_pk = account_ident.pk` |
| Stream | Servers (replicas) | `stream_replica_stats.server_pk = server_ident.pk` |
| Server | Streams (hosted) | `stream_replica_stats.server_pk = server_ident.pk` |
| Server | Connections | `conn_ident.server_pk = server_ident.pk` |
| Account | Streams | `stream_ident.account_pk = account_ident.pk` |
| Account | Connections | `conn_ident.account_pk = account_ident.pk` |
| Account | Exports/Imports | `account_export_opts` / `account_import_opts` |
| Consumer | Stream (parent) | `consumer_ident.stream_pk = stream_ident.pk` |

### Cross-account impact

If the changed entity is exported to other accounts, those importing accounts are also in the blast radius.

```sql
-- Find exports from the affected account
SELECT subject, type FROM hx.account_export_opts
WHERE account_pk = <account_pk>
ORDER BY epoch DESC

-- Find imports that reference the affected account
SELECT ai.name AS importing_account, imp.subject, imp.local_subject
FROM hx.account_import_opts imp
JOIN hx.account_ident ai ON ai.pk = imp.account_pk
WHERE imp.account = '<exporting_account_name>'
ORDER BY imp.epoch DESC
```

### Server removal — replica redistribution

When a server is being removed, find every stream with a replica on it and check whether the remaining cluster has capacity.

```sql
-- Streams with replicas on the server being removed
SELECT DISTINCT si.pk, si.name, ai.name AS account, so.num_replicas
FROM hx.stream_replica_stats rs
JOIN hx.stream_ident si ON si.pk = rs.stream_pk
JOIN hx.account_ident ai ON ai.pk = si.account_pk
JOIN hx.stream_opts so ON so.stream_pk = rs.stream_pk
WHERE rs.server_pk = <server_pk>
  AND rs.epoch = (SELECT max(epoch) FROM hx.stream_replica_stats)
  AND so.epoch = (SELECT max(epoch) FROM hx.stream_opts WHERE stream_pk = rs.stream_pk)

-- Other servers in the same cluster (candidates for redistribution)
SELECT si.pk, si.name,
       ss.js_memory, ss.js_storage, ss.js_reserved_memory, ss.js_reserved_storage,
       ss.connections
FROM hx.server_ident si
JOIN hx.server_stats ss ON ss.server_pk = si.pk
WHERE si.cluster = '<cluster_name>'
  AND si.pk != <removed_server_pk>
  AND ss.epoch = (SELECT max(epoch) FROM hx.server_stats)
```

## Step 5: Measure Operational Impact

For every entity in the blast radius, pull current metrics and translate counts into operational impact.

### Consumer throughput and health per stream

```sql
SELECT ci.name AS consumer,
       crs.lag, crs.num_ack_pending, crs.num_redelivered, crs.num_waiting
FROM hx.consumer_replica_stats crs
JOIN hx.consumer_ident ci ON ci.pk = crs.consumer_pk
WHERE ci.stream_pk = <stream_pk> AND crs.is_leader
  AND crs.epoch = (SELECT max(epoch) FROM hx.consumer_replica_stats)
```

### Server capacity headroom

```sql
SELECT js_memory, js_storage, js_reserved_memory, js_reserved_storage,
       connections, memory, cpu
FROM hx.server_stats
WHERE server_pk = <server_pk>
ORDER BY epoch DESC LIMIT 1
```

### Storage trends (growth rate estimation)

Compare storage across recent epochs to estimate growth rate. Use at least 1 hour of data for a meaningful trend.

```sql
SELECT epoch, bytes
FROM hx.stream_replica_stats
WHERE stream_pk = <pk> AND is_leader
  AND epoch >= (SELECT max(epoch) - INTERVAL '1 hour' FROM hx.stream_replica_stats WHERE stream_pk = <pk>)
ORDER BY epoch
```

Compute the rate from the delta between earliest and latest values divided by the time span.

### Connection count on a server

```sql
SELECT count(*) AS active_connections
FROM hx.conn_stats cs
JOIN hx.conn_ident ci ON ci.pk = cs.conn_pk
WHERE ci.server_pk = <server_pk>
  AND cs.epoch = (SELECT max(epoch) FROM hx.conn_stats)
  AND cs.stop_time = '0001-01-01T00:00:00Z'
```

## Step 6: Check Existing Issues

Query `hx.check_results` for all entity PKs in the blast radius. Pre-existing issues that the change might exacerbate are critical context.

```sql
-- Direct check: findings on the changed entity
SELECT code, severity, entity_type, entity_key
FROM hx.check_results
WHERE entity_pk = <pk>
  AND epoch = (SELECT max(epoch) FROM hx.server_stats)
ORDER BY severity DESC, code

-- Broad check: findings on all affected entities
SELECT code, severity, entity_type, entity_pk, entity_key
FROM hx.check_results
WHERE entity_pk IN (<all_affected_pks>)
  AND epoch = (SELECT max(epoch) FROM hx.server_stats)
ORDER BY severity DESC, code
```

For richer output, use the `audit.*` macros (discover signatures with `insights db macros --schema audit`). Audit macros resolve entity names automatically, avoiding manual ident JOINs.

## Step 7: Evaluate the Finding Catalog

Findings are restricted to a **fixed catalog**. Emit a finding only when its required evidence is present in data you actually queried — no measurement, no finding. This is a checklist, not a brainstorm.

| Code | Finding | Required evidence |
|---|---|---|
| `DATA_LOSS` | Change destroys unconsumed messages | Deleted stream/consumer with a consumer at `lag > 0` or `num_pending > 0`; or retention tightened (`max_bytes`/`max_age`/`max_msgs`) below what lagging consumers have not yet processed — all from latest-epoch replica stats |
| `LIMIT_VIOLATION` | New limit is below current measured usage | Proposed limit < current usage at the latest epoch (e.g. `max_bytes` 1 GiB vs 1.2 GiB stored; `max_conn` 100 vs 140 active). Applying the change fails or truncates immediately |
| `HEADROOM_EXHAUSTION` | New limit leaves operationally meaningless headroom | Current usage plus measured growth rate crosses the proposed limit within a short horizon (default < 24h), computed from at least 1h of trend data |
| `FT_LOSS` | Fault tolerance eliminated or unsatisfiable | Replicas reduced to 1 on a stream with active consumers; or `num_replicas` unsatisfiable with remaining servers after a server removal; or removed server is the only gateway/leafnode path |
| `CAPACITY_EXCEEDED` | Remaining infrastructure cannot absorb shifted load | Measured reserved storage/memory on remaining servers is insufficient for the replicas/connections being displaced |
| `BROKEN_IMPORT` | Cross-account dependency severed | Deleted/renamed export whose subject overlaps an active import in another account, both from current opts tables |
| `UNRESOLVED_ENTITY` | Change references an entity the data cannot find | Entity name extracted from the diff with no match (or multiple undisambiguable matches) in the database |

Rules:

- **Every finding cites its evidence**: the queried values, the epoch they came from, and the query that produced them. Cite the query **verbatim as executed** — citations are checked against the run's execution log, and evidence citing a query that was never run is rejected.
- **Freshness gate**: evidence must come from the latest available epoch. If the newest data is stale (older than ~10m, or the configured bound), suppress the finding and note the degraded data instead.
- **Ambiguity gate**: no findings against ambiguous entities — surface the ambiguity as `UNRESOLVED_ENTITY` with the candidate list.
- **No speculation**: observations that don't meet a catalog entry's bar (code-style concerns, "this might be risky", missing DLQs on unaffected paths) are not findings. At most they go in a non-scored **Notes** section, and only when directly related to a changed entity. Notes carry evidence exactly like findings: `file:line` for repo observations, query + value + epoch for data observations. A note the reader can't verify is dropped.
- Pre-existing audit check results on affected entities are reported as **context**, not as findings of this analysis.

**Risk level is derived mechanically** from the findings, never assigned by judgment: `critical` if any `DATA_LOSS`/`LIMIT_VIOLATION`/`CAPACITY_EXCEEDED`; `high` for `FT_LOSS`/`BROKEN_IMPORT`; `medium` for `HEADROOM_EXHAUSTION`; `low` when no findings (`UNRESOLVED_ENTITY` alone does not raise the level, but must be prominent in the report).

## Change-Type Playbooks

Each playbook gathers the evidence needed to evaluate the finding catalog for that change type — the "key risk signals" map onto catalog codes. Run the playbook's queries, then test each catalog entry against the results.

### Delete stream

1. Count consumers on the stream: `SELECT ci.pk, ci.name FROM hx.consumer_ident ci WHERE ci.stream_pk = <pk>`
2. Measure consumer throughput: query `consumer_replica_stats` for lag, `num_ack_pending`, `num_redelivered` per consumer
3. Check cross-account imports: query `account_import_opts` for imports referencing the stream's subjects
4. Check current message count and storage: `msgs`, `bytes` from `stream_replica_stats`
5. Check for unprocessed messages: consumers with `lag > 0` will lose those messages
6. Check existing audit issues on the stream and its consumers

**Key risk signals:** Active consumers with non-zero throughput, cross-account imports, consumers with lag > 0, high message count.

### Reduce replicas

1. Current replica set: query `stream_replica_stats` for all servers hosting replicas (`server_pk`, `is_leader`, `is_offline`, `lag`)
2. Stream size: `bytes` from `stream_replica_stats` (leader)
3. Server health: query `server_health_stats` for each server in the replica set
4. Cluster server count: how many servers remain vs `num_replicas` after the change
5. Per-server JetStream load: `js_memory`, `js_storage`, `js_reserved_memory`, `js_reserved_storage` from `server_stats` for servers retaining replicas
6. Check existing audit issues on the stream

**Key risk signals:** Reducing to R1 eliminates fault tolerance, remaining servers near capacity, any replica currently offline or lagging, large stream size increases re-sync cost on recovery.

### Lower account limits

1. Current usage vs proposed limit: compare `account_stats` aggregated values against the new limit
2. Current limits vs proposed: compare `account_opts` existing limit against the new value
3. Stream/consumer counts in the account: count entities in `stream_ident` / `consumer_ident` where `account_pk` matches

**Key risk signals:** Current usage already exceeds the proposed limit (change will immediately fail or reject new operations), usage trending toward the new limit, `-1` (unlimited) being replaced with a finite value.

### Change retention

1. Current storage: `bytes` from `stream_replica_stats` (leader)
2. Current retention config: `retention_policy`, `max_bytes`, `max_age`, `max_msgs` from `stream_opts`
3. Consumer lag: query all consumers on the stream for `lag` and `num_pending` — if retention tightens, lagging consumers may lose unprocessed messages
4. Storage growth rate: compare `bytes` across recent epochs to estimate when the new limit would be hit

**Key risk signals:** Consumer lag exceeds the new retention window (messages purged before consumption), current storage exceeds new `max_bytes` (immediate truncation), high ingestion rate with tight retention creates narrow replay window.

### Remove server

1. Streams with replicas on the server: query `stream_replica_stats` for all `stream_pk` values on the server
2. Connection count: query `conn_ident` / `conn_stats` for active connections on the server
3. Remaining cluster capacity: query `server_stats` for all other servers in the cluster — compare `js_memory`/`js_storage` headroom
4. Replica redistribution: for each affected stream, check if `num_replicas` can still be satisfied with remaining servers
5. Server health: query `server_health_stats` for the server being removed and remaining servers
6. Gateway/leafnode topology: check if the server has unique gateway or leafnode connections via `server_ident`

**Key risk signals:** Streams that cannot meet `num_replicas` with remaining servers, remaining servers near capacity, high connection count (clients must reconnect), server is the only gateway to a remote cluster.

### Change max-ack-pending

1. Current `max_ack_pending` setting: from `consumer_opts`
2. Current `num_ack_pending`: from `consumer_replica_stats` (leader)
3. Consumer throughput: compare `delivered_consumer_seq` across recent epochs for delivery rate
4. Ack wait and redelivery: `ack_wait`, `max_deliver` from `consumer_opts`, `num_redelivered` from `consumer_replica_stats`

**Key risk signals:** Lowering below current `num_ack_pending` causes immediate back-pressure, high redelivery rate suggests processing issues that lower pending limits will worsen, consumers with long `ack_wait` need proportionally higher pending limits.

### Delete consumer

1. Parent stream: `stream_pk` from `consumer_ident`, then stream name and account
2. Unprocessed messages: `lag` and `num_pending` from `consumer_replica_stats` (leader)
3. Current delivery state: `num_ack_pending`, `num_redelivered` from `consumer_replica_stats`
4. Sibling consumers: other consumers on the same stream (are they covering the same subjects?)
5. Check existing audit issues on the consumer

**Key risk signals:** Consumer has unprocessed messages (`lag > 0`), consumer is the only subscriber for its filter subjects, no dead-letter or retry mechanism evident.

## Output Format

Structure the final report as follows:

```
## Impact Analysis

**Change:** <one-line interpretation of the change>
**Risk: <LOW | MEDIUM | HIGH | CRITICAL>** · data epoch <timestamp>

### Findings

<Numbered list. Each entry: **CODE** — one-sentence summary, then an
Evidence: line citing the queried values, epoch, and query.>

(If none: "No catalog findings — current operational data does not
substantiate the risk conditions this analysis checks for.")

### Affected Entities

| Entity | Type | Account | Relationship |
|---|---|---|---|
| ... | ... | ... | ... |

### Existing Audit Issues (context)

| Code | Severity | Entity |
|---|---|---|
| ... | ... | ... |

(If none: "No active audit issues on affected entities.")

### Recommendations

<Numbered list of specific, actionable recommendations, each tied to a
finding by number. No findings -> no recommendations section.>

### Notes

<Non-scored observations directly related to changed entities, each with
its own evidence (file:line, or query + value + epoch). Omit if empty.>
```

The risk line must match the mechanical derivation from Step 7 — never adjust it editorially.

## Common Pitfalls

- **Schema qualification:** Always use `hx.` prefix — `stream_opts` alone will error; write `hx.stream_opts`
- **Leader filtering:** Stream and consumer metrics are per-replica. Filter `is_leader = true` for authoritative counts; omit the filter when you need the full replica set
- **Counters vs gauges:** `in_msgs`, `out_msgs`, `bytes_sent`, `bytes_recv` are counters (diff across epochs for rates). `memory`, `connections`, `lag`, `num_ack_pending` are gauges (latest value is meaningful)
- **`-1` means unlimited:** Limit fields use `-1` for "no limit." When comparing usage against limits, treat `-1` as no constraint. When a change replaces `-1` with a finite value, flag it
- **Name ambiguity:** The same stream/consumer name can exist in multiple accounts. Always resolve via `_ident` tables with account context. If ambiguous, report the candidates as `UNRESOLVED_ENTITY` rather than guessing or asking
- **`account_stats` is per-server:** Each row has a `server_pk`. To get account-level totals, `SUM()` grouped by `account_pk` at a single epoch
- **Epoch sourcing:** Always source the current epoch from a `_stats` table: `(SELECT max(epoch) FROM hx.<entity>_stats)`. Different entity types may have different latest epochs
- **`_opts` deduplication:** Rows in `_opts` tables are deduplicated by hash. A new row means something changed. Multiple identical rows at consecutive epochs are collapsed — so `ORDER BY epoch DESC LIMIT 1` gives the current config, not the config at every epoch

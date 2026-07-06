---
name: impact-analysis
description: Assess the blast radius of proposed NATS infrastructure changes
---

# Impact Analysis Skill

Assess the blast radius of proposed NATS infrastructure changes against live operational data in the Insights database.

**Prerequisite:** This skill depends on the `query-insights` skill for schema discovery and query mechanics. Use `insights db columns`, `insights db tables`, and `insights db macros` to verify column names and table structure before writing any query — never assume a column exists.

**When to use:** A user presents a proposed change to NATS infrastructure (stream config, account limits, consumer lifecycle, server topology) and wants to understand the operational impact before applying it. The change may arrive as a diff, a structured description, or natural language.

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

### Confirm understanding

Before proceeding, summarize what you understood:

> **Change:** Update stream `ORDERS` in account `PROD` — `num_replicas`: 3 -> 1
>
> Does this capture the change correctly?

Wait for confirmation before continuing.

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

**Handle ambiguity:** The same stream name can exist in multiple accounts. If multiple matches are returned, ask the user to clarify which account/cluster. Always capture the `pk` for all subsequent queries.

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

## Step 7: Assess and Synthesize

Assign a risk level based on what you found:

| Risk level | Criteria |
|---|---|
| **Low** | No active consumers affected, no cross-account dependencies, ample capacity headroom, no existing audit issues |
| **Medium** | Active consumers affected but with low throughput/lag, sufficient capacity remains, minor existing audit issues |
| **High** | High-throughput consumers affected, cross-account dependencies exist, limited capacity headroom, significant existing audit issues |
| **Critical** | Data loss likely (retention truncation with lagging consumers), single point of failure created (replicas reduced to 1), capacity exceeded, production-impacting existing issues |

## Change-Type Playbooks

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

Structure the final assessment as follows:

```
## Change Summary

<1-2 sentence description of what is being changed>

## Affected Entities

| Entity | Type | Account | Current State | Impact |
|---|---|---|---|---|
| ... | ... | ... | ... | ... |

## Findings

<Numbered list of specific observations from the data, each with supporting metrics>

## Existing Audit Issues

| Code | Severity | Entity | Description |
|---|---|---|---|
| ... | ... | ... | ... |

(If none: "No active audit issues found for affected entities.")

## Risk Level: <LOW | MEDIUM | HIGH | CRITICAL>

<1-2 sentence rationale referencing specific findings>

## Recommendations

<Numbered list of specific, actionable recommendations>

## Deployment Timing

<Based on usage patterns if available, or "Insufficient data for timing recommendation">
```

## Common Pitfalls

- **Schema qualification:** Always use `hx.` prefix — `stream_opts` alone will error; write `hx.stream_opts`
- **Leader filtering:** Stream and consumer metrics are per-replica. Filter `is_leader = true` for authoritative counts; omit the filter when you need the full replica set
- **Counters vs gauges:** `in_msgs`, `out_msgs`, `bytes_sent`, `bytes_recv` are counters (diff across epochs for rates). `memory`, `connections`, `lag`, `num_ack_pending` are gauges (latest value is meaningful)
- **`-1` means unlimited:** Limit fields use `-1` for "no limit." When comparing usage against limits, treat `-1` as no constraint. When a change replaces `-1` with a finite value, flag it
- **Name ambiguity:** The same stream/consumer name can exist in multiple accounts. Always resolve via `_ident` tables with account context. If ambiguous, ask the user
- **`account_stats` is per-server:** Each row has a `server_pk`. To get account-level totals, `SUM()` grouped by `account_pk` at a single epoch
- **Epoch sourcing:** Always source the current epoch from a `_stats` table: `(SELECT max(epoch) FROM hx.<entity>_stats)`. Different entity types may have different latest epochs
- **`_opts` deduplication:** Rows in `_opts` tables are deduplicated by hash. A new row means something changed. Multiple identical rows at consecutive epochs are collapsed — so `ORDER BY epoch DESC LIMIT 1` gives the current config, not the config at every epoch

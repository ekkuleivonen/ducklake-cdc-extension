# Producer outbox metadata convention

> **Status: convention, not enforcement.** This document recommends a
> JSON shape for the `extra_info` argument of
> `CALL <lake>.set_commit_message(author, message, extra_info => ...)`.
> The extension does not validate or enforce the shape; reference sinks
> (Phase 1 stdout / file / webhook; Phase 5 Kafka / Postgres-mirror /
> Redis Streams) do read it. Documenting the recommended shape now —
> in Phase 0, before any sink ships — is what prevents the ecosystem
> from fragmenting into per-app JSON conventions before there's
> anything to be compatible with.

## Why a recommended shape exists

DuckLake's `set_commit_message` is the producer-side outbox primitive
(ADR 0004 row 4): producers stamp every commit with `(author, message,
extra_info)` and consumers read those three columns alongside every
event via `cdc_events`, `cdc_changes`, and `cdc_ddl`. The first two
fields have an obvious shape. `extra_info` is `JSON` and could carry
anything, which means without a recommendation:

- Every producer team invents its own `{"event": "...", ...}` schema.
- Every reference sink learns N schemas to do the obvious thing
  ("route on event name; propagate trace id").
- Schema-registry integration (Confluent / Apicurio) has no canonical
  subject-name field to bind to.
- Distributed-tracing integration (OpenTelemetry, Datadog APM) has no
  canonical trace-id field to lift into spans.

The cost of saying "here's what we recommend" in Phase 0 is one page.
The cost of saying it after sinks ship is a deprecation cycle.

## The recommended shape

```json
{
  "event":  "OrderPlaced",
  "trace_id": "01HXC4Z3M7P2W8K9N6Q5T1R0AB",
  "schema": "orderplaced.v1",
  "actor":  "user:42",
  "extra":  { /* any application-specific payload */ }
}
```

### Field semantics

- **`event`** *(string; only recommended-required field)* — the
  application-meaningful event name. Reference sinks (webhook, Kafka)
  route on this. Convention is `PascalCase` for the event name itself
  (`OrderPlaced`, `InventoryReserved`) — this matches how OLTP outbox
  patterns label messages and reads naturally on dashboards.
- **`trace_id`** *(string; optional)* — the upstream distributed-trace
  identifier. ULID, UUID, or W3C trace-context format are all
  acceptable; the project does not pick one. Reference sinks
  propagate it (HTTP header on webhook, Kafka header, span tag in
  binding-side instrumentation) when present. Bindings (Phase 3 / 4)
  surface it on the `Event` model as `event.trace_id` so application
  code can stitch spans without parsing the JSON.
- **`schema`** *(string; optional)* — the schema-version identifier,
  in the form `<event_name>.v<integer>` (case-insensitive matching
  on the event prefix). The Confluent-Schema-Registry / Apicurio
  reference sink uses this verbatim as the subject name when
  registering or looking up schemas. Producers that don't ship to a
  schema registry can omit it.
- **`actor`** *(string; optional)* — the principal that triggered the
  commit, in the form `<kind>:<id>` (e.g. `user:42`,
  `service:order-api`, `system:nightly-batch`). Reference sinks
  surface it on dashboards; audit-logging sinks index on it. The
  `<kind>` is free-form but the `kind:id` shape is the pattern that
  makes downstream filtering tractable.
- **`extra`** *(object; optional)* — escape hatch for
  application-specific payload. Reference sinks pass it through
  untouched. Bindings expose it as a typed `dict` (Python) /
  `map[string]any` (Go) / native `Json` (Rust, future).

### What we deliberately don't include

- **Timestamps.** DuckLake already stamps every snapshot with
  `snapshot_time`; bindings carry it on every event row (per ADR
  0008's wall-clock-time consistency rule). Adding `extra_info.ts`
  would be redundant and would invite drift between the application
  clock and the catalog clock.
- **Snapshot id.** Same reason — `cdc_events` / `cdc_changes` /
  `cdc_ddl` all carry `snapshot_id`. Producers that include
  `extra_info.snapshot_id` are signalling a misunderstanding of where
  the canonical id lives; the doc names this loudly.
- **Lake / catalog identifier.** Multi-lake routing is a binding-side
  concern (the consumer subscribes to one lake at a time;
  cross-lake fan-out is a Phase 5 reference-sink discussion).

## Producer-side example

```sql
-- Within the producing application's transaction:
INSERT INTO orders (id, customer_id, total) VALUES (1234, 42, 99.99);
CALL my_lake.set_commit_message(
    'order-api',
    'create order 1234',
    extra_info => '{
        "event":    "OrderPlaced",
        "trace_id": "01HXC4Z3M7P2W8K9N6Q5T1R0AB",
        "schema":   "orderplaced.v1",
        "actor":    "user:42",
        "extra":    {"channel": "web", "ab_bucket": "B"}
    }'
);
COMMIT;
```

## Binding-side example

The Phase 3 / Phase 4 helpers (`ducklake_cdc.outbox(...)` /
`ducklakecdc.WithOutbox(...)`) accept these fields as named arguments
and serialize them. Callers can still pass arbitrary JSON via `extra`;
the project does not enforce the shape, it recommends it.

```python
# Phase 3 binding helper (forward-looking)
import ducklake_cdc as cdc

with cdc.producer("ducklake:my.ducklake") as p:
    p.commit(
        author="order-api",
        message="create order 1234",
        outbox=cdc.outbox(
            event="OrderPlaced",
            trace_id="01HXC4Z3M7P2W8K9N6Q5T1R0AB",
            schema="orderplaced.v1",
            actor="user:42",
            extra={"channel": "web", "ab_bucket": "B"},
        ),
    )
```

## Consumer-side example

```python
# Phase 3 binding consumer (forward-looking)
with cdc.consumer("ducklake:my.ducklake", name="order-fanout") as c:
    for batch in c.poll():
        for ev in batch.dml:
            outbox = ev.commit_extra_info  # parsed JSON dict
            event_name = outbox.get("event")
            trace_id   = outbox.get("trace_id")
            if event_name == "OrderPlaced":
                route_to_kafka(ev, topic="orders", trace_id=trace_id)
        c.commit(batch.next_snapshot_id)
```

## Reference-sink behaviour summary

For sinks that ship in v0.1 (Phase 1) and v0.2 (Phase 5):

| Sink | Honors `event` | Honors `trace_id` | Honors `schema` | Honors `actor` | Pass-through `extra` |
| --- | --- | --- | --- | --- | --- |
| stdout JSONL (Phase 1) | yes — printed alongside row | yes — `trace_id` field on the line | no — printed in the `extra_info` blob | no — printed in the `extra_info` blob | yes |
| file JSONL (Phase 1) | same as stdout | same as stdout | same as stdout | same as stdout | yes |
| webhook (Phase 1) | routes on `event` if `routes:` config provided; passes verbatim otherwise | added as `X-Trace-Id` HTTP header | not used (webhooks don't speak schema registry) | added as `X-Actor` HTTP header | yes — body |
| Kafka (Phase 5) | used as topic suffix if `routes:` config provided; otherwise routed by `cdc.topic` table tag | added as Kafka header `trace_id` | resolved to schema registry subject; payload registered against `<schema>` subject | added as Kafka header `actor` | yes — value payload |
| Postgres mirror (Phase 5) | ignored (table-shape mirror; event name not used) | logged in the mirror's `_meta` JSONB column | ignored | logged in `_meta` | logged in `_meta` |
| Redis Streams (Phase 5) | used as stream-name suffix if `routes:` config provided | added as a stream field `trace_id` | ignored | added as stream field `actor` | yes — stream fields |

Sinks that don't recognise a key surface it via the catch-all
`extra_info` blob; nothing is silently dropped.

## Validation discipline (consumer side)

Per `docs/security.md` § "`set_commit_message` `extra_info` is
consumer-trusted input": downstream sinks must **treat it as
untrusted**:

- **Validate JSON shape.** A producer can write
  `extra_info => 'not-json'`; the bindings parse it and surface a
  `MalformedExtraInfo` warning rather than crashing the consumer
  loop.
- **Length-limit before logging.** Pathological payloads
  (`extra_info => '"' || repeat('A', 1_000_000) || '"'`) are
  legal SQL; sinks truncate before stamping into log lines or
  forwarding to systems with payload-size limits (Slack, PagerDuty).
- **Escape before rendering.** Webhook reference sinks that template
  `event` into URL paths must URL-encode it; HTML rendering must
  HTML-escape; `psql`-style logs must not interpret backslash
  escapes.

These are sink-side rules, not extension-side rules. The extension
exposes the value verbatim per `docs/roadmap/README.md`
(serializers live in clients).

## See also

- ADR 0004 row 4 — `set_commit_message` is `use-directly`; this
  document is the convention layered on top.
- `docs/api.md#producer-outbox-metadata` — entry point.
- `docs/security.md` — threat model rules for `extra_info`.
- `docs/roadmap/README.md` — pillar 10 (serializers in clients), pillar 11
  (filtering is consumer policy).
- DuckLake spec — Snapshots § `commit_message`, `commit_extra_info`,
  `author`.

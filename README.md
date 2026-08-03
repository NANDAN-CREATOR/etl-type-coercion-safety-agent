# Day 18 — ETL Transform Data Type Coercion Safety Agent

**Series:** Agentic AI in Data Engineering — ETL Edition  
**Domain:** Transform  
**Pattern:** Pre-cast validation gate — verify that planned type coercions are safe across the actual data in the current batch before applying them.  
**Core guardrail:** Never apply a type coercion that hasn't been validated against the actual data distribution. A schema-valid cast that fails on even one row can silently drop that row or raise a runtime error that kills the entire batch.

---

## The Problem

Type coercions look fine in schema definitions and pass unit tests, but fail on real production data because a "numeric" column has a legacy string ID mixed in, a boolean column uses Y/N encoding but the target expects TRUE/FALSE, or a timestamp column has mixed format variants that some SQL engines won't handle uniformly. The engine either drops the row silently (Spark default) or kills the batch (Snowflake).

---

## What the Agent Does

1. **Fetches a column sample** from the current batch  
2. **Checks format consistency** for timestamp/date — detects mixed format variants  
3. **Validates cast safety** — tests every sampled value against a strict type validator  
4. **Approves, blocks, or escalates**

---

## Core Guardrail

> If ANY sampled value fails the validator, the coercion is blocked.  
> Silent row drops are worse than batch failures — they produce incorrect aggregates with no error signal.

---

## Scenarios

| # | Column | Cast | Outcome |
|---|--------|------|---------|
| 1 | `orders.order_amount` | VARCHAR → DECIMAL | ✅ APPROVED |
| 2 | `orders.customer_id` | VARCHAR → INTEGER | 🚫 BLOCKED — `CID-LEGACY-004` fails |
| 3 | `orders.is_priority` | VARCHAR → BOOLEAN | 🚫 BLOCKED — Y/N not valid BOOLEAN |
| 4 | `transactions.event_ts` | VARCHAR → TIMESTAMP | 🚫 BLOCKED — FORMAT_FRAGMENTATION before cast |

---

## How to Run

```bash
python agent.py
```

---

## Transform Day Comparison

| Day | Pattern |
|-----|---------|
| 08 | Test case generator |
| **18** | **Type coercion safety validation** |

---

## License

MIT

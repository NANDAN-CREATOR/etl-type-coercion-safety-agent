"""
Day 18 — ETL Transform Data Type Coercion Safety Agent
=======================================================
Domain   : Transform (ETL)
Pattern  : Pre-cast validation gate — verify that planned type coercions are safe
           across the actual data in the current batch before applying them.
Guardrail: NEVER apply a type coercion that hasn't been validated against the
           actual data distribution. A schema-valid cast that fails on even one
           row can silently drop that row or raise a runtime error that kills the
           entire batch — depending on the engine's error mode.
"""

import re
import datetime

BATCH_SAMPLES = {
    "orders.order_amount": {
        "source_type": "VARCHAR",
        "sample_values": ["149.99", "3200.00", "0.50", "12.00", "87.45"],
        "null_count": 0, "total_rows": 18500, "null_fraction": 0.0,
    },
    "orders.order_date_str": {
        "source_type": "VARCHAR",
        "sample_values": ["2026-07-31", "2026-08-01", "2026-08-02", "N/A", "2026-08-03"],
        "null_count": 12, "total_rows": 18500, "null_fraction": 0.0006,
    },
    "orders.customer_id": {
        "source_type": "VARCHAR",
        "sample_values": ["1001", "1002", "9999", "10045", "CID-LEGACY-004"],
        "null_count": 0, "total_rows": 18500, "null_fraction": 0.0,
    },
    "orders.is_priority": {
        "source_type": "VARCHAR",
        "sample_values": ["Y", "N", "Y", "Y", "1"],
        "null_count": 3, "total_rows": 18500, "null_fraction": 0.0002,
    },
    "transactions.amount_cents": {
        "source_type": "VARCHAR",
        "sample_values": ["1500", "2300", "500", "750", "125"],
        "null_count": 0, "total_rows": 42000, "null_fraction": 0.0,
    },
    "transactions.event_ts": {
        "source_type": "VARCHAR",
        "sample_values": [
            "2026-08-03 01:23:45", "2026-08-03 02:11:00", "2026-08-03 03:00:00",
            "2026-08-03T04:15:30Z", "2026-08-03 05:00:00",
        ],
        "null_count": 0, "total_rows": 42000, "null_fraction": 0.0,
    },
}

def _try_cast_integer(v):
    try: int(v); return True
    except: return False

def _try_cast_decimal(v):
    try: float(v); return True
    except: return False

def _try_cast_date(v):
    try: datetime.date.fromisoformat(v); return True
    except: return False

def _try_cast_timestamp(v):
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]:
        try: datetime.datetime.strptime(v, fmt); return True
        except: continue
    return False

def _try_cast_boolean(v):
    return v.strip().upper() in ("TRUE", "FALSE", "1", "0", "T", "F")

CAST_VALIDATORS = {
    "INTEGER": _try_cast_integer, "DECIMAL": _try_cast_decimal,
    "DATE": _try_cast_date, "TIMESTAMP": _try_cast_timestamp, "BOOLEAN": _try_cast_boolean,
}


def get_column_sample(column_ref):
    if column_ref not in BATCH_SAMPLES:
        return {"column": column_ref, "status": "NOT_SAMPLED",
                "message": f"No sample data available for '{column_ref}'. Cannot validate coercion without data."}
    s = BATCH_SAMPLES[column_ref].copy()
    s["column"] = column_ref; s["status"] = "OK"; s["sample_size"] = len(s["sample_values"])
    return s


def validate_cast_safety(column_ref, target_type, allow_nulls=True):
    sample = get_column_sample(column_ref)
    if sample["status"] == "NOT_SAMPLED":
        return {"status": "UNVERIFIABLE", "column": column_ref, "target_type": target_type, "detail": sample["message"]}
    target_upper = target_type.upper()
    validator = CAST_VALIDATORS.get(target_upper)
    if not validator:
        return {"status": "UNKNOWN_TARGET_TYPE", "column": column_ref, "target_type": target_type,
                "detail": f"No validator registered for target type '{target_type}'."}
    values, failures, passes = sample["sample_values"], [], []
    for val in values:
        if val is None or val == "" or val.upper() in ("NULL", "N/A", "NONE", "NA"):
            passes.append(val); continue
        if validator(val): passes.append(val)
        else: failures.append({"value": val, "issue": "CAST_WILL_FAIL", "detail": f"Value {repr(val)} cannot be cast to {target_upper}."})
    if not allow_nulls and sample["null_fraction"] > 0:
        failures.append({"value": "(null)", "issue": "NULL_FRACTION_VIOLATES_POLICY",
                         "detail": f"Column has null_fraction={sample['null_fraction']:.4f} but target is NOT NULL."})
    if failures:
        return {"status": "UNSAFE", "column": column_ref, "source_type": sample["source_type"],
                "target_type": target_upper, "sample_size": len(values),
                "pass_count": len(passes), "fail_count": len(failures), "failures": failures,
                "detail": f"{len(failures)} of {len(values)} sampled value(s) would fail the {sample['source_type']} \u2192 {target_upper} cast. Coercion is unsafe for this batch."}
    return {"status": "SAFE", "column": column_ref, "source_type": sample["source_type"],
            "target_type": target_upper, "sample_size": len(values),
            "pass_count": len(passes), "null_count": sample["null_count"],
            "detail": f"All {len(passes)} sampled value(s) cast cleanly to {target_upper}. Null fraction: {sample['null_fraction']:.4f}."}


def check_format_consistency(column_ref, expected_format):
    sample = get_column_sample(column_ref)
    if sample["status"] == "NOT_SAMPLED":
        return {"status": "UNVERIFIABLE", "column": column_ref}
    values, format_counts = sample["sample_values"], {}
    if expected_format == "TIMESTAMP":
        patterns = {
            "space_separated": re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
            "iso8601_Z": re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"),
            "iso8601_no_tz": re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),
            "with_millis": re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$"),
        }
        for val in values:
            if val is None: continue
            matched = "other"
            for name, pat in patterns.items():
                if pat.match(val): matched = name; break
            format_counts[matched] = format_counts.get(matched, 0) + 1
        if len(format_counts) > 1:
            return {"status": "FORMAT_FRAGMENTATION", "column": column_ref, "format_variants": format_counts,
                    "detail": f"Column contains {len(format_counts)} different timestamp format(s): {format_counts}. Mixed formats can cause partial failures depending on the SQL engine's CAST strictness."}
    return {"status": "FORMAT_CONSISTENT", "column": column_ref, "format_counts": format_counts,
            "detail": "Format appears consistent across sampled values."}


def approve_coercion(column_ref, target_type, detail):
    return {"action": "APPROVED", "column": column_ref, "target_type": target_type, "detail": detail,
            "message": "Cast safety verified. Coercion may proceed."}

def block_coercion(column_ref, target_type, reason, detail):
    return {"action": "BLOCKED", "column": column_ref, "target_type": target_type, "reason": reason,
            "detail": detail, "message": "Coercion blocked. Fix data or transform logic before proceeding."}

def escalate_to_human(column_ref, target_type, reason, detail):
    return {"action": "ESCALATED", "column": column_ref, "target_type": target_type, "reason": reason,
            "detail": detail, "message": "Cast safety cannot be determined. Human review required."}


def run_agent(scenario):
    trace = []
    column, target_type = scenario["column"], scenario["target_type"]
    allow_nulls, check_format = scenario.get("allow_nulls", True), scenario.get("check_format", False)

    sample = get_column_sample(column)
    trace.append({"tool": "get_column_sample", "result": sample})
    if sample["status"] == "NOT_SAMPLED":
        trace.append({"tool": "escalate_to_human", "result": escalate_to_human(column, target_type, "NO_SAMPLE_DATA", sample["message"])})
        return trace

    if check_format:
        fmt_check = check_format_consistency(column, target_type.upper())
        trace.append({"tool": "check_format_consistency", "result": fmt_check})
        if fmt_check["status"] == "FORMAT_FRAGMENTATION":
            trace.append({"tool": "block_coercion", "result": block_coercion(column, target_type, "FORMAT_FRAGMENTATION", fmt_check["detail"])})
            return trace

    safety = validate_cast_safety(column, target_type, allow_nulls)
    trace.append({"tool": "validate_cast_safety", "result": safety})

    if safety["status"] == "UNSAFE":
        trace.append({"tool": "block_coercion", "result": block_coercion(column, target_type, "CAST_WILL_FAIL_ON_SAMPLE", safety["detail"])})
    elif safety["status"] in ("UNVERIFIABLE", "UNKNOWN_TARGET_TYPE"):
        trace.append({"tool": "escalate_to_human", "result": escalate_to_human(column, target_type, f"CAST_{safety['status']}", safety["detail"])})
    else:
        trace.append({"tool": "approve_coercion", "result": approve_coercion(column, target_type, f"All {safety['pass_count']} sampled values cast safely to {target_type.upper()}.")})
    return trace


SCENARIOS = [
    {"id": 1, "name": "Safe cast \u2014 VARCHAR order_amount \u2192 DECIMAL",
     "description": "orders.order_amount contains clean numeric strings. All 5 samples cast cleanly. Should APPROVE.",
     "column": "orders.order_amount", "target_type": "DECIMAL", "allow_nulls": True, "check_format": False},
    {"id": 2, "name": "Unsafe cast \u2014 VARCHAR customer_id \u2192 INTEGER (has CID-LEGACY-004)",
     "description": "Mostly numeric IDs but 1 legacy string format breaks the cast. Should BLOCK.",
     "column": "orders.customer_id", "target_type": "INTEGER", "allow_nulls": False, "check_format": False},
    {"id": 3, "name": "Mixed boolean encoding \u2014 is_priority has Y/N and 1",
     "description": "Y/N encoding fails strict BOOLEAN cast. Transform needs explicit CASE mapping first. Should BLOCK.",
     "column": "orders.is_priority", "target_type": "BOOLEAN", "allow_nulls": True, "check_format": False},
    {"id": 4, "name": "Format fragmentation \u2014 event_ts has mixed timestamp formats",
     "description": "space_separated and iso8601_Z formats mixed in the same column. Should BLOCK at format check.",
     "column": "transactions.event_ts", "target_type": "TIMESTAMP", "allow_nulls": False, "check_format": True},
]


def print_trace(scenario, trace):
    print("=" * 70)
    print(f"SCENARIO {scenario['id']}: {scenario['name']}")
    print(f"Column : {scenario['column']} \u2192 {scenario['target_type']}")
    print(f"\n{scenario['description']}\n")
    for step in trace:
        print(f"  -> TOOL: {step['tool']}")
        result = step["result"]
        for key in ["status","source_type","target_type","sample_size","pass_count","fail_count",
                    "null_count","null_fraction","format_variants","failures","action","reason","detail","message"]:
            if key in result:
                val = result[key]
                if isinstance(val, (list, dict)): val = str(val)
                print(f"       {key:26s}: {val}")
    print()


if __name__ == "__main__":
    print("\nDay 18 \u2014 ETL Transform Data Type Coercion Safety Agent\n")
    for scenario in SCENARIOS:
        trace = run_agent(scenario)
        print_trace(scenario, trace)

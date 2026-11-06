"""Tiny helper for the clean room setup scripts: run SQL on a Databricks SQL warehouse through the Databricks CLI.

Uses `databricks api` with a CLI profile (no SDK or token handling). Statements are polled until they finish.
"""
import json
import subprocess
import time


def cli(profile, *args, body=None):
    """Run a `databricks` CLI command with JSON output and return the parsed result (or {})."""
    cmd = ["databricks", *args, "--profile", profile, "-o", "json"]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"{' '.join(args[:3])} failed: {p.stderr.strip() or p.stdout.strip()}")
    return json.loads(p.stdout) if p.stdout.strip() else {}


def warehouse_id(profile):
    """Pick a serverless SQL warehouse (preferring one named 'Serverless Starter Warehouse')."""
    whs = cli(profile, "warehouses", "list")
    whs.sort(key=lambda w: (w["name"] != "Serverless Starter Warehouse", not w.get("enable_serverless_compute")))
    return whs[0]["id"]


def sql(profile, wh, statement, params=None):
    """Execute one statement; return a list of row dicts (empty for DDL/DML)."""
    body = {"warehouse_id": wh, "statement": statement, "wait_timeout": "50s",
            "disposition": "INLINE", "format": "JSON_ARRAY"}
    if params:
        body["parameters"] = [{"name": k, "value": str(v)} for k, v in params.items()]
    r = cli(profile, "api", "post", "/api/2.0/sql/statements", body=body)
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = cli(profile, "api", "get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {r['status'].get('error', {}).get('message')}\n{statement[:400]}")
    cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = list(r.get("result", {}).get("data_array") or [])
    # Large results come back in chunks.
    nxt = r.get("result", {}).get("next_chunk_internal_link")
    while nxt:
        chunk = cli(profile, "api", "get", nxt)
        rows += chunk.get("data_array") or []
        nxt = chunk.get("next_chunk_internal_link")
    return [dict(zip(cols, row)) for row in rows]


def sql_file(path, **subs):
    """Read a .sql file, substitute ${name} placeholders and split it into statements on lines that are just ';'."""
    text = open(path).read()
    for k, v in subs.items():
        text = text.replace("${" + k + "}", str(v))
    def has_code(s):
        return any(ln.strip() and not ln.strip().startswith("--") for ln in s.splitlines())
    return [s.strip() for s in text.split("\n;\n") if has_code(s)]

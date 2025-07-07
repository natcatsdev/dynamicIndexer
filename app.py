#!/usr/bin/env python3
# ---------------------------------------------------
# DynamicIndexer API – v0.9.1
#   • 120-sec reservation window (enableSig=true)
#   • reserved_until / added_at stored as Number (Decimal)
# ---------------------------------------------------
from __future__ import annotations

import os, sys, time, subprocess, re
from pathlib import Path
from decimal import Decimal                       # ← NEW

from flask import Flask, jsonify, request, abort
from flask_cors import CORS
import boto3
from boto3.dynamodb.conditions import Attr

# ────────── constants ──────────
DYNAMO_REGION  = "us-east-1"
INDEX_TABLE    = "dynamicIndex1"
BLOCKS_TABLE   = "mintBlocks"

HOLD_MS        = 120_000        # 120-second reservation window

# paths / timers (unchanged) ──────────────────────────────────────
BASE_DIR   = Path(__file__).parent
AUTH_SCRIPT  = BASE_DIR / "scripts" / "authLooperBackend.py"
INDEX_SCRIPT = BASE_DIR / "scripts" / "indexLooper.py"
BOTH_SCRIPT  = BASE_DIR / "scripts" / "run_both.py"

AUTH_LOCK  = "/tmp/authscript.lock"
INDEX_LOCK = "/tmp/indexscript.lock"
BOTH_LOCK  = "/tmp/runboth.lock"

TIMER_UNIT = "runboth.timer"
TIMER_FILE = BASE_DIR / "systemd" / "runboth.timer"
SEC_RE     = re.compile(r"^OnUnitActiveSec=(\d+)s$", re.M)

WATCHER_UNIT = "blockWatcher.timer"
LAST_FILE    = BASE_DIR / "scripts" / "state" / "last_height.txt"
SUDO_PATH    = "/usr/bin/sudo"
# ----------------------------------------------------------------

# ────────── Flask ──────────
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ────────── DynamoDB ───────
dynamo       = boto3.resource("dynamodb", region_name=DYNAMO_REGION)
index_table  = dynamo.Table(INDEX_TABLE)
blocks_table = dynamo.Table(BLOCKS_TABLE)

# ────────── helper: spawn scripts ──────────
def _running(lock: str) -> bool:
    p = Path(lock)
    if not p.exists():
        return False
    try:
        pid  = int(p.read_text())
        proc = Path(f"/proc/{pid}")
        if not proc.exists() or "\nState:\tZ" in (proc / "status").read_text():
            raise RuntimeError
        return True
    except Exception:
        p.unlink(missing_ok=True)
        return False

def _spawn(script: Path, lock: str, env_var: str):
    if _running(lock):
        return jsonify({"status": "busy"}), 409
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        env={**os.environ, env_var: lock},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    Path(lock).write_text(str(proc.pid))
    return jsonify({"status": "started"}), 202

def _sd(*args): subprocess.run([SUDO_PATH, "systemctl", *args], check=True)

# ────────── misc routes (unchanged) ──────────
@app.get("/api/ping")
def ping(): return {"status": "ok"}

@app.get("/api/block-data")
def block_data():
    items = index_table.scan().get("Items", [])
    items.sort(key=lambda x: int(x.get("block_number", 0)))
    return jsonify(items)

@app.post("/api/run-authscript")
def run_auth():   return _spawn(AUTH_SCRIPT, AUTH_LOCK, "AUTH_LOCK_FILE")
@app.post("/api/run-inscriptionscript")
def run_idx():    return _spawn(INDEX_SCRIPT, INDEX_LOCK, "INDEX_LOCK_FILE")
@app.post("/api/run-both")
def run_both():   return _spawn(BOTH_SCRIPT, BOTH_LOCK, "RUN_BOTH_LOCK_FILE")

# (schedule - watcher endpoints unchanged …)

# ===============================================================
#  Mint-Blocks endpoints
# ===============================================================

@app.get("/api/blocks/available")
def list_available_blocks():
    """Return blocks that are free or whose hold has expired."""
    now = Decimal(int(time.time() * 1000))
    resp = blocks_table.scan(
        FilterExpression=(
            Attr("status").not_exists() |
            Attr("status").eq("available") |
            (
                Attr("status").eq("reserved") &
                Attr("reserved_until").lt(now)
            )
        )
    )
    items = resp.get("Items", [])
    items.sort(key=lambda x: int(x.get("block", 0)))
    return jsonify(items)

@app.get("/api/blocks")
def list_all_blocks():
    items = blocks_table.scan().get("Items", [])
    items.sort(key=lambda x: int(x.get("block", 0)))
    return jsonify(items)

@app.get("/api/blocks/<int:block>")
def get_block(block: int):
    item = blocks_table.get_item(Key={"block": block}).get("Item")
    if not item: abort(404, "Block not found")
    return jsonify(item)

@app.post("/api/blocks/<int:block>/mint")
def reserve_block(block: int):
    """
    • enableSig=true → 120-sec hold (reserved_by / reserved_until)
    • legacy (no enableSig) → indefinite
    """
    body     = request.get_json(force=True) or {}
    now_ms   = int(time.time() * 1000)
    use_sig  = bool(body.get("enableSig"))
    wallet   = body.get("wallet")

    if use_sig and not wallet:
        abort(400, "wallet required when enableSig true")

    if use_sig:
        expires = now_ms + HOLD_MS
        cond = (
            Attr("status").not_exists() |
            Attr("status").eq("available") |
            (Attr("status").eq("reserved") & Attr("reserved_until").lt(Decimal(now_ms)))
        )
        try:
            blocks_table.update_item(
                Key={"block": block},
                ConditionExpression=cond,
                UpdateExpression="""
                    SET #s = :r,
                        reserved_by    = :rb,
                        reserved_until = :ru,
                        added_at       = :a
                """,
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":r" : "reserved",
                    ":rb": wallet,
                    ":ru": Decimal(expires),   # ← store as Number
                    ":a" : Decimal(now_ms),
                },
            )
        except blocks_table.meta.client.exceptions.ConditionalCheckFailedException:
            abort(400, "Block not available")
        return {"reserved_until": expires}, 200

    # ── legacy: indefinite hold ──
    existing = blocks_table.get_item(Key={"block": block}).get("Item", {})
    if existing.get("status") not in (None, "available"):
        abort(400, "Block not available")
    blocks_table.update_item(
        Key={"block": block},
        UpdateExpression="SET #s=:r, added_at=:a",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":r": "reserved",
            ":a": Decimal(now_ms),
        },
    )
    return "", 204

@app.patch("/api/blocks/<int:block>/status")
def update_block_status(block: int):
    body = request.get_json(force=True) or {}
    upd, names, vals = [], {}, {}
    if "status" in body:
        upd.append("#s=:s"); names["#s"]="status"; vals[":s"]=body["status"]
    if "inscription_id" in body:
        upd.append("#i=:i"); names["#i"]="inscription_id"; vals[":i"]=body["inscription_id"]
    now_ms = int(time.time() * 1000)
    upd.append("#a=:a"); names["#a"]="added_at"; vals[":a"]=Decimal(now_ms)

    blocks_table.update_item(
        Key={"block": block},
        UpdateExpression="SET " + ", ".join(upd),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=vals,
    )
    return "", 204

# ===============================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)

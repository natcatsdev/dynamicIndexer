#!/usr/bin/env python3
# ---------------------------------------------------
# DynamicIndexer API – v0.9.4
#   • Web-Sockets via Flask-SocketIO + eventlet
#   • Single-reservation guard
#   • 120-s hold window (enableSig=true)
# ---------------------------------------------------
from __future__ import annotations

# eventlet must patch stdlib *before* anything else uses sockets
import eventlet
eventlet.monkey_patch()

import os, sys, time, subprocess
from pathlib import Path
from decimal import Decimal

from flask import Flask, jsonify, request, abort
from flask_cors import CORS
from flask_socketio import SocketIO
import boto3
from boto3.dynamodb.conditions import Attr

# ─── constants ──────────────────────────────────────
REGION       = "us-east-1"
INDEX_TABLE  = "dynamicIndex1"
BLOCKS_TABLE = "mintBlocks"
HOLD_MS      = 120_000                             # 120 s

BASE_DIR    = Path(__file__).parent
AUTH_SCRIPT = BASE_DIR / "scripts" / "authLooperBackend.py"
INDEX_SCRIPT= BASE_DIR / "scripts" / "indexLooper.py"
BOTH_SCRIPT = BASE_DIR / "scripts" / "run_both.py"

AUTH_LOCK = "/tmp/authscript.lock"
INDEX_LOCK= "/tmp/indexscript.lock"
BOTH_LOCK = "/tmp/runboth.lock"

_now_ms = lambda: int(time.time() * 1000)

# ─── app + WS server ────────────────────────────────
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    ping_interval=25,
    ping_timeout=60,
)
import os
from flask import request

@app.get("/api/pid")
def pid():
    return {"pid": os.getpid()}

@socketio.on("whoami")
def whoami():
    socketio.emit("iam", {"pid": os.getpid()}, to=request.sid)
dynamo        = boto3.resource("dynamodb", region_name=REGION)
index_table   = dynamo.Table(INDEX_TABLE)
blocks_table  = dynamo.Table(BLOCKS_TABLE)


# ── helpers ─────────────────────────────────────────
def _broadcast(block: int, payload: dict):
    """
    Emit a JSON-safe diff to every connected Web-Socket client.
    Any Decimal values coming from DynamoDB are coerced to int so
    socketio.emit() never trips over “Decimal is not JSON serializable”.
    """
    def _clean(v):
        return int(v) if isinstance(v, Decimal) else v

    safe_payload = {k: _clean(v) for k, v in payload.items()}
    socketio.emit("block_update", {"block": int(block), **safe_payload})

def _running(lock: str) -> bool:
    """Is the helper script already running?"""
    p = Path(lock)
    if not p.exists():
        return False
    try:
        pid = int(p.read_text())
        proc = Path(f"/proc/{pid}")
        if not proc.exists() or "State:\tZ" in (proc / "status").read_text():
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

def _active_reservation(wallet: str, now_ms: int) -> int | None:
    """Return block # still held by this wallet (or None)."""
    resp = blocks_table.scan(
        ProjectionExpression="#b, reserved_until, #s",
        ExpressionAttributeNames={"#b": "block", "#s": "status"},
        FilterExpression=Attr("reserved_by").eq(wallet) & Attr("status").eq("reserved"),
    )
    for it in resp.get("Items", []):
        until = int(it.get("reserved_until", 0))
        if until == 0 or until > now_ms:
            return int(it["block"])
    return None

def _release_expired_holds(now_ms: int):
    """Flip status -> available for holds whose timer ran out."""
    resp = blocks_table.scan(
        ProjectionExpression="#b, reserved_until",
        ExpressionAttributeNames={"#b": "block"},
        FilterExpression=(
            Attr("status").eq("reserved")
            & Attr("reserved_until").exists()
            & Attr("reserved_until").lt(Decimal(now_ms))
        ),
    )
    for it in resp["Items"]:
        try:
            blocks_table.update_item(
                Key={"block": it["block"]},
                ConditionExpression=Attr("status").eq("reserved"),
                UpdateExpression="""
                    REMOVE reserved_by, reserved_until
                    SET #s = :a, added_at = :at
                """,
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":a": "available",
                    ":at": Decimal(now_ms),
                },
            )
            _broadcast(it["block"], {"status": "available"})
        except blocks_table.meta.client.exceptions.ConditionalCheckFailedException:
            pass

# ─── misc endpoints ─────────────────────────────────
@app.get("/api/ping")
def ping():
    return {"status": "ok"}

# ─── push-style “announce-block” endpoint ──────────────────────────────
@app.post("/api/announce-block")
def announce_block():
    """
    Body: {
      "block"   : <int>,                     # REQUIRED
      ...any other fields to merge...
    }
    Simply re-emits the payload via Web-Sockets so every connected
    front-end gets a live diff without polling.
    """
    body = request.get_json(force=True) or {}
    blk  = body.get("block")
    if blk is None:
        abort(400, "missing block")

    # Broadcast the diff exactly as it came in
    _broadcast(int(blk), {k: v for k, v in body.items() if k != "block"})
    return "", 204



@app.post("/api/run-authscript")
def run_auth():
    return _spawn(AUTH_SCRIPT, AUTH_LOCK, "AUTH_LOCK_FILE")

@app.post("/api/run-inscriptionscript")
def run_index():
    return _spawn(INDEX_SCRIPT, INDEX_LOCK, "INDEX_LOCK_FILE")

@app.post("/api/run-both")
def run_both():
    return _spawn(BOTH_SCRIPT, BOTH_LOCK, "RUN_BOTH_LOCK_FILE")

# ─── public data endpoints ──────────────────────────
@app.get("/api/blocks")
def all_blocks():
    now = _now_ms()
    _release_expired_holds(now)
    resp = blocks_table.scan()
    items = resp.get("Items", [])
    items.sort(key=lambda x: int(x["block"]))
    return jsonify(items)

@app.get("/api/blocks/<int:block>")
def get_block(block: int):
    """
    Return a single block record so /broadcast/package can
    verify the reservation still belongs to the caller.
    """
    now = _now_ms()
    _release_expired_holds(now)                # keep timers honest

    rec = blocks_table.get_item(Key={"block": block}).get("Item")
    if not rec:                                # nonexistent block → 404
        abort(404, "block not found")

    # DynamoDB numbers arrive as Decimal → coerce to int for JSON
    clean = {
        k: int(v) if isinstance(v, Decimal) else v
        for k, v in rec.items()
    }
    return jsonify(clean), 200

# ─── reservation endpoints ──────────────────────────
@app.post("/api/blocks/<int:block>/mint")
def reserve_block(block: int):
    now = _now_ms()
    _release_expired_holds(now)

    body    = request.get_json(force=True) or {}
    wallet  = body.get("wallet")
    use_sig = bool(body.get("enableSig"))

    if use_sig and not wallet:
        abort(400, "wallet required when enableSig true")

    if wallet:
        other = _active_reservation(wallet, now)
        if other and other != block:
            return jsonify({"message": f"wallet already holds block {other}"}), 409

    if use_sig:   # 120-s timed hold
        expires = now + HOLD_MS
        cond = (
            Attr("status").not_exists()
            | Attr("status").eq("available")
            | (
                Attr("status").eq("reserved")
                & (
                    Attr("reserved_until").not_exists()
                    | Attr("reserved_until").lt(Decimal(now))
                )
            )
        )
        try:
            blocks_table.update_item(
                Key={"block": block},
                ConditionExpression=cond,
                UpdateExpression="""
                    SET #s=:r, reserved_by=:wb,
                        reserved_until=:ru, added_at=:at
                """,
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":r": "reserved",
                    ":wb": wallet,
                    ":ru": Decimal(expires),
                    ":at": Decimal(now),
                },
            )
            _broadcast(block, {
                "status": "reserved",
                "reserved_by": wallet,
                "reserved_until": expires,
            })
        except blocks_table.meta.client.exceptions.ConditionalCheckFailedException:
            abort(400, "Block not available")
        return {"reserved_until": expires}, 200

    # ── legacy indefinite hold (enableSig false) ───────────
    existing = blocks_table.get_item(Key={"block": block}).get("Item", {})
    if existing.get("status") not in (None, "available"):
        abort(400, "Block not available")

    blocks_table.update_item(
        Key={"block": block},
        UpdateExpression="""
            SET #s=:r,
                reserved_by=:wb,
                added_at=:at
        """,
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":r" : "reserved",
            ":wb": wallet,
            ":at": Decimal(now),
        },
    )
    _broadcast(block, {
        "status": "reserved",
        "reserved_by": wallet,
    })
    return "", 204

@app.delete("/api/blocks/<int:block>/mint")
def release_block(block: int):
    body   = request.get_json(force=True) or {}
    wallet = body.get("wallet") or ""
    cond = Attr("reserved_by").eq(wallet) & Attr("status").eq("reserved")
    try:
        blocks_table.update_item(
            Key={"block": block},
            ConditionExpression=cond,
            UpdateExpression="""
                REMOVE reserved_by, reserved_until
                SET #s=:a, added_at=:at
            """,
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":a": "available", ":at": Decimal(_now_ms())},
        )
        _broadcast(block, {"status": "available"})
    except blocks_table.meta.client.exceptions.ConditionalCheckFailedException:
        abort(409, "reservation not held by this wallet")
    return "", 204

# ─── status patch (minted / complete etc.) ─────────────────────────
@app.patch("/api/blocks/<int:block>/status")
def update_status(block: int):
    """
    Body: { "status": "<new-status>", "inscription_id": "<id (optional)>" }
    Allowed status values: "minted", "complete", "reserved", "available".
    "complete" is normalised → "minted".
    """
    body        = request.get_json(force=True) or {}
    new_status  = body.get("status")
    insc_id     = body.get("inscription_id")

    if new_status not in ("minted", "complete", "reserved", "available"):
        abort(400, "bad status")

    if new_status == "complete":
        new_status = "minted"

    expr_names  = {"#s": "status"}
    expr_values = {
        ":ns": new_status,
        ":at": Decimal(_now_ms()),
    }
    update_expr = "SET #s = :ns, added_at = :at"

    if insc_id:
        expr_names["#i"] = "inscription_id"
        expr_values[":iid"] = insc_id
        update_expr += ", #i = :iid"

    blocks_table.update_item(
        Key={"block": block},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
    )

    diff = {"status": new_status}
    if insc_id:
        diff["inscription_id"] = insc_id
    _broadcast(block, diff)

    return "", 204


# ─── run ────────────────────────────────────────────
if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=8080)

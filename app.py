#!/usr/bin/env python3
# ---------------------------------------------------
# DynamicIndexer API – v0.5  (Auth, Index, and Run-Both)
# ---------------------------------------------------
from __future__ import annotations
import os, sys, subprocess, json
from pathlib import Path
from flask import Flask, jsonify, request
from flask_cors import CORS
import boto3

# ───────────── constants ─────────────
DYNAMO_REGION = "us-east-1"
TABLE_NAME    = "dynamicIndex1"

BASE_DIR      = Path(__file__).parent
AUTH_SCRIPT   = BASE_DIR / "scripts" / "authLooperBackend.py"
INDEX_SCRIPT  = BASE_DIR / "scripts" / "indexLooper.py"
BOTH_SCRIPT   = BASE_DIR / "scripts" / "run_both.py"        # NEW

AUTH_LOCK     = "/tmp/authscript.lock"
INDEX_LOCK    = "/tmp/indexscript.lock"
BOTH_LOCK     = "/tmp/runboth.lock"                         # NEW

# ───────────── Flask / CORS ──────────
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ───────────── DynamoDB ──────────────
table = boto3.resource("dynamodb", region_name=DYNAMO_REGION) \
             .Table(TABLE_NAME)

# ───────────── helpers ───────────────
def _running(lock_path: str) -> bool:
    """Return True iff the PID stored in lock_path is a live, non-zombie process."""
    p = Path(lock_path)
    if not p.exists():
        return False
    try:
        pid = int(p.read_text())
        proc_dir = Path(f"/proc/{pid}")
        if not proc_dir.exists():
            raise RuntimeError
        if "\nState:\tZ" in (proc_dir / "status").read_text():  # zombie
            raise RuntimeError
        return True
    except Exception:
        p.unlink(missing_ok=True)          # clean stale lock
        return False

def _spawn(script: Path, lock_path: str, env_var: str):
    """Launch script in background, write PID to lock-file, or 409 if busy."""
    if _running(lock_path):
        return jsonify({"status": "busy"}), 409

    proc = subprocess.Popen(
        [sys.executable, str(script)],
        env={**os.environ, env_var: lock_path},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    Path(lock_path).write_text(str(proc.pid))
    return jsonify({"status": "started"}), 202

# ───────────── routes ────────────────
@app.get("/api/ping")
def ping():
    return {"status": "ok"}

@app.get("/api/block-data")
def block_data():
    items = table.scan().get("Items", [])
    items.sort(key=lambda x: int(x.get("block_number", 0)))
    return jsonify(items)

@app.post("/api/run-authscript")
def run_auth():
    return _spawn(AUTH_SCRIPT, AUTH_LOCK, "AUTH_LOCK_FILE")

@app.post("/api/run-inscriptionscript")
def run_index():
    return _spawn(INDEX_SCRIPT, INDEX_LOCK, "INDEX_LOCK_FILE")

# ---------- NEW: wrapper that runs Auth then Inscription ----------
@app.post("/api/run-both")
def run_both():
    return _spawn(BOTH_SCRIPT, BOTH_LOCK, "RUN_BOTH_LOCK_FILE")

# ───────────── local dev ─────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)

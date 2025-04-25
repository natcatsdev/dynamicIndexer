#!/usr/bin/env python3
# ---------------------------------------------------
# DynamicIndexer API – minimal v0.4
# ---------------------------------------------------
from __future__ import annotations
import os, sys, subprocess
from pathlib import Path
from flask import Flask, jsonify
from flask_cors import CORS
import boto3

# ───────────── constants ─────────────
DYNAMO_REGION = "us-east-1"
TABLE_NAME    = "dynamicIndex1"

BASE_DIR      = Path(__file__).parent
AUTH_SCRIPT   = BASE_DIR / "scripts" / "authLooperBackend.py"
INDEX_SCRIPT  = BASE_DIR / "scripts" / "indexLooper.py"

AUTH_LOCK     = "/tmp/authscript.lock"
INDEX_LOCK    = "/tmp/indexscript.lock"

# ───────────── Flask / CORS ──────────
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ───────────── DynamoDB ──────────────
table = boto3.resource("dynamodb", region_name=DYNAMO_REGION)\
             .Table(TABLE_NAME)

# ───────────── helpers ───────────────
def _running(lock_path: str) -> bool:
    """
    True  → PID in lock-file is alive and **not** a zombie.
    False → process gone; stale lock is deleted for next run.
    """
    p = Path(lock_path)
    if not p.exists():
        return False
    try:
        pid = int(p.read_text())
        proc_dir = Path(f"/proc/{pid}")
        if not proc_dir.exists():
            raise RuntimeError("PID dead")

        # Detect zombie (defunct) state → treat as not running
        status_txt = (proc_dir / "status").read_text()
        if "\nState:\tZ" in status_txt:   # zombie
            raise RuntimeError("PID defunct")

        return True          # process truly running
    except Exception:
        p.unlink(missing_ok=True)  # auto-clean stale lock
        return False

def _spawn(script: Path, lock_path: str, env_var: str):
    """Launch script in background & write its PID to lock-file."""
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

# ───────────── local dev ─────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)

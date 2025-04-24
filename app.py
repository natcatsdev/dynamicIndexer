#!/usr/bin/env python3
# ---------------------------------------------------
# DynamicIndexer API – minimal v0.2  (adds CORS)
# ---------------------------------------------------
from __future__ import annotations
import os, sys, subprocess
from pathlib import Path

from flask import Flask, jsonify
from flask_cors import CORS               # ★ NEW
import boto3

# ---------- constants -------------------------------------------------
DYNAMO_REGION = "us-east-1"
TABLE_NAME    = "dynamicIndex1"

BASE_DIR    = Path(__file__).parent
AUTH_SCRIPT = BASE_DIR / "scripts" / "authLooperBackend.py"
LOCK_FILE   = "/tmp/authscript.lock"

# ---------- Flask + CORS ----------------------------------------------
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})   # ★ NEW

# ---------- DynamoDB ---------------------------------------------------
table = boto3.resource("dynamodb", region_name=DYNAMO_REGION)\
              .Table(TABLE_NAME)

# ---------- routes -----------------------------------------------------
@app.get("/api/ping")
def ping():
    return {"status": "ok"}

@app.get("/api/block-data")
def block_data():
    items = table.scan().get("Items", [])
    items.sort(key=lambda x: int(x.get("block_number", 0)))
    return jsonify(items)

def _running() -> bool:
    if not Path(LOCK_FILE).exists():
        return False
    try:
        pid = int(Path(LOCK_FILE).read_text())
        return Path(f"/proc/{pid}").exists()
    except Exception:
        return False

@app.post("/api/run-authscript")
def run_auth():
    if _running():
        return jsonify({"status": "busy"}), 409

    subprocess.Popen(
        [sys.executable, str(AUTH_SCRIPT)],
        env={**os.environ, "AUTH_LOCK_FILE": LOCK_FILE},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    Path(LOCK_FILE).write_text(str(os.getpid()))
    return jsonify({"status": "started"}), 202

# ---------- dev entrypoint --------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)

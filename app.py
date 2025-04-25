#!/usr/bin/env python3
# ---------------------------------------------------
# DynamicIndexer API – v0.6.3
# Supports Auth, Index, Run-Both + Scheduler with error reporting
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
BOTH_SCRIPT   = BASE_DIR / "scripts" / "run_both.py"

AUTH_LOCK     = "/tmp/authscript.lock"
INDEX_LOCK    = "/tmp/indexscript.lock"
BOTH_LOCK     = "/tmp/runboth.lock"

TIMER_UNIT    = "runboth.timer"
CADENCE_DIR   = f"/etc/systemd/system/{TIMER_UNIT}.d"
CADENCE_FILE  = f"{CADENCE_DIR}/override.conf"

# ───────────── Flask / CORS ──────────
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ───────────── DynamoDB ──────────────
table = boto3.resource("dynamodb", region_name=DYNAMO_REGION).Table(TABLE_NAME)

# ───────────── helpers ───────────────
def _running(lock_path: str) -> bool:
    p = Path(lock_path)
    if not p.exists():
        return False
    try:
        pid = int(p.read_text())
        proc_dir = Path(f"/proc/{pid}")
        if not proc_dir.exists():
            raise RuntimeError
        if "\nState:\tZ" in (proc_dir / "status").read_text():
            raise RuntimeError
        return True
    except Exception:
        p.unlink(missing_ok=True)
        return False

def _spawn(script: Path, lock_path: str, env_var: str):
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

# ───────────── script routes ───────────────
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

@app.post("/api/run-both")
def run_both():
    return _spawn(BOTH_SCRIPT, BOTH_LOCK, "RUN_BOTH_LOCK_FILE")

# ───────────── scheduler endpoints ─────────────
@app.get("/api/schedule/status")
def schedule_status():
    try:
        out = subprocess.run(["sudo", "systemctl", "is-active", TIMER_UNIT],
                             capture_output=True, text=True, check=True)
        return {"enabled": out.stdout.strip() == "active"}
    except subprocess.CalledProcessError as e:
        return jsonify({"error": f"is-active failed: {e.stdout.strip()}"}), 500
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500

@app.get("/api/schedule/cadence")
def schedule_get_cadence():
    try:
        out = subprocess.run(
            ["sudo", "systemctl", "show", TIMER_UNIT, "--property=OnUnitActiveSec", "--value"],
            capture_output=True, text=True, check=True
        ).stdout.strip()
        out = out.rstrip("s")
        return {"seconds": int(out) if out.isdigit() else 300}
    except subprocess.CalledProcessError as e:
        return jsonify({"error": f"systemctl show failed: {e.stdout.strip()}"}), 500
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500

@app.post("/api/schedule/cadence")
def schedule_set_cadence():
    try:
        data = request.get_json(force=True) or {}
        seconds = int(data.get("seconds", 0))
        if not 10 <= seconds <= 86400:
            return {"error": "Seconds must be 10–86400"}, 400

        os.makedirs(CADENCE_DIR, exist_ok=True)
        Path(CADENCE_FILE).write_text(
            f"[Timer]\nOnUnitActiveSec={seconds}s\n", encoding="utf-8")

        subprocess.run(["sudo", "systemctl", "daemon-reload"])
        subprocess.run(["sudo", "systemctl", "restart", TIMER_UNIT])
        return {"seconds": seconds}
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500

@app.post("/api/schedule/enable")
def schedule_enable():
    try:
        subprocess.run(["sudo", "systemctl", "start", TIMER_UNIT])
        return {"enabled": True}
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500

@app.post("/api/schedule/disable")
def schedule_disable():
    try:
        subprocess.run(["sudo", "systemctl", "stop", TIMER_UNIT])
        return {"enabled": False}
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500

# ───────────── dev entrypoint ─────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)

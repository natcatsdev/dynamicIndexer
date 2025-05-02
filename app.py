#!/usr/bin/env python3
# ---------------------------------------------------
# DynamicIndexer API – v0.9.9  (adds /api/export/index.json)
# ---------------------------------------------------
from __future__ import annotations

import os, sys, subprocess, re, json                     # ← json for export
from inscription_utils import build_payload, DELEGATE_ID
from datetime import datetime, timezone
from pathlib import Path
from flask import Flask, jsonify, request, abort, Response
from flask_cors import CORS
import boto3
from inscription_utils import build_payload, DELEGATE_ID
from decimal import Decimal
import requests, functools
from boto3.dynamodb.conditions import Attr
# ← new import

# ───────────── constants ─────────────

NODE_BASE = "http://172.31.19.167:5000" 
DYNAMO_REGION = "us-east-1"
TABLE_NAME    = "dynamicIndex1"

BASE_DIR      = Path(__file__).parent
AUTH_SCRIPT   = BASE_DIR / "scripts" / "authLooperBackend.py"
INDEX_SCRIPT  = BASE_DIR / "scripts" / "indexLooper.py"
BOTH_SCRIPT   = BASE_DIR / "scripts" / "run_both.py"

AUTH_LOCK     = "/tmp/authscript.lock"
INDEX_LOCK    = "/tmp/indexscript.lock"
BOTH_LOCK     = "/tmp/runboth.lock"

RUNFAST_UNIT  = "runfast.timer"
RUNSLOW_UNIT  = "runslow.timer"
TIMER_UNITS   = {"runfast": RUNFAST_UNIT, "runslow": RUNSLOW_UNIT}

TIMER_UNIT    = "runboth.timer"                 # legacy
TIMER_FILE    = BASE_DIR / "systemd" / "runboth.timer"
SEC_RE        = re.compile(r"^OnUnitActiveSec=(\d+)s$", re.M)

WATCHER_UNIT  = "blockWatcher.timer"
LAST_FILE     = BASE_DIR / "scripts" / "state" / "last_height.txt"

SUDO_PATH     = "/usr/bin/sudo"

# ───────────── Flask / CORS ──────────
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ───────────── DynamoDB ──────────────
table = boto3.resource("dynamodb", region_name=DYNAMO_REGION).Table(TABLE_NAME)

# ───────────── helpers ───────────────
def _running(lock: str) -> bool:
    p = Path(lock)
    if not p.exists():
        return False
    try:
        pid = int(p.read_text())
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


def _sd(*args):
    subprocess.run([SUDO_PATH, "systemctl", *args], check=True)


def _timer_status(unit: str) -> dict[str, str | bool | None]:
    """
    Return {enabled, lastRun, nextRun}.  Falls back to `list-timers -l` and
    handles rows that omit UTC by joining the first 4 tokens.
    """
    try:
        raw = subprocess.check_output(
            [
                SUDO_PATH,
                "systemctl",
                "show",
                unit,
                "--property=ActiveState,State,"
                "LastTriggerUSec,NextElapseUSec,"
                "NextElapseUSecRealtime,NextElapseUSecMonotonic",
                "--no-page",
            ],
            text=True,
        )
        kv = dict(line.split("=", 1) for line in raw.strip().splitlines())
    except Exception as e:
        return {"enabled": False, "lastRun": None, "nextRun": None, "error": str(e)}

    def _parse(ts: str | None) -> str | None:
        if not ts or ts == "n/a":
            return None
        if ts.isdigit():  # epoch-microseconds
            try:
                return (
                    datetime.utcfromtimestamp(int(ts) / 1_000_000)
                    .replace(tzinfo=timezone.utc)
                    .isoformat()
                )
            except Exception:
                return None
        for fmt in (
            "%a %Y-%m-%d %H:%M:%S %Z",
            "%a %Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S %Z",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                return (
                    datetime.strptime(ts, fmt)
                    .replace(tzinfo=timezone.utc)
                    .isoformat()
                )
            except ValueError:
                continue
        return None

    last_iso = _parse(kv.get("LastTriggerUSec"))
    next_iso = _parse(
        kv.get("NextElapseUSec")
        or kv.get("NextElapseUSecRealtime")
        or kv.get("NextElapseUSecMonotonic")
    )

    # fallback: list-timers -l
    if next_iso is None:
        try:
            row = subprocess.check_output(
                [SUDO_PATH, "systemctl", "list-timers", "--all", "-l", "--no-legend", unit],
                text=True,
            ).strip()
            if row:
                parts = row.split()
                if "UTC" in parts:
                    ts_str = " ".join(parts[: parts.index("UTC") + 1])  # Sun … UTC
                else:                                                  # no UTC token
                    ts_str = " ".join(parts[:4])                       # Sun … HH:MM:SS
                next_iso = _parse(ts_str)
        except Exception:
            pass

    enabled = kv.get("ActiveState") == "active" or kv.get("State") in {"waiting", "running"}

    return {"enabled": enabled, "lastRun": last_iso, "nextRun": next_iso}

# ───────────── one-shot routes ─────────────



#bitcoin node api
@app.get("/api/node-status")
def node_status():
    try:
        return requests.get(f"{NODE_BASE}/status", timeout=5).json()
    except Exception as e:
        return {"error": f"node unreachable – {e}"}, 502

@app.post("/api/inscribe")
def api_inscribe_proxy():
    resp = requests.post(
        f"{NODE_BASE}/inscribe",
        json=request.get_json(force=True),
        timeout=60
    )
    return resp.json(), resp.status_code

#indexer api

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


# ───────────── timer status route ───────────
@app.get("/api/timer/<timer>/status")
def timer_status(timer: str):
    if timer not in TIMER_UNITS:
        abort(404, description="unknown timer")
    return _timer_status(TIMER_UNITS[timer])

# ───────────── legacy run-both endpoints ────
@app.get("/api/schedule/status")
def schedule_status():
    out = subprocess.run(
        [SUDO_PATH, "systemctl", "is-active", TIMER_UNIT],
        capture_output=True,
        text=True,
    )
    return {"enabled": out.stdout.strip() == "active"}


@app.get("/api/schedule/cadence")
def schedule_cadence_get():
    txt = TIMER_FILE.read_text()
    m = SEC_RE.search(txt)
    return {"seconds": int(m.group(1)) if m else None}


@app.post("/api/schedule/cadence")
def schedule_cadence_set():
    seconds = int((request.get_json(force=True) or {}).get("seconds", 0))
    if not 60 <= seconds <= 86_400:
        return {"error": "Seconds must be 60–86400"}, 400
    txt = TIMER_FILE.read_text()
    txt = (
        SEC_RE.sub(f"OnUnitActiveSec={seconds}s", txt)
        if SEC_RE.search(txt)
        else txt + f"\nOnUnitActiveSec={seconds}s\n"
    )
    TIMER_FILE.write_text(txt)
    _sd("daemon-reload")
    _sd("restart", TIMER_UNIT)
    return {"seconds": seconds}, 202


@app.post("/api/schedule/enable")
def schedule_enable():
    _sd("start", TIMER_UNIT)
    return {"enabled": True}


@app.post("/api/schedule/disable")
def schedule_disable():
    _sd("stop", TIMER_UNIT)
    return {"enabled": False}


# ───────────── Block-Watcher endpoints ─────
@app.get("/api/watcher/status")
def watcher_status():
    out = subprocess.run(
        [SUDO_PATH, "systemctl", "is-active", WATCHER_UNIT],
        capture_output=True,
        text=True,
    )
    return {"running": out.stdout.strip() == "active"}


@app.post("/api/watcher/enable")
def watcher_enable():
    _sd("start", WATCHER_UNIT)
    return {"running": True}


@app.post("/api/watcher/disable")
def watcher_disable():
    _sd("stop", WATCHER_UNIT)
    return {"running": False}


@app.get("/api/watcher/lastht")
def watcher_last_ht():
    return {"lastHeight": int(LAST_FILE.read_text()) if LAST_FILE.exists() else None}

# ───────────── export index (NEW) ───────────────


# ───────────── authorised-parent lookup (NEW) ─────────────
# ───────────── authorised-parent lookup (UPDATED) ─────────────
@app.get("/api/parent/<int:block>")
def get_parent(block: int):
    """
    Return:
      {
        "parent"   : <inscriptionID> | None,
        "authBlock": <authorised-parent block height> | None
      }

    Steps
      • Row A  = item where block_number == <block>
      • auth   = Row A["authParent"]       (absent → no parent yet)
      • Row B  = item where block_number == auth
      • iid    = Row B["inscriptionID"]    (must be >64 chars to count)
    """
    try:
        rowA = table.get_item(Key={"block_number": block}).get("Item")
        auth = rowA and rowA.get("authParent")

        if not auth:
            return {"parent": None, "authBlock": None}

        rowB = table.get_item(Key={"block_number": int(auth)}).get("Item")
        iid  = rowB and rowB.get("inscriptionID")

        if iid and len(iid) > 64:
            return {"parent": iid, "authBlock": int(auth)}

        # parent block known but inscription not yet available
        return {"parent": None, "authBlock": int(auth)}

    except Exception as e:
        abort(500, f"lookup failed: {e}")



# ───────── inscription owner lookup (NEW) ───────────────────
@functools.lru_cache(maxsize=1024)
def _owner_cached(iid: str):
    """
    One-minute cache so repeated look-ups don’t hit Hiro every time.
    Returns a bech32 address string or None.
    """
    url  = f"https://api.hiro.so/ordinals/v1/inscriptions/{iid}"
    data = requests.get(url, timeout=15).json()
    # Hiro returns either 'address' or 'owner' depending on transfer history
    return data.get("address") or data.get("owner")


@app.get("/api/inscription/<path:iid>/owner")
def inscription_owner(iid: str):
    """
    → 200  {"owner": "bc1p…"}     when found
    → 404  when Hiro has no record yet
    → 502  on network / API error
    """
    try:
        owner = _owner_cached(iid)
        if not owner:
            abort(404, "Owner not found")
        return {"owner": owner}
    except Exception as e:
        abort(502, f"Lookup failed: {e}")
# ────────────────────────────────────────────────────────────
# ───────────── build-payload preview (NEW) ─────────────
@app.get("/api/payload/<int:block>")
def payload_preview(block: int):
    """
    Return JSON payload + delegate for front-end preview / wallet download.
      {
        "payload": {…},       # the on-chain JSON
        "delegate": "6647…ef7i0"
      }
    """
    return jsonify({
        "payload":  build_payload(block),
        "delegate": DELEGATE_ID
    })
@app.get("/api/export/index.json")
def export_index():
    """
    Return JSON array of rows whose inscriptionID exists and is >64 chars.
    Dummy traits: 'Trait 6' / 'Trait 9' when block_number text contains 6 or 9.
    """
    try:
        scan = table.scan(
            FilterExpression=Attr("inscriptionID").exists() &
                             Attr("inscriptionID").size().gt(64)
        )
    except Exception as e:
        abort(500, f"dynamo scan failed: {e}")

    items = scan.get("Items", [])
    out   = []
    for it in items:
        h   = int(it["block_number"])
        iid = it["inscriptionID"]

        traits = []
        if "6" in str(h):
            traits.append({"value": "true", "trait_type": "Trait 6"})
        if "9" in str(h):
            traits.append({"value": "true", "trait_type": "Trait 9"})

        out.append({
            "id": iid,
            "meta": {
                "name": f"Natcat #{h}",
                "attributes": traits,
                "high_res_img_url": (
                    "https://renderer.magiceden.dev/v2/dmt-natcats"
                    f"?v=2&block={h}"
                )
            }
        })

    resp = Response(json.dumps(out), mimetype="application/json")
    resp.headers["Content-Disposition"] = "attachment; filename=index.json"
    return resp

# ───────── bulk-queue endpoints ─────────
BULK_TABLE = boto3.resource("dynamodb", region_name=DYNAMO_REGION)\
                  .Table("bulkInscribeQueue1")

@app.post("/api/bulk/enqueue")
def bulk_enqueue():
    """
    Body: { blocks:[830500,830501,…] }
    Ignores duplicates, returns { added: n }.
    """
    blocks = (request.get_json(force=True) or {}).get("blocks", [])
    added  = 0
    for b in blocks:
        try:
            BULK_TABLE.put_item(
                Item={
                    "block_number": Decimal(str(b)),
                    "payload": json.dumps(build_payload(b)),
                    "delegate": DELEGATE_ID,
                    "status": "pending",
                    "created": datetime.utcnow().isoformat() + "Z"
                },
                ConditionExpression="attribute_not_exists(block_number)"
            )
            added += 1
        except boto3.client("dynamodb").exceptions.ConditionalCheckFailedException:
            pass   # duplicate
    return {"added": added}

@app.get("/api/bulk/list")
def bulk_list():
    scan = BULK_TABLE.scan()
    return scan.get("Items", [])

@app.post("/api/bulk/clear")
def bulk_clear():
    """
    Body: { status:["done","error"] }  – deletes rows with those statuses.
    """
    sts = set((request.get_json(force=True) or {}).get("status", []))
    scan = BULK_TABLE.scan(FilterExpression=Attr("status").is_in(sts))
    for it in scan.get("Items", []):
        BULK_TABLE.delete_item(Key={"block_number": it["block_number"]})
    return {"deleted": len(scan.get("Items", []))}


# ───────────── dev entrypoint ─────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)

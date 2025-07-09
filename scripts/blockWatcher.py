#!/usr/bin/env python3
"""
Block-Watcher
─────────────
• Polls Blockstream every 120 s
• For each new block whose `bits` contains “8b”:
    – inserts a row into the DynamoDB table
    – POSTs a lightweight JSON diff to DynamicIndexer so the
      front-end updates instantly via Web-Sockets
"""
import requests, boto3, time
from pathlib import Path
from datetime import datetime, timezone

# ─── config ────────────────────────────────────────────────
REGION       = "us-east-1"
TABLE_NAME   = "dynamicIndex1"
POLL_SECS    = 120
STATE_FILE   = Path(__file__).parent / "state" / "last_height.txt"

API_HEIGHT   = "https://blockstream.info/api/blocks/tip/height"
API_BLOCK    = "https://blockstream.info/api/block/{}"

# Local DynamicIndexer REST endpoint (same host)
ANNOUNCE_URL = "http://127.0.0.1:8080/api/announce-block"

# ─── init ──────────────────────────────────────────────────
table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)

# ─── helpers ───────────────────────────────────────────────
def criteria(block: dict) -> bool:
    """Return True if this block should be inserted / announced."""
    return "8b" in block.get("bits", "")

def tip_height() -> int:
    return int(requests.get(API_HEIGHT, timeout=15).text)

def block_json(h: int) -> dict:
    return requests.get(API_BLOCK.format(h), timeout=15).json()

def put_row(b: dict):
    """Insert DynamoDB row (idempotent)."""
    table.put_item(Item={
        "block_number" : str(b["height"]),
        "bits"         : b["bits"],
        "dateAvailable": datetime.now(timezone.utc).isoformat(),
        "hash"         : b["id"],
        "timestamp"    : b["timestamp"],         # unix seconds
        "status"       : "available",
    })

def announce(b: dict):
    """POST a diff to DynamicIndexer so UIs update instantly."""
    try:
        requests.post(
            ANNOUNCE_URL,
            json={
                "block"   : b["height"],
                "hash"    : b["id"],
                "mined_at": b["timestamp"],
                "status"  : "available",
            },
            timeout=2,
        )
    except Exception as e:
        print("WARN: announce failed:", e)

def load_last() -> int:
    if STATE_FILE.exists():
        return int(STATE_FILE.read_text().strip())

    h = tip_height()                      # first-ever run
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(str(h))
    return h

def save_last(h: int):
    STATE_FILE.write_text(str(h))

# ─── main loop ─────────────────────────────────────────────
def main():
    last = load_last()
    print("BlockWatcher ▶ starting at height", last)

    while True:
        try:
            tip = tip_height()
            if tip > last:
                for h in range(last + 1, tip + 1):
                    blk = block_json(h)
                    if criteria(blk):
                        put_row(blk)
                        announce(blk)
                        print("Inserted & announced", h)
                    save_last(h)
                last = tip
        except Exception as e:
            print("ERROR:", e)

        time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()

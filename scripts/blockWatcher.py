#!/usr/bin/env python3
"""
One-shot BlockWatcher
• Reads last processed height from scripts/state/last_height.txt
• Fetches every new block since then (via blockstream.info)
• Inserts a row into DynamoDB when the block’s bits (hex) contains MATCH_SUBSTRING
• Always writes the new height back to last_height.txt
Exit code 0 → systemd timer schedules the next run in 120 s.
"""

from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import requests, boto3, sys

# ────────── CONFIG ──────────
REGION           = "us-east-1"
TABLE_NAME       = "dynamicIndex1"
MATCH_SUBSTRING  = "8b".lower()       # ← change rule here
STATE_FILE       = Path(__file__).parent / "state" / "last_height.txt"
API_HEIGHT       = "https://blockstream.info/api/blocks/tip/height"
API_BLOCK_JSON   = "https://blockstream.info/api/block/{}"

# ────────── AWS TABLE ────────
table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)

# ────────── HELPERS ──────────
def tip_height() -> int:
    return int(requests.get(API_HEIGHT, timeout=15).text)

def block_json(h: int) -> dict:
    return requests.get(API_BLOCK_JSON.format(h), timeout=15).json()

def row_matches(blk: dict) -> bool:
    return MATCH_SUBSTRING in format(blk["bits"], "x")

def put_row(blk: dict):
    table.put_item(
        Item={
            "block_number":  str(blk["height"]),
            "bits":          blk["bits"],
            "dateAvailable": datetime.now(timezone.utc).isoformat(),
        }
    )

def load_last() -> int:
    if STATE_FILE.exists():
        return int(STATE_FILE.read_text().strip())
    # first run → start at current tip
    h = tip_height()
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(str(h))
    return h

def save_last(h: int):
    STATE_FILE.write_text(str(h))

# ────────── MAIN ─────────────
def main() -> None:
    last = load_last()
    print(f"[Watcher] starting at height {last}")

    try:
        tip = tip_height()
        if tip <= last:
            print("[Watcher] chain tip unchanged → exit")
            sys.exit(0)

        for h in range(last + 1, tip + 1):
            blk = block_json(h)
            if row_matches(blk):
                put_row(blk)
                print(f"[Watcher] MATCH  height={h} bits=0x{blk['bits']:x}")
            else:
                print(f"[Watcher] SKIP   height={h} bits=0x{blk['bits']:x}")

            save_last(h)

        print(f"[Watcher] processed up to {tip}")
    except Exception as exc:
        print("[Watcher] ERROR:", exc, file=sys.stderr)
        sys.exit(1)          # non-zero → systemd will log failure

if __name__ == "__main__":
    main()

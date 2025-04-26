#!/usr/bin/env python3
"""
BlockWatcher – one-shot script (Python 3.9 compatible).

Invoked by a systemd timer every N seconds (120 s by default on the timer):
  • Processes every block above the last recorded height
  • Writes rows whose `bits` contains SUBSTRING
  • Updates the height marker and exits 0 so the timer re-fires
"""

import sys, json, requests, boto3
from typing import Optional
from pathlib import Path
from datetime import datetime, timezone

REGION      = "us-east-1"
TABLE_NAME  = "dynamicIndex1"
SUBSTRING   = "8b"   # hex substring to match in `bits`

STATE_FILE  = Path(__file__).parent / "state" / "last_height.txt"
API_TIP_H   = "https://blockstream.info/api/blocks/tip/height"
API_H2HASH  = "https://blockstream.info/api/block-height/{}"
API_HASH2BL = "https://blockstream.info/api/block/{}"

table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)


def criteria(block: dict) -> bool:
    return SUBSTRING in str(block.get("bits", ""))


def tip_height() -> int:
    return int(requests.get(API_TIP_H, timeout=15).text)


def block_json(height: int) -> Optional[dict]:
    """Return full block JSON for *height*, or None if not yet available."""
    r_hash = requests.get(API_H2HASH.format(height), timeout=15)
    blk_hash = r_hash.text.strip()
    if r_hash.status_code != 200 or not blk_hash:
        print(f"skip {height}: height→hash HTTP {r_hash.status_code}")
        return None

    r_blk = requests.get(API_HASH2BL.format(blk_hash), timeout=15)
    if r_blk.status_code != 200:
        print(f"skip {height}: block HTTP {r_blk.status_code}")
        return None
    try:
        return r_blk.json()
    except json.JSONDecodeError:
        print(f"skip {height}: bad JSON")
        return None


def put_row(blk: dict):
    table.put_item(Item={
        "block_number":  str(blk["height"]),
        "bits":          blk["bits"],
        "dateAvailable": datetime.now(timezone.utc).isoformat(),
    })


def load_last() -> int:
    if STATE_FILE.exists():
        return int(STATE_FILE.read_text().strip())
    ht = tip_height()
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(str(ht))
    return ht


def save_last(h: int):
    STATE_FILE.write_text(str(h))


def main():
    last = load_last()
    print(f"BlockWatcher → starting at {last}")

    try:
        tip = tip_height()
        for h in range(last + 1, tip + 1):
            blk = block_json(h)
            if blk is None:
                break                         # retry next run
            if criteria(blk):
                put_row(blk)
                print(f"Inserted {h}")
            save_last(h)
    except Exception as exc:
        print("ERROR:", exc)

    print("Done; exiting 0")
    sys.exit(0)


if __name__ == "__main__":
    main()

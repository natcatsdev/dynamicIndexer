#!/usr/bin/env python3
"""
BlockWatcher – one-shot producer.

• Runs every ~120 s via runfast.timer
• Inserts a row the first time we see a block whose bits (hex) contains
  the substring “8b”.
• Stores bits **in hexadecimal** (e.g. 0x1d00ffff) and firstSeen timestamp.
"""

import sys, json, requests, boto3
from typing import Optional
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal

REGION, TABLE_NAME = "us-east-1", "dynamicIndex1"
SUBSTRING           = "8b"

STATE_FILE = Path(__file__).parent / "state" / "last_height.txt"
API_TIP_H  = "https://blockstream.info/api/blocks/tip/height"
API_H2HASH = "https://blockstream.info/api/block-height/{}"
API_BHASH  = "https://blockstream.info/api/block/{}"

table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)

# ─────────────── helpers ──────────────────────────────────────────────
def criteria(block: dict) -> bool:
    bits_val = block.get("bits")
    bits_hex = format(bits_val, "x") if isinstance(bits_val, int) else str(bits_val).lower()
    return SUBSTRING in bits_hex


def tip_height() -> int:
    return int(requests.get(API_TIP_H, timeout=15).text)


def block_json(h: int) -> Optional[dict]:
    r_hash = requests.get(API_H2HASH.format(h), timeout=15)
    blk_hash = r_hash.text.strip()
    if r_hash.status_code != 200 or not blk_hash:
        print(f"skip {h}: height→hash HTTP {r_hash.status_code}")
        return None

    r_blk = requests.get(API_BHASH.format(blk_hash), timeout=15)
    if r_blk.status_code != 200:
        print(f"skip {h}: block HTTP {r_blk.status_code}")
        return None
    try:
        return r_blk.json()
    except json.JSONDecodeError:
        print(f"skip {h}: bad JSON")
        return None


def put_row(blk: dict) -> None:
    """Insert row on first encounter; bits stored as hex string."""
    bits_hex = f"0x{blk['bits']:x}"
    table.put_item(Item={
        "block_number": Decimal(str(blk["height"])),   # numeric PK
        "bits":        bits_hex,                       # now hex
        "firstSeen":   datetime.now(timezone.utc).isoformat(),
        # dateAvailable written later by authLooperBackend
    })


def load_last() -> int:
    if STATE_FILE.exists():
        return int(STATE_FILE.read_text().strip())
    ht = tip_height()
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(str(ht))
    return ht


def save_last(h: int) -> None:
    STATE_FILE.write_text(str(h))

# ─────────────── main ────────────────────────────────────────────────
def main() -> None:
    last = load_last()
    print(f"BlockWatcher → starting at {last}")

    try:
        tip = tip_height()
        for h in range(last + 1, tip + 1):
            blk = block_json(h)
            if blk is None:
                continue            # skip gaps; retry next run
            if criteria(blk):
                bits_hex = f"0x{blk['bits']:x}"
                put_row(blk)
                print(f"Inserted {h}  bits-hex={bits_hex}")
            save_last(h)
    except Exception as exc:
        print("ERROR:", exc)

    print("Done; exiting 0")
    sys.exit(0)


if __name__ == "__main__":
    main()

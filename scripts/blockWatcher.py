#!/usr/bin/env python3
"""
BlockWatcher – one-shot script.
Invoked by a systemd timer every N seconds (120 s by default on the timer).
It processes every block higher than the last recorded height, stores any
that match the `bits` substring, advances the height marker, and exits 0.
"""

import sys
import requests
import boto3
from pathlib import Path
from datetime import datetime, timezone

REGION      = "us-east-1"
TABLE_NAME  = "dynamicIndex1"
SUBSTRING   = "8b"          # hex substring to match in `bits`
STATE_FILE  = Path(__file__).parent / "state" / "last_height.txt"
API_HEIGHT  = "https://blockstream.info/api/blocks/tip/height"
API_BLOCK   = "https://blockstream.info/api/block/{}"

table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)


def criteria(block: dict) -> bool:
    """Return True if the block’s `bits` field contains SUBSTRING."""
    return SUBSTRING in block.get("bits", "")


def tip_height() -> int:
    """Current chain tip height."""
    return int(requests.get(API_HEIGHT, timeout=15).text)


def block_json(h: int) -> dict:
    """Full JSON for block *h*."""
    return requests.get(API_BLOCK.format(h), timeout=15).json()


def put_row(b: dict) -> None:
    """Insert a DynamoDB row for block *b*."""
    table.put_item(Item={
        "block_number":  str(b["height"]),
        "bits":          b["bits"],
        "dateAvailable": datetime.now(timezone.utc).isoformat(),
    })


def load_last() -> int:
    """Height of the last processed block (initialises to current tip)."""
    if STATE_FILE.exists():
        return int(STATE_FILE.read_text().strip())

    ht = tip_height()
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(str(ht))
    return ht


def save_last(h: int) -> None:
    STATE_FILE.write_text(str(h))


def main() -> None:
    start_ht = load_last()
    print(f"BlockWatcher → starting at {start_ht}")

    try:
        tip = tip_height()
        if tip > start_ht:
            for h in range(start_ht + 1, tip + 1):
                blk = block_json(h)
                if criteria(blk):
                    put_row(blk)
                    print(f"Inserted {h}")
                save_last(h)

        print(f"Done (processed up to {tip}); exiting cleanly")
        sys.exit(0)
    except Exception as exc:
        print("ERROR:", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()

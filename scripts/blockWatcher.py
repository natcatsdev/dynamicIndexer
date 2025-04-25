#!/usr/bin/env python3
"""
BlockWatcher v1.1  –  ONE-SHOT version
• Reads last processed height from scripts/state/last_height.txt
• Fetches every new block since then (blockstream.info)
• Inserts a DynamoDB row when the block’s bits (hex) contains MATCH_SUBSTRING
• ALWAYS records the latest height, even when a block is skipped
• Exits after a single pass (systemd oneshot unit finishes cleanly)

The systemd timer fires this script every 120 s.
"""

from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import requests, boto3, sys, logging

# ────────── CONFIG ──────────
REGION           = "us-east-1"
TABLE_NAME       = "dynamicIndex1"
MATCH_SUBSTRING  = "8b".lower()      # ← rule: substring in bits (hex)
STATE_FILE       = Path(__file__).parent / "state" / "last_height.txt"
API_HEIGHT       = "https://blockstream.info/api/blocks/tip/height"
API_BLOCK_JSON   = "https://blockstream.info/api/block/{}"
TIMEOUT          = 15               # seconds for HTTP requests

# ────────── LOGGING ──────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("BlockWatcher")

# ────────── AWS TABLE ────────
table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)

# ────────── HELPERS ──────────
def tip_height() -> int:
    return int(requests.get(API_HEIGHT, timeout=TIMEOUT).text)

def block_json(height: int) -> dict:
    return requests.get(API_BLOCK_JSON.format(height), timeout=TIMEOUT).json()

def row_matches(bits_int: int) -> bool:
    return MATCH_SUBSTRING in format(bits_int, "x")

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

def save_last(height: int):
    STATE_FILE.write_text(str(height))

# ────────── MAIN ─────────────
def main() -> None:
    last_height = load_last()
    LOG.info("start at height %s", last_height)

    try:
        tip = tip_height()
        if tip <= last_height:
            LOG.info("chain tip unchanged (%s) → exit", tip)
            sys.exit(0)

        # process every new block once
        for h in range(last_height + 1, tip + 1):
            blk = block_json(h)
            if row_matches(blk["bits"]):
                put_row(blk)
                LOG.info("MATCH height=%s bits=0x%x", h, blk["bits"])
            else:
                LOG.info("SKIP  height=%s bits=0x%x", h, blk["bits"])

            save_last(h)

        LOG.info("processed up to %s → exit", tip)
        sys.exit(0)

    except Exception as exc:
        LOG.error("ERROR: %s", exc, exc_info=True)
        sys.exit(1)   # non-zero → systemd records failure

if __name__ == "__main__":
    main()

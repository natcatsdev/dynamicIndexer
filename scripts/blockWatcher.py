#!/usr/bin/env python3
"""
BlockWatcher v1.3 – ONE-SHOT
• Reads last processed height from scripts/state/last_height.txt
• Fetches every new block since then (via blockstream.info)
• Inserts a DynamoDB row when block.bits hex contains MATCH_SUBSTRING
• ALWAYS records the latest height
• Exits after one pass (for systemd oneshot service)
"""

from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
import requests, boto3, logging, sys

# ────────── CONFIG ──────────
REGION           = "us-east-1"
TABLE_NAME       = "dynamicIndex1"
MATCH_SUBSTRING  = "8b".lower()          # rule
STATE_FILE       = Path(__file__).parent / "state" / "last_height.txt"
API_TIP_HEIGHT   = "https://blockstream.info/api/blocks/tip/height"
API_BLOCK_HASH   = "https://blockstream.info/api/block-height/{}"   # → hash
API_BLOCK_JSON   = "https://blockstream.info/api/block/{}"          # needs hash
TIMEOUT          = 15                                               # seconds

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
    return int(requests.get(API_TIP_HEIGHT, timeout=TIMEOUT).text)

def block_json(height: int) -> dict:
    blk_hash = requests.get(API_BLOCK_HASH.format(height), timeout=TIMEOUT).text
    return requests.get(API_BLOCK_JSON.format(blk_hash), timeout=TIMEOUT).json()

def matches(bits_int: int) -> bool:
    return MATCH_SUBSTRING in format(bits_int, "x")

def put_row(blk: dict):
    table.put_item(
        Item={
            "block_number":  Decimal(str(blk["height"])),   # numeric PK
            "bits":          Decimal(str(blk["bits"])),
            "dateAvailable": datetime.now(timezone.utc).isoformat(),
        }
    )

def load_last() -> int:
    if STATE_FILE.exists():
        return int(STATE_FILE.read_text().strip())
    h = tip_height()
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(str(h))
    return h

def save_last(height: int):
    STATE_FILE.write_text(str(height))

# ────────── MAIN ─────────────
def main() -> None:
    last = load_last()
    LOG.info("start at %s", last)

    try:
        tip = tip_height()
        if tip <= last:
            LOG.info("chain tip unchanged → exit")
            sys.exit(0)

        for h in range(last + 1, tip + 1):
            blk = block_json(h)

            if matches(blk["bits"]):
                put_row(blk)
                LOG.info("MATCH height=%s bits=0x%x", h, blk["bits"])
            else:
                LOG.info("SKIP  height=%s bits=0x%x", h, blk["bits"])

            save_last(h)

        LOG.info("processed up to %s → exit", tip)
        sys.exit(0)

    except Exception as exc:
        LOG.error("ERROR: %s", exc, exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()

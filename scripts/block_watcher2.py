#!/usr/bin/env python3
"""
BlockWatcher 2 – pushes EVERY new Bitcoin block header to DynamoDB.mintBlocks
"""
import os
import time
import requests
import boto3
from pathlib import Path

# DynamoDB / table config
REGION      = os.getenv("AWS_REGION", "us-east-1")
TABLE_NAME  = os.getenv("MINT_TABLE", "mintBlocks")

# Polling config
POLL_SECS   = 120
STATE_FILE  = Path(__file__).parent / "state" / "last_height2.txt"

# Blockstream API endpoints
API_HEIGHT             = "https://blockstream.info/api/blocks/tip/height"
API_HASH_FROM_HEIGHT   = "https://blockstream.info/api/block-height/{}"
API_BLOCK              = "https://blockstream.info/api/block/{}"

# Initialize DynamoDB table resource
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table    = dynamodb.Table(TABLE_NAME)

def tip_height() -> int:
    """Get the current chain tip height."""
    return int(requests.get(API_HEIGHT, timeout=15).text)

def fetch_block(h: int) -> dict:
    """
    Fetch full block data by height:
      1) get block hash string for the height
      2) fetch the block header JSON by that hash
    """
    # 1) lookup hash from height
    blk_hash = requests.get(API_HASH_FROM_HEIGHT.format(h), timeout=15).text.strip()
    # 2) fetch full header by hash
    return requests.get(API_BLOCK.format(blk_hash), timeout=15).json()

def write_block(b: dict):
    """
    Put the block record into DynamoDB with all desired attributes:
      - block        (PK, Number)
      - hash         (String)
      - mined_at     (Number, Unix ts)
      - added_at     (Number, ms since epoch)
      - status       (String, initial "available")
      - inscription_id (String, empty until inscribed)
    """
    now_ms = int(time.time() * 1000)
    table.put_item(Item={
        "block":          b["height"],            # partition key
        "hash":           b["id"],
        "mined_at":       b["timestamp"],
        "added_at":       now_ms,
        "status":         "available",
        "inscription_id": ""
    })

def load_last() -> int:
    """Read last-height from disk, or bootstrap to current tip."""
    if STATE_FILE.exists():
        return int(STATE_FILE.read_text())
    # First run: start at current tip but don’t backfill
    h = tip_height()
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(str(h))
    return h

def save_last(h: int):
    """Persist last-height to disk."""
    STATE_FILE.write_text(str(h))

def main():
    last = load_last()
    print(f"BlockWatcher2 starting at {last}")
    while True:
        try:
            tip = tip_height()
            if tip > last:
                for h in range(last + 1, tip + 1):
                    blk = fetch_block(h)
                    write_block(blk)
                    print(f"→ wrote block {h}")
                    save_last(h)
                last = tip
        except Exception as e:
            print("ERROR:", e)
        time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()

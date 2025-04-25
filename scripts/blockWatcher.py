#!/usr/bin/env python3
# Polls Blockstream every 120 s, inserts rows whose `bits` includes "3b".
import requests, boto3, time
from pathlib import Path
from datetime import datetime, timezone

REGION      = "us-east-1"
TABLE_NAME  = "dynamicIndex1"
POLL_SECS   = 120
STATE_FILE  = Path(__file__).parent / "state" / "last_height.txt"
API_HEIGHT  = "https://blockstream.info/api/blocks/tip/height"
API_BLOCK   = "https://blockstream.info/api/block/{}"

table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)

def criteria(block: dict) -> bool:
    return "3b" in block.get("bits", "")

def tip_height() -> int:
    return int(requests.get(API_HEIGHT, timeout=15).text)

def block_json(h: int) -> dict:
    return requests.get(API_BLOCK.format(h), timeout=15).json()

def put_row(b: dict):
    table.put_item(Item={
        "block_number":  str(b["height"]),
        "bits":          b["bits"],
        "dateAvailable": datetime.now(timezone.utc).isoformat(),
    })

def load_last() -> int:
    if STATE_FILE.exists():
        return int(STATE_FILE.read_text().strip())
    h = tip_height()                         # first-ever run
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(str(h))
    return h

def save_last(h: int):
    STATE_FILE.write_text(str(h))

def main():
    last = load_last()
    print("BlockWatcher → starting at", last)
    while True:
        try:
            tip = tip_height()
            if tip > last:
                for h in range(last + 1, tip + 1):
                    blk = block_json(h)
                    if criteria(blk):
                        put_row(blk)
                        print("Inserted", h)
                    save_last(h)
                last = tip
        except Exception as e:
            print("ERROR:", e)
        time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()

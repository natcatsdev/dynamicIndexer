#!/usr/bin/env python3
import os, time, requests, boto3
from pathlib import Path
from boto3.dynamodb.conditions import Attr

# ── config ──────────────────────────────────────────────────
REGION      = os.getenv("AWS_REGION",  "us-east-1")
TABLE_NAME  = os.getenv("MINT_TABLE",  "mintBlocks")
POLL_SECS   = int(os.getenv("POLL_SECS", 120))
STATE_FILE  = Path(__file__).parent / "state" / "last_height2.txt"

API_HEIGHT   = "https://blockstream.info/api/blocks/tip/height"
API_HASH_H   = "https://blockstream.info/api/block-height/{}"
API_BLOCK    = "https://blockstream.info/api/block/{}"
API_TX_STAT  = "https://blockstream.info/api/tx/{}/status"

ANNOUNCE_URL = "http://127.0.0.1:8080/api/announce-block"

# ── AWS & helpers ───────────────────────────────────────────
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table    = dynamodb.Table(TABLE_NAME)

tip_height = lambda: int(requests.get(API_HEIGHT, timeout=15).text)
fetch_block = lambda h: requests.get(
    API_BLOCK.format(requests.get(API_HASH_H.format(h), timeout=15).text.strip()),
    timeout=15,
).json()

def write_block(b):
    table.put_item(Item={
        "block": b["height"],
        "hash": b["id"],
        "mined_at": b["timestamp"],
        "added_at": int(time.time()*1000),
        "status": "available",
        "inscription_id": "",
        "confirmed": False,
    })

def announce(diff: dict):
    try:
        requests.post(ANNOUNCE_URL, json=diff, timeout=2).raise_for_status()
        print("ANNOUNCE", diff)
    except Exception as e:
        print("WARN announce-block:", e)

def tx_confirmed(txid: str) -> bool:
    r = requests.get(API_TX_STAT.format(txid), timeout=15)
    return r.ok and r.json().get("confirmed", False)

# ── scan every run (verbose) ─────────────────────────────────
def confirm_pending_inscriptions():
    scan_kw = {
        "FilterExpression":
            Attr("inscription_id").ne("") &
            (Attr("confirmed").not_exists() | Attr("confirmed").eq(False)),
        # ↓ alias the reserved word “block”
        "ProjectionExpression": "#b, inscription_id",
        "ExpressionAttributeNames": {"#b": "block"},
    }
    print("\nSCAN start ↓")
    start = None
    count = 0
    while True:
        if start:
            scan_kw["ExclusiveStartKey"] = start
        resp   = table.scan(**scan_kw)
        items  = resp.get("Items", [])
        count += len(items)

        for it in items:
            blk  = int(it["block"])
            txid = it["inscription_id"].split("i")[0]
            ok   = tx_confirmed(txid)
            print(f"  • block {blk}  tx {txid[:8]}… confirmed={ok}")
            if ok:
                table.update_item(
                    Key={"block": blk},
                    UpdateExpression="SET confirmed = :t",
                    ExpressionAttributeValues={":t": True},
                )
                announce({"block": blk, "confirmed": True})
                print("    ✓ flipped to confirmed=True")

        start = resp.get("LastEvaluatedKey")
        if not start:
            break
    print(f"SCAN done – candidates: {count}\n")

# ── state helpers ───────────────────────────────────────────
def load_last():
    if STATE_FILE.exists():
        return int(STATE_FILE.read_text())
    h = tip_height()
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(str(h))
    return h

def save_last(h: int):
    STATE_FILE.write_text(str(h))

# ── main loop ───────────────────────────────────────────────
def main():
    last = load_last()
    print("BlockWatcher2 ▶ starting at height", last)
    while True:
        try:
            tip = tip_height()
            if tip > last:
                for h in range(last + 1, tip + 1):
                    blk = fetch_block(h)
                    write_block(blk)
                    announce({
                        "block": blk["height"],
                        "hash":  blk["id"],
                        "mined_at": blk["timestamp"],
                        "status": "available",
                    })
                    print("→ wrote block", h)
                    save_last(h)
                last = tip

            # runs every POLL_SECS (120 s by default)
            confirm_pending_inscriptions()

        except Exception as e:
            print("ERROR:", e)
        time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()

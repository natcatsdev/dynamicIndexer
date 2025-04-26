#!/usr/bin/env python3
"""
matchChecker – add item.match = true / false when bits (hex) contains '3b'.
Runs once per BlockWatcher batch, updates only rows that both:
    • have a `bits` attribute
    • lack a `match` attribute
Then exits 0.
"""

import time, logging, boto3
from boto3.dynamodb.conditions import Attr

REGION      = "us-east-1"
TABLE_NAME  = "dynamicIndex1"
SUBSTRING   = "3b"
SCAN_LIMIT  = 500

# ─── logging ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("matchChecker")

table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)

def bits_contains_sub(bits_val) -> bool:
    if bits_val is None:
        return False
    hex_str = format(bits_val, "x") if isinstance(bits_val, int) else str(bits_val).lower()
    return SUBSTRING in hex_str

def main():
    start = time.time()
    scanned = updated = 0

    scan_kwargs = {
        "FilterExpression": Attr("match").not_exists() & Attr("bits").exists(),
        "ProjectionExpression": "block_number, bits",
        "Limit": SCAN_LIMIT,
    }

    resp = table.scan(**scan_kwargs)
    while True:
        for item in resp.get("Items", []):
            scanned += 1
            flag = bits_contains_sub(item["bits"])
            try:
                table.update_item(
                    Key={"block_number": int(item["block_number"])},
                    UpdateExpression="SET #m = :v",
                    ExpressionAttributeNames={"#m": "match"},
                    ExpressionAttributeValues={":v": flag},
                )
                updated += 1
            except Exception as exc:
                log.error("Update failed for block %s: %s", item["block_number"], exc)

        if "LastEvaluatedKey" not in resp:
            break
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **scan_kwargs)

    log.info("Done in %.2fs – scanned %d, updated %d",
             time.time() - start, scanned, updated)

if __name__ == "__main__":
    main()

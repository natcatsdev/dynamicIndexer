#!/usr/bin/env python3
"""
matchChecker – add item.match = true/false when bits (hex) contains '3b'.
Runs once, updates only rows that don’t yet have a `match` field, then exits 0.
"""

import time, logging, boto3
from boto3.dynamodb.conditions import Attr

# ─── Config ────────────────────────────────────────────────────────────
REGION      = "us-east-1"
TABLE_NAME  = "dynamicIndex1"
SUBSTRING   = "3b"                      # ← change criteria here if needed
SCAN_LIMIT  = 500                       # items per DynamoDB scan page

# ─── Logging setup ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("matchChecker")

# ─── DynamoDB table handle ─────────────────────────────────────────────
table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)

# ─── Helper ────────────────────────────────────────────────────────────
def bits_contains_3b(bits_val) -> bool:
    hex_str = format(bits_val, "x") if isinstance(bits_val, int) else str(bits_val).lower()
    return SUBSTRING in hex_str

# ─── Main routine ──────────────────────────────────────────────────────
def main():
    start = time.time()
    scanned = updated = 0

    log.info("matchChecker started")

    scan_kwargs = {
        "FilterExpression": Attr("match").not_exists(),
        "ProjectionExpression": "block_number, bits",  # fetch only needed fields
        "Limit": SCAN_LIMIT,
    }

    resp = table.scan(**scan_kwargs)
    while True:
        items = resp.get("Items", [])
        for it in items:
            scanned += 1
            flag = bits_contains_3b(it["bits"])
            try:
                table.update_item(
                    Key={"block_number": int(it["block_number"])},
                    UpdateExpression="SET #m = :v",
                    ExpressionAttributeNames={"#m": "match"},
                    ExpressionAttributeValues={":v": flag},
                )
                updated += 1
            except Exception as exc:
                log.error("Update failed for block %s: %s", it["block_number"], exc)

        if "LastEvaluatedKey" not in resp:
            break
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **scan_kwargs)

    elapsed = time.time() - start
    log.info("Done in %.2fs – scanned %d, updated %d", elapsed, scanned, updated)

if __name__ == "__main__":
    main()

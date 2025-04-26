#!/usr/bin/env python3
from __future__ import annotations

# lock-file cleanup
import os, atexit
LOCK_FILE = os.getenv("AUTH_LOCK_FILE")
if LOCK_FILE:
    atexit.register(lambda: os.remove(LOCK_FILE)
                    if os.path.exists(LOCK_FILE) else None)

# std-lib & deps
import json, time, datetime, logging, requests, boto3
from boto3.dynamodb.conditions import Attr
from playwright.sync_api import sync_playwright

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(),
              logging.FileHandler("authLooperBackend.log")]
)
log = logging.getLogger("authLooperBackend")

# dynamodb
table = boto3.resource("dynamodb", region_name="us-east-1") \
             .Table("dynamicIndex1")

# helpers ---------------------------------------------------------------
def fetch_block_mined_iso(h: int) -> str | None:
    try:
        bhash = requests.get(f"https://blockstream.info/api/block-height/{h}",
                             timeout=10).text.strip()
        ts = requests.get(f"https://blockstream.info/api/block/{bhash}",
                          timeout=10).json().get("timestamp", 0)
        return datetime.datetime.utcfromtimestamp(ts).isoformat() + "Z"
    except Exception as e:
        log.error("mined-date fetch failed for %s: %s", h, e)
        return None

def fetch_al_for_block(h: int) -> tuple[str, str]:
    url = ("https://ordinals.com/inscription/"
           "66475024139f5a7500b48ac688a7418fdf5838a7eabbc7e6792b7dc7829c8ef7i0")
    with sync_playwright() as p:
        page = p.chromium.launch(headless=True).new_page()
        try:
            page.goto(url); page.wait_for_load_state("networkidle")
            frame = next((f for f in page.frames if "/preview/" in f.url), None)
            if not frame:
                raise RuntimeError("iframe missing")
            frame.fill("#blockAL", str(h)); frame.click("#alButton"); time.sleep(2)
            txt = frame.inner_text("#alOutput").strip()
        except Exception as e:
            return str(h), "ERROR"
    try:
        data = json.loads(txt)
        return str(data.get("block", h)), str(data.get("authorizedParent","invalid"))
    except Exception as e:
        log.error("JSON parse error for block %s: %s", h, e)
        return str(h), "PARSE_ERROR"

# main ------------------------------------------------------------------
def main():
    log.info("authLooperBackend started")
    while True:
        try:
            resp = table.scan(
                FilterExpression=Attr("authParent").not_exists() |
                                 Attr("authParent").eq("")
            )
        except Exception as e:
            log.error("scan error: %s", e); break

        items = resp.get("Items", [])
        if not items:
            log.info("No blocks pending – exit."); break

        blk = min(items, key=lambda x:int(x["block_number"]))["block_number"]
        log.info("Processing block %s", blk)

        blk_str, parent = fetch_al_for_block(int(blk))
        log.info("Result: authParent=%s", parent)

        date_iso = fetch_block_mined_iso(int(blk))

        # single update: authParent (+ dateAvailable if fetched)
        expr  = "SET authParent = :a"
        vals  = {":a": parent}
        if date_iso:
            expr += ", dateAvailable = :d"; vals[":d"] = date_iso

        try:
            table.update_item(Key={"block_number": int(blk_str)},
                              UpdateExpression=expr,
                              ExpressionAttributeValues=vals)
        except Exception as e:
            log.error("update failed for block %s: %s", blk_str, e)

        time.sleep(1)           # be polite to external APIs
    log.info("authLooperBackend completed.")

if __name__ == "__main__":
    main()

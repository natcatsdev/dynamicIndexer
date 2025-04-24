#!/usr/bin/env python3
# --------------------------------------------------------------
# authLooperBackend.py
# Fills in `authParent` and `dateAvailable` for blocks that lack them.
# --------------------------------------------------------------
from __future__ import annotations     # ← enables PEP 604 unions on Py 3.9

# ---- lock-file cleanup -------------------------------------------------
import os, atexit
LOCK_FILE = os.getenv("AUTH_LOCK_FILE")
if LOCK_FILE:
    atexit.register(lambda: os.remove(LOCK_FILE)
                    if os.path.exists(LOCK_FILE) else None)

# -----------------------------------------------------------------------
# Standard libs & deps
# -----------------------------------------------------------------------
import json
import time
import datetime
import logging
import requests
import boto3
from boto3.dynamodb.conditions import Attr
from playwright.sync_api import sync_playwright

# -----------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------
logger = logging.getLogger("authLooperBackend")
logger.setLevel(logging.DEBUG)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_console = logging.StreamHandler(); _console.setFormatter(_fmt)
_file    = logging.FileHandler("authLooperBackend.log"); _file.setFormatter(_fmt)

logger.addHandler(_console)
logger.addHandler(_file)

# -----------------------------------------------------------------------
# DynamoDB
# -----------------------------------------------------------------------
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table    = dynamodb.Table("dynamicIndex1")

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------
def fetch_block_mined_iso(block_height: int) -> str | None:
    """Return the block’s mined-time as an ISO-8601 UTC string (or None)."""
    try:
        h = requests.get(
            f"https://blockstream.info/api/block-height/{block_height}", timeout=10
        )
        h.raise_for_status()
        block_hash = h.text.strip()

        info = requests.get(
            f"https://blockstream.info/api/block/{block_hash}", timeout=10
        )
        info.raise_for_status()
        ts = info.json().get("timestamp")          # epoch-seconds
        return datetime.datetime.utcfromtimestamp(ts).isoformat() + "Z"
    except Exception as exc:
        logger.error("Error fetching mined date for block %s: %s", block_height, exc)
        return None


def fetch_al_for_block(block_num: int | str) -> tuple[str, str]:
    """
    Resolve authorised parent via ordinals.com’s hidden ‘AL’ widget.
    Returns (block_number, authorised_parent | error_flag)
    """
    url = ("https://ordinals.com/inscription/"
           "66475024139f5a7500b48ac688a7418fdf5838a7eabbc7e6792b7dc7829c8ef7i0")
    logger.debug("Launching Playwright to load URL: %s", url)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()
        try:
            page.goto(url); page.wait_for_load_state("networkidle")
        except Exception as exc:
            logger.error("Error loading page: %s", exc)
            browser.close(); return str(block_num), "PAGE_LOAD_ERROR"

        frame = next((f for f in page.frames if "/preview/" in f.url), None)
        if not frame:
            browser.close(); return str(block_num), "NO_IFRAME"

        try:
            frame.wait_for_selector("#blockAL", timeout=5_000)
            frame.fill("#blockAL", str(block_num))
            frame.click("#alButton")
            time.sleep(2)
            result_text = frame.inner_text("#alOutput").strip()
        except Exception as exc:
            browser.close(); return str(block_num), f"INTERACTION_ERROR:{exc}"

        browser.close()

    # parse JSON result
    try:
        data      = json.loads(result_text)
        blk_str   = str(data.get("block", block_num))
        parent_id = str(data.get("authorizedParent", "invalid"))
        return blk_str, parent_id
    except Exception as exc:
        logger.error("JSON parse error for block %s: %s", block_num, exc)
        return str(block_num), result_text

# -----------------------------------------------------------------------
# Main loop
# -----------------------------------------------------------------------
def main() -> None:
    logger.info("Starting authLooperBackend execution")

    while True:
        # 1) scan for blocks missing authParent
        try:
            response = table.scan(
                FilterExpression=Attr("authParent").not_exists() |
                                 Attr("authParent").eq("")
            )
        except Exception as exc:
            logger.error("Error scanning DynamoDB: %s", exc)
            break

        items = response.get("Items", [])
        if not items:
            logger.info("No blocks to process – exiting.")
            break

        # earliest block first
        current = min(items, key=lambda x: int(x["block_number"]))
        blk = current["block_number"]
        logger.info("Processing block %s", blk)

        # 2) resolve authParent
        _, parent_str = fetch_al_for_block(int(blk))
        logger.info("Block %s: authParent = %s", blk, parent_str)

        # 3) store authParent + timestamp
        now_iso = datetime.datetime.utcnow().isoformat() + "Z"
        try:
            table.update_item(
                Key={"block_number": int(blk)},                     # Number PK
                UpdateExpression="SET authParent = :a, lastProcessedAt = :t",
                ExpressionAttributeValues={":a": parent_str, ":t": now_iso},
            )
        except Exception as exc:
            logger.error("Failed to update authParent for block %s: %s", blk, exc)

        # 4) mined-time for THIS block
        date_iso = fetch_block_mined_iso(int(blk))
        if date_iso:
            try:
                table.update_item(
                    Key={"block_number": int(blk)},                 # Number PK
                    UpdateExpression="SET dateAvailable = :d",
                    ExpressionAttributeValues={":d": date_iso},
                )
                logger.info("Block %s: wrote dateAvailable = %s", blk, date_iso)
            except Exception as exc:
                logger.error("Error writing dateAvailable for block %s: %s", blk, exc)

        time.sleep(1)  # polite delay to external services

    logger.info("authLooperBackend completed.")
    print(json.dumps({"message": "Processing completed"}))

# -----------------------------------------------------------------------
if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# --------------------------------------------------------------
# authLooperBackend.py
# Fills in `authParent`, `authStatus`, and `dateAvailable` for
# blocks that currently lack an authParent entry.
# --------------------------------------------------------------
from __future__ import annotations          # PEP 604 unions (Py 3.9)

# ---- lock-file cleanup -------------------------------------------------
import os, atexit
LOCK_FILE = os.getenv("AUTH_LOCK_FILE")
if LOCK_FILE:
    atexit.register(lambda: os.remove(LOCK_FILE)
                    if os.path.exists(LOCK_FILE) else None)

# -----------------------------------------------------------------------
# Standard libs & deps
# -----------------------------------------------------------------------
import json, time, datetime, logging, requests, boto3
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
logger.addHandler(_console); logger.addHandler(_file)

# -----------------------------------------------------------------------
# DynamoDB
# -----------------------------------------------------------------------
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table    = dynamodb.Table("dynamicIndex1")

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------
def fetch_block_mined_iso(block_height: int) -> str | None:
    """Return the block’s mined-time ISO-8601 string, or None."""
    try:
        h = requests.get(f"https://blockstream.info/api/block-height/{block_height}",
                         timeout=10)
        h.raise_for_status()
        block_hash = h.text.strip()

        info = requests.get(f"https://blockstream.info/api/block/{block_hash}",
                            timeout=10)
        info.raise_for_status()
        ts = info.json().get("timestamp", 0)
        return datetime.datetime.utcfromtimestamp(ts).isoformat() + "Z"
    except Exception as exc:
        logger.error("Failed to fetch mined date for %s: %s", block_height, exc)
        return None


def fetch_al_for_block(height: int) -> tuple[str, str]:
    """
    Returns (block_number_str, authorised_parent | error_flag)
    Uses ordinals.com hidden AL widget via Playwright.
    """
    url = ("https://ordinals.com/inscription/"
           "66475024139f5a7500b48ac688a7418fdf5838a7eabbc7e6792b7dc7829c8ef7i0")
    logger.debug("Launching headless Chromium for AL lookup %s", height)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()
        try:
            page.goto(url); page.wait_for_load_state("networkidle")
            frame = next((f for f in page.frames if "/preview/" in f.url), None)
            if not frame:
                raise RuntimeError("no preview iframe")

            frame.wait_for_selector("#blockAL", timeout=5_000)
            frame.fill("#blockAL", str(height)); frame.click("#alButton")
            time.sleep(2)
            result_text = frame.inner_text("#alOutput").strip()
        except Exception as exc:
            browser.close(); return str(height), f"INTERACT_ERR:{exc}"
        browser.close()

    try:
        data = json.loads(result_text)
        return str(data.get("block", height)), str(data.get("authorizedParent","invalid"))
    except Exception as exc:
        logger.error("JSON parse error for block %s: %s", height, exc)
        return str(height), "PARSE_ERROR"

# -----------------------------------------------------------------------
# Main loop
# -----------------------------------------------------------------------
def main() -> None:
    logger.info("authLooperBackend started")

    while True:
        # 1) fetch next block needing authParent
        try:
            response = table.scan(
                FilterExpression=Attr("authParent").not_exists() |
                                 Attr("authParent").eq("")
            )
        except Exception as exc:
            logger.error("Scan error: %s", exc)
            break

        items = response.get("Items", [])
        if not items:
            logger.info("All done – exiting.")
            break

        blk = min(items, key=lambda x: int(x["block_number"]))["block_number"]
        logger.info("Processing block %s", blk)

        # 2) resolve authorised parent
        blk_str, parent_val = fetch_al_for_block(int(blk))
        status_val = "ok" if parent_val not in ("invalid", "PARSE_ERROR") else "error"
        logger.info("Block %s authParent=%s status=%s", blk_str, parent_val, status_val)

        # 3) mined-time for this block
        date_iso = fetch_block_mined_iso(int(blk))

        # 4) single atomic write: parent + status + optional date
        update_expr = "SET authParent = :a, authStatus = :s"
        expr_vals   = {":a": parent_val, ":s": status_val}
        if date_iso:
            update_expr += ", dateAvailable = :d"
            expr_vals[":d"] = date_iso

        try:
            table.update_item(Key={"block_number": int(blk_str)},
                              UpdateExpression=update_expr,
                              ExpressionAttributeValues=expr_vals)
        except Exception as exc:
            logger.error("Dynamo update failed for %s: %s", blk_str, exc)

        time.sleep(1)  # courteous delay to external APIs

    logger.info("authLooperBackend completed.")
    print(json.dumps({"message": "Processing completed"}))

# -----------------------------------------------------------------------
if __name__ == "__main__":
    main()

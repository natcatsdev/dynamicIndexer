#!/usr/bin/env python3
# --------------------------------------------------------------
# indexLooper.py – fills inscriptionID / inscriptionTimestamp
# + inscriptionNumber for rows that already have authParent
# --------------------------------------------------------------

import json
import time
import datetime
import logging
import requests
import boto3
from boto3.dynamodb.conditions import Attr
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

ENABLE_DATE_FETCH = True
HIRO_API_BASE     = "https://api.hiro.so/ordinals/v1/inscriptions"
WAIT_MAX_SEC      = 20
WAIT_POLL_SEC     = 0.5

# ───────────────────────────────────────────────────────────────
# Logging
# ───────────────────────────────────────────────────────────────
log = logging.getLogger("indexLooper")
log.setLevel(logging.DEBUG)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
for h in (logging.StreamHandler(), logging.FileHandler("indexLooper.log")):
    h.setFormatter(_fmt)
    log.addHandler(h)

# ───────────────────────────────────────────────────────────────
# DynamoDB
# ───────────────────────────────────────────────────────────────
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table    = dynamodb.Table("dynamicIndex1")

# ───────────────────────────────────────────────────────────────
# ordinals.com fallback widget
# ───────────────────────────────────────────────────────────────
def fetch_il_for_block(block_num: int | str) -> tuple[str, str]:
    """
    Uses the hidden “IL” widget on ordinals.com to resolve the
    minted‑inscription that corresponds to `block_num`.
    Returns (block_number, inscription_id | error_flag)
    """
    url = ("https://ordinals.com/inscription/"
           "66475024139f5a7500b48ac688a7418fdf5838a7eabbc7e6792b7dc7829c8ef7i0")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()
        try:
            page.goto(url)
            page.wait_for_load_state("networkidle")
        except Exception:
            browser.close()
            return str(block_num), "PAGE_LOAD_ERROR"

        frame = next((f for f in page.frames if "/preview/" in f.url), None)
        if not frame:
            browser.close()
            return str(block_num), "NO_IFRAME"

        try:
            frame.wait_for_selector("#blockIL", timeout=5_000)
            frame.fill("#blockIL", str(block_num))
            frame.click("#ilButton")
        except PWTimeout:
            browser.close()
            return str(block_num), "NO_FALLBACK_UI"
        except Exception as exc:
            browser.close()
            return str(block_num), f"UI_ERROR:{exc}"

        deadline = time.time() + WAIT_MAX_SEC
        while time.time() < deadline:
            out = frame.inner_text("#ilOutput").strip()
            if out and not out.lower().startswith("running"):
                break
            time.sleep(WAIT_POLL_SEC)
        else:
            browser.close()
            return str(block_num), "TIMEOUT"

        browser.close()

    if out.startswith('"') and out.endswith('"'):
        out = out[1:-1]

    try:
        data = json.loads(out)
        return (
            str(data.get("block", block_num)),
            str(data.get("mintedInscription", "error")),
        )
    except Exception:
        return str(block_num), out

# ───────────────────────────────────────────────────────────────
# Hiro helpers
# ───────────────────────────────────────────────────────────────
def fetch_ts_and_number(insc_id: str) -> tuple[int | None, int | None]:
    try:
        r = requests.get(f"{HIRO_API_BASE}/{insc_id}", timeout=10)
        r.raise_for_status()
        j   = r.json()
        ts  = j.get("timestamp")
        num = j.get("number")
        ts  = ts // 1000 if isinstance(ts, int) else None
        num = int(num)   if isinstance(num, int) else None
        return ts, num
    except Exception as exc:
        log.error("Hiro API error for %s: %s", insc_id, exc)
        return None, None

# ───────────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────────
def main() -> None:
    log.info("indexLooper start")

    try:
        scan = table.scan(
            FilterExpression=Attr("authParent").exists()
            & (
                Attr("inscriptionID").not_exists()
                | Attr("inscriptionID").eq("")
                | Attr("inscriptionID").eq("None")
            )
        )
    except Exception as exc:
        log.error("scan error: %s", exc)
        return

    items = scan.get("Items", [])
    if not items:
        log.info("nothing to do")
        return

    for it in sorted(items, key=lambda x: int(x["block_number"])):
        blk = it["block_number"]
        log.info("block %s", blk)

        # ---------- resolve inscriptionID ----------
        _, insc_id = fetch_il_for_block(int(blk))

        now = datetime.datetime.utcnow().isoformat() + "Z"
        table.update_item(
            Key={"block_number": int(blk)},                     # ***** Number PK *****
            UpdateExpression="SET inscriptionID=:i, lastProcessedAt=:t",
            ExpressionAttributeValues={
                ":i": insc_id,
                ":t": now,
            },
        )

        # ---------- optional Hiro enrichment ----------
        if ENABLE_DATE_FETCH and not insc_id.startswith("NO_"):
            ts, num = fetch_ts_and_number(insc_id)
            expr_parts, vals = [], {}
            if ts is not None:
                expr_parts.append("inscriptionTimestamp=:ts")
                vals[":ts"] = ts
            if num is not None:
                expr_parts.append("inscriptionNumber=:n")
                vals[":n"] = num
            if expr_parts:
                table.update_item(
                    Key={"block_number": int(blk)},             # ***** Number PK *****
                    UpdateExpression="SET " + ", ".join(expr_parts),
                    ExpressionAttributeValues=vals,
                )

        time.sleep(1)

    log.info("indexLooper done")

if __name__ == "__main__":
    main()

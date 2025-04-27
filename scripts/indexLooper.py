#!/usr/bin/env python3
"""
indexLooper.py – heavy inscription indexer

• Runs every 30 min via runslow.timer
• For each row that already has authParent but no inscriptionID, resolves it
  through the hidden “IL” widget on ordinals.com.
• Writes:
    inscriptionID
    inscriptionTimestamp  (via Hiro API, optional)
    inscriptionNumber     (via Hiro API, optional)
    lastProcessedAt       (always)
• Does NOT modify firstSeen or dateAvailable.
"""
from __future__ import annotations   # PEP-604 unions on Py 3.9

import os, atexit, json, time, datetime, logging, boto3, requests
from pathlib import Path
from boto3.dynamodb.conditions import Attr
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ─────────────── lock-file (for UI) ───────────────────────────
LOCK_FILE = os.getenv("INDEX_LOCK_FILE")        # set by app.py
if LOCK_FILE:
    atexit.register(lambda: Path(LOCK_FILE).unlink(missing_ok=True))

# ─────────────── configuration ────────────────────────────────
ENABLE_DATE_FETCH = True
HIRO_API_BASE     = "https://api.hiro.so/ordinals/v1/inscriptions"
WAIT_MAX_SEC      = 20
WAIT_POLL_SEC     = 0.5

# ─────────────── logging setup ────────────────────────────────
log = logging.getLogger("indexLooper")
log.setLevel(logging.DEBUG)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
for h in (logging.StreamHandler(),
          logging.FileHandler("indexLooper.log")):
    h.setFormatter(_fmt)
    log.addHandler(h)

# ─────────────── DynamoDB table ───────────────────────────────
table = boto3.resource("dynamodb", region_name="us-east-1") \
             .Table("dynamicIndex1")

# ─────────────── ordinals.com IL widget helper ────────────────
def fetch_il_for_block(block_num: int | str) -> tuple[str, str]:
    """
    Use the hidden “IL” widget to map a block to its minted inscription.
    Returns (block_str, inscription_id | error_flag).
    """
    url = ("https://ordinals.com/inscription/"
           "66475024139f5a7500b48ac688a7418fdf5838a7eabbc7e6792b7dc7829c8ef7i0")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()
        try:
            page.goto(url); page.wait_for_load_state("networkidle")
        except Exception:
            browser.close(); return str(block_num), "PAGE_LOAD_ERROR"

        frame = next((f for f in page.frames if "/preview/" in f.url), None)
        if not frame:
            browser.close(); return str(block_num), "NO_IFRAME"

        try:
            frame.wait_for_selector("#blockIL", timeout=5_000)
            frame.fill("#blockIL", str(block_num))
            frame.click("#ilButton")
        except PWTimeout:
            browser.close(); return str(block_num), "NO_FALLBACK_UI"
        except Exception as exc:
            browser.close(); return str(block_num), f"UI_ERROR:{exc}"

        deadline = time.time() + WAIT_MAX_SEC
        while time.time() < deadline:
            out = frame.inner_text("#ilOutput").strip()
            if out and not out.lower().startswith("running"):
                break
            time.sleep(WAIT_POLL_SEC)
        else:
            browser.close(); return str(block_num), "TIMEOUT"

        browser.close()

    if out.startswith('"') and out.endswith('"'):
        out = out[1:-1]          # strip quotes sometimes returned

    try:
        j = json.loads(out)      # widget sometimes returns JSON
        return str(j.get("block", block_num)), str(j.get("mintedInscription"))
    except Exception:
        return str(block_num), out

# ─────────────── Hiro API helper ──────────────────────────────
def fetch_ts_and_number(insc_id: str) -> tuple[int | None, int | None]:
    try:
        r = requests.get(f"{HIRO_API_BASE}/{insc_id}", timeout=10)
        r.raise_for_status()
        j   = r.json()
        ts  = j.get("timestamp")      # millis
        num = j.get("number")
        return (ts // 1000 if isinstance(ts, int) else None,
                int(num) if isinstance(num, int) else None)
    except Exception as exc:
        log.error("Hiro API error for %s: %s", insc_id, exc)
        return None, None

# ─────────────── main loop ────────────────────────────────────
def main() -> None:
    log.info("indexLooper start")

    try:
        scan = table.scan(
            FilterExpression=Attr("authParent").exists() &
            (
                Attr("inscriptionID").not_exists() |
                Attr("inscriptionID").eq("") |
                Attr("inscriptionID").eq("None")
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

        # -------- resolve inscriptionID via IL widget ----------
        _, insc_id = fetch_il_for_block(int(blk))

        # Skip clearly invalid results
        if not insc_id or insc_id.lower() in {"none", "error", "timeout"} \
           or insc_id.startswith(("NO_", "PAGE_", "UI_", "TIMEOUT")):
            log.warning("block %s invalid inscriptionID: %s", blk, insc_id)
            table.update_item(
                Key={"block_number": int(blk)},
                UpdateExpression="SET lastProcessedAt = :t",
                ExpressionAttributeValues={":t": datetime.datetime.utcnow().isoformat() + "Z"},
            )
            time.sleep(1)
            continue

        # Valid ID → write inscriptionID and lastProcessedAt
        now_iso = datetime.datetime.utcnow().isoformat() + "Z"
        table.update_item(
            Key={"block_number": int(blk)},
            UpdateExpression="SET inscriptionID = :i, lastProcessedAt = :t",
            ExpressionAttributeValues={":i": insc_id, ":t": now_iso},
        )

        # -------- optional Hiro enrichment ---------------------
        if ENABLE_DATE_FETCH:
            ts, num = fetch_ts_and_number(insc_id)
            expr, vals = [], {}
            if ts is not None:
                expr.append("inscriptionTimestamp = :ts"); vals[":ts"] = ts
            if num is not None:
                expr.append("inscriptionNumber = :n");    vals[":n"]  = num
            if expr:
                table.update_item(
                    Key={"block_number": int(blk)},
                    UpdateExpression="SET " + ", ".join(expr),
                    ExpressionAttributeValues=vals,
                )

        time.sleep(1)  # politeness delay to external services

    log.info("indexLooper done")

if __name__ == "__main__":
    main()

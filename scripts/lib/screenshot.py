"""Screenshots for scripts/obs-evidence.sh: loads a page, waits until it shows what it must, then captures it.

    python3 screenshot.py URL OUT.png WIDTH HEIGHT SELECTOR MIN_COUNT [TEXT,TEXT,...]

Waits (at most 90 s) until at least MIN_COUNT elements match SELECTOR and the page's text contains every TEXT, then
5 s more for late panels; exits non-zero if that never happens, so a blank or half-loaded page is never saved.
"""
import sys

from playwright.sync_api import sync_playwright

url, out, width, height, selector, minimum = sys.argv[1:7]
texts = [t for t in (sys.argv[7] if len(sys.argv) > 7 else "").split(",") if t]
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": int(width), "height": int(height)})
    page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    page.wait_for_function(
        "([s, n, texts]) => document.querySelectorAll(s).length >= n"
        " && texts.every(t => document.body.innerText.includes(t))",
        arg=[selector, int(minimum), texts], timeout=90_000)
    page.wait_for_timeout(5_000)
    page.screenshot(path=out, full_page=True)
    browser.close()
print(f"screenshot: {out}")

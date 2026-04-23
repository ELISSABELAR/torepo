#!/usr/bin/env python3

import os
import sys
import json
import time
import random
import argparse
import requests
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────
BASE_URL   = "https://www.pointdevente.parionssport.fdj.fr/v1/events"
ORIGIN     = "https://www.pointdevente.parionssport.fdj.fr/grilles/resultats"
OUT        = Path("v1/events/resulted")
STATE_FILE = "state/sync.json"
LIMIT      = 100

# ─────────────────────────────────────────────────────────────
_JID = 0

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}][{_JID}] {msg}", flush=True)


# ─────────────────────────────────────────────────────────────
#  COOKIE VIA PLAYWRIGHT
# ─────────────────────────────────────────────────────────────
def get_cookie() -> str:
    from playwright.sync_api import sync_playwright
    log("browser: getting session cookie...")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled",
                  "--disable-dev-shm-usage"],
        )
        ctx = browser.new_context(
            locale="fr-FR",
            timezone_id="Europe/Paris",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
        )
        ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
        """)
        page = ctx.new_page()
        try:
            page.goto(ORIGIN, wait_until="networkidle", timeout=30000)
        except Exception:
            pass
        time.sleep(3 + random.uniform(0, 2))
        cookies = ctx.cookies()
        browser.close()

    val = next((c["value"] for c in cookies if c["name"] == "datadome"), "")
    if val:
        log(f"cookie ok ({val[:20]}...)")
    else:
        log("cookie not found")
    return val


# ─────────────────────────────────────────────────────────────
#  FETCH
# ─────────────────────────────────────────────────────────────
def fetch(offset: int, cookie: str) -> tuple:
    url = f"{BASE_URL}?status=resulted&offset={offset}&limit={LIMIT}&sort=DESC"
    hdrs = {
        "accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "fr-FR,fr;q=0.9,en;q=0.8",
        "cache-control":   "no-cache",
        "pragma":          "no-cache",
        "sec-ch-ua":       '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":  "document",
        "sec-fetch-mode":  "navigate",
        "sec-fetch-site":  "none",
        "sec-fetch-user":  "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
        ),
    }
    if cookie:
        hdrs["cookie"] = f"datadome={cookie}"

    r500 = 0
    r403 = 0

    while True:
        try:
            resp = requests.get(url, headers=hdrs, timeout=30)
        except requests.exceptions.Timeout:
            log(f"timeout {offset} → retry")
            time.sleep(2)
            continue
        except Exception as e:
            log(f"error {offset}: {e} → 10s")
            time.sleep(10)
            continue

        # Mise à jour cookie depuis Set-Cookie
        sc = resp.headers.get("set-cookie", "")
        if "datadome=" in sc:
            v = sc.split("datadome=")[1].split(";")[0]
            if v and v != cookie:
                cookie = v
                hdrs["cookie"] = f"datadome={cookie}"

        if resp.status_code == 200:
            try:
                return resp.json(), cookie
            except Exception:
                log(f"bad json {offset}")
                time.sleep(2)
                continue

        elif resp.status_code == 500:
            # Timeout serveur passager → retry immédiat
            r500 += 1
            if r500 > 5:
                log(f"500x5 {offset} → skip")
                return None, cookie
            log(f"500 {offset} retry {r500}/5 (immediate)")
            # pas de sleep

        elif resp.status_code == 403:
            r403 += 1
            if r403 > 2:
                log(f"403x2 {offset} → skip")
                return None, cookie
            log(f"403 {offset} → refresh cookie ({r403}/2)")
            cookie = get_cookie()
            if cookie:
                hdrs["cookie"] = f"datadome={cookie}"
            time.sleep(5)

        elif resp.status_code == 429:
            log(f"429 {offset} → 90s")
            time.sleep(90)

        else:
            log(f"http {resp.status_code} {offset} → 10s")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────
#  STOCKAGE
# ─────────────────────────────────────────────────────────────
def save(data: dict, offset: int, page: int) -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUT / f"offset_{offset:07d}_page{page:04d}_{ts}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def exists(offset: int) -> bool:
    return any(OUT.glob(f"offset_{offset:07d}_*.json"))


# ─────────────────────────────────────────────────────────────
#  SYNCHRONISATION VIA GITHUB API
# ─────────────────────────────────────────────────────────────
GH_API  = "https://api.github.com"
GH_TOK  = os.environ.get("GITHUB_TOKEN", "")
GH_REPO = os.environ.get("GITHUB_REPOSITORY", "")


def _gh():
    return {
        "Authorization":        f"Bearer {GH_TOK}",
        "Accept":               "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def read_state() -> dict:
    try:
        r = requests.get(f"{GH_API}/repos/{GH_REPO}/contents/{STATE_FILE}",
                         headers=_gh(), timeout=15)
        if r.status_code == 404:
            return {"sha": None, "data": {}}
        r.raise_for_status()
        import base64
        d = r.json()
        return {"sha": d["sha"],
                "data": json.loads(base64.b64decode(d["content"]).decode())}
    except Exception as e:
        log(f"read_state err: {e}")
        return {"sha": None, "data": {}}


def write_state(jid: int, offset: int, st: dict):
    import base64
    st["data"][str(jid)] = offset
    b64 = base64.b64encode(json.dumps(st["data"], indent=2).encode()).decode()
    payload = {"message": f"s {jid}:{offset}", "content": b64}
    if st["sha"]:
        payload["sha"] = st["sha"]
    try:
        r = requests.put(f"{GH_API}/repos/{GH_REPO}/contents/{STATE_FILE}",
                         headers=_gh(), json=payload, timeout=15)
        if r.status_code in (200, 201):
            st["sha"] = r.json()["content"]["sha"]
        else:
            log(f"write_state {r.status_code}")
    except Exception as e:
        log(f"write_state err: {e}")


def wait_peers(offset: int, total: int, timeout_min: int = 15):
    deadline = time.time() + timeout_min * 60
    while time.time() < deadline:
        st = read_state()
        behind = [j for j in range(total) if st["data"].get(str(j), -1) < offset]
        if not behind:
            log(f"sync ok @ {offset}")
            return st
        log(f"waiting {behind} @ {offset}")
        time.sleep(20)
    log(f"sync timeout @ {offset}")
    return read_state()


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
def main():
    global _JID

    p = argparse.ArgumentParser()
    p.add_argument("--job-id",       type=int,   required=True)
    p.add_argument("--total-jobs",   type=int,   default=8)
    p.add_argument("--total",        type=int,   default=127962)
    p.add_argument("--start",        type=int,   default=0)
    p.add_argument("--delay",        type=float, default=5.0)
    p.add_argument("--no-sync",      action="store_true")
    args = p.parse_args()

    _JID   = args.job_id
    stride = args.total_jobs * LIMIT

    last   = (args.total // LIMIT) * LIMIT
    if last >= args.total:
        last -= LIMIT

    my_start   = args.start + args.job_id * LIMIT
    my_offsets = list(range(my_start, last + 1, stride))

    log(f"start={my_start} stride={stride} pages={len(my_offsets)}")

    cookie = get_cookie()
    st     = read_state()

    done = fail = skip = 0

    for i, offset in enumerate(my_offsets, 1):

        if exists(offset):
            log(f"[{i}/{len(my_offsets)}] skip {offset}")
            skip += 1
            if not args.no_sync:
                write_state(args.job_id, offset, st)
            continue

        data, cookie = fetch(offset, cookie)

        if data is None:
            fail += 1
        else:
            page = data.get("pagination", {}).get("page", i)
            path = save(data, offset, page)
            done += 1
            log(f"[{i}/{len(my_offsets)}] p{page} off={offset} {path.name}")

        if not args.no_sync:
            write_state(args.job_id, offset, st)
            if i < len(my_offsets):
                st = wait_peers(offset, args.total_jobs)

        if i < len(my_offsets):
            time.sleep(args.delay + random.uniform(0, 2))

    log(f"done={done} fail={fail} skip={skip}")


if __name__ == "__main__":
    main()

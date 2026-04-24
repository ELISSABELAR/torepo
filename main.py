#!/usr/bin/env python3
"""
Scraper – pointdevente.parionssport.fdj.fr/v1/events
─────────────────────────────────────────────────────
Problème racine identifié : DataDome encode le TLS fingerprint (JA3/JA4)
dans le cookie `datadome`. Python requests utilise OpenSSL → fingerprint
différent de Chrome → DataDome rejette côté serveur avec 403.

Solution : curl-cffi impersonne le TLS exact de Chrome (même JA3/JA4/HTTP2
fingerprint). Avec le cookie datadome obtenu sur une vraie session Chrome
(ou Playwright), le couple fingerprint+cookie est cohérent → passe.

Install : pip install curl-cffi playwright
"""

import os
import json
import time
import random
import argparse
import requests as _requests   # uniquement pour GitHub API
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
BASE_URL   = "https://www.pointdevente.parionssport.fdj.fr/v1/events"
ORIGIN     = "https://www.pointdevente.parionssport.fdj.fr/grilles/resultats"
OUT        = Path("v1/events/resulted")
STATE_FILE = "state/sync.json"
LIMIT      = 100

_JID = 0

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}][{_JID}] {msg}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
#  COOKIES via Playwright (visite ORIGIN, extrait tous les cookies)
# ─────────────────────────────────────────────────────────────────────────────
def get_cookies_via_browser() -> dict:
    from playwright.sync_api import sync_playwright
    log("browser: obtention cookies sur ORIGIN...")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            locale="fr-FR",
            timezone_id="Europe/Paris",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
            extra_http_headers={"Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"},
        )
        ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
        """)
        page = ctx.new_page()
        try:
            page.goto(ORIGIN, wait_until="domcontentloaded", timeout=45_000)
            time.sleep(3 + random.uniform(0, 2))   # laisse DataDome valider
        except Exception as e:
            log(f"warn: goto ORIGIN: {e}")
        cookies = {c["name"]: c["value"] for c in ctx.cookies()}
        browser.close()

    log(f"cookies obtenus: {list(cookies.keys())}")
    return cookies


# ─────────────────────────────────────────────────────────────────────────────
#  SESSION curl-cffi — impersonne Chrome TLS/HTTP2 fingerprint
# ─────────────────────────────────────────────────────────────────────────────
def make_session(cookies: dict):
    """
    curl-cffi envoie exactement le même TLS ClientHello que Chrome124
    (JA3, JA4, GREASE, HTTP/2 settings, header order...).
    DataDome voit un fingerprint cohérent avec le cookie → laisse passer.
    """
    from curl_cffi import requests as cf
    session = cf.Session(impersonate="chrome124")

    # Headers d'une navigation document (sec-fetch-* corrects)
    session.headers.update({
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding":           "gzip, deflate, br, zstd",
        "Accept-Language":           "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control":             "max-age=0",
        "Referer":                   ORIGIN,
        "Sec-Ch-Ua":                 '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile":          "?0",
        "Sec-Ch-Ua-Platform":        '"Windows"',
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "same-origin",
        "Sec-Fetch-User":            "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent":                (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    })

    for name, value in cookies.items():
        session.cookies.set(name, value, domain=".pointdevente.parionssport.fdj.fr")

    return session


def update_cookies(session, response) -> None:
    """Absorbe les Set-Cookie DataDome envoyés en réponse."""
    for c in response.cookies:
        session.cookies.set(
            c.name, c.value,
            domain=c.domain or ".pointdevente.parionssport.fdj.fr",
        )


# ─────────────────────────────────────────────────────────────────────────────
#  FETCH avec retry
# ─────────────────────────────────────────────────────────────────────────────
def fetch(offset: int, session, cookies_fn) -> dict | None:
    url = f"{BASE_URL}?status=resulted&offset={offset}&limit={LIMIT}&sort=DESC"
    consecutive_403 = 0

    for attempt in range(8):
        try:
            resp = session.get(url, timeout=30)
        except Exception as e:
            log(f"réseau @ {offset}: {e} — retry {attempt}")
            time.sleep(5)
            continue

        update_cookies(session, resp)

        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception:
                log(f"json invalide @ {offset} — retry {attempt}")
                time.sleep(2)
                continue

        elif resp.status_code == 500:
            wait = min(5 * (2 ** attempt), 120)
            log(f"500 @ {offset} — retry {attempt+1}/8 dans {wait}s")
            time.sleep(wait)
            continue

        elif resp.status_code == 403:
            consecutive_403 += 1
            if consecutive_403 >= 3:
                log(f"403x3 @ {offset} — skip")
                return None
            log(f"403 @ {offset} — renouvellement cookies ({consecutive_403}/3)")
            new_cookies = cookies_fn()
            for name, value in new_cookies.items():
                session.cookies.set(name, value, domain=".pointdevente.parionssport.fdj.fr")
            time.sleep(5 + random.uniform(0, 3))
            continue

        elif resp.status_code == 429:
            log(f"429 @ {offset} — pause 90s")
            time.sleep(90)
            continue

        else:
            log(f"http {resp.status_code} @ {offset} — skip")
            return None

    log(f"max retries @ {offset} — skip")
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  STOCKAGE
# ─────────────────────────────────────────────────────────────────────────────
def save(data, offset: int) -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUT / f"offset_{offset:07d}_{ts}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def exists(offset: int) -> bool:
    return any(OUT.glob(f"offset_{offset:07d}_*.json"))


# ─────────────────────────────────────────────────────────────────────────────
#  ÉTAT GITHUB
# ─────────────────────────────────────────────────────────────────────────────
GH_API  = "https://api.github.com"
GH_TOK  = os.environ.get("GITHUB_TOKEN", "")
GH_REPO = os.environ.get("GITHUB_REPOSITORY", "")
_STATE_WRITE_EVERY = 10


def _gh_headers() -> dict:
    return {
        "Authorization":        f"Bearer {GH_TOK}",
        "Accept":               "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def read_state() -> dict:
    try:
        r = _requests.get(
            f"{GH_API}/repos/{GH_REPO}/contents/{STATE_FILE}",
            headers=_gh_headers(), timeout=15,
        )
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


def write_state(jid: int, offset: int, st: dict) -> None:
    import base64
    st["data"][str(jid)] = offset
    b64     = base64.b64encode(json.dumps(st["data"], indent=2).encode()).decode()
    payload = {"message": f"s {jid}:{offset}", "content": b64}
    if st["sha"]:
        payload["sha"] = st["sha"]
    try:
        r = _requests.put(
            f"{GH_API}/repos/{GH_REPO}/contents/{STATE_FILE}",
            headers=_gh_headers(), json=payload, timeout=15,
        )
        if r.status_code in (200, 201):
            st["sha"] = r.json()["content"]["sha"]
        else:
            log(f"write_state {r.status_code}")
    except Exception as e:
        log(f"write_state err: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    global _JID

    ap = argparse.ArgumentParser()
    ap.add_argument("--job-id",     type=int,   required=True)
    ap.add_argument("--total-jobs", type=int,   default=5)
    ap.add_argument("--total",      type=int,   default=127_962)
    ap.add_argument("--start",      type=int,   default=0)
    ap.add_argument("--delay",      type=float, default=2.0)
    ap.add_argument("--no-sync",    action="store_true")
    args = ap.parse_args()

    _JID   = args.job_id
    stride = args.total_jobs * LIMIT

    last       = ((args.total - 1) // LIMIT) * LIMIT
    my_start   = args.start + args.job_id * LIMIT
    my_offsets = list(range(my_start, last + 1, stride))

    log(f"start={my_start}  stride={stride}  pages={len(my_offsets)}")

    # Init cookies + session curl-cffi
    cookies    = get_cookies_via_browser()
    session    = make_session(cookies)

    st   = read_state() if not args.no_sync else {"sha": None, "data": {}}
    done = fail = skip = 0

    for i, offset in enumerate(my_offsets, 1):

        if exists(offset):
            log(f"[{i}/{len(my_offsets)}] skip off={offset}")
            skip += 1
            continue

        data = fetch(offset, session, get_cookies_via_browser)

        if data is None:
            fail += 1
            log(f"[{i}/{len(my_offsets)}] FAIL off={offset}")
        else:
            path = save(data, offset)
            done += 1
            total_hint = data.get("total", "?") if isinstance(data, dict) else len(data)
            log(f"[{i}/{len(my_offsets)}] ok  off={offset}  total={total_hint}  → {path.name}")

        if not args.no_sync and i % _STATE_WRITE_EVERY == 0:
            write_state(args.job_id, offset, st)

        if i < len(my_offsets):
            time.sleep(args.delay + random.uniform(0, 1.5))

    if not args.no_sync and my_offsets:
        write_state(args.job_id, my_offsets[-1], st)

    log(f"terminé — done={done}  fail={fail}  skip={skip}")


if __name__ == "__main__":
    main()

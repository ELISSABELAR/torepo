#!/usr/bin/env python3
"""
Scraper – pointdevente.parionssport.fdj.fr/v1/events
─────────────────────────────────────────────────────
Tous les appels API passent par page.evaluate(fetch()) depuis
le navigateur Playwright : sec-fetch-*, XSRF-TOKEN, cookies et
Referer sont positionnés automatiquement et correctement par le
browser, ce qui évite les 403 dus aux headers incohérents.
"""

import os
import json
import time
import random
import argparse
import requests as _requests
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
#  BROWSER — session persistante, appels API via fetch() natif du navigateur
# ─────────────────────────────────────────────────────────────────────────────
class Browser:
    """
    Ouvre Chromium une seule fois, navigue vers ORIGIN pour établir
    les cookies de session (dont XSRF-TOKEN), puis effectue tous les
    appels API via page.evaluate(fetch()) dans ce même contexte.

    Pourquoi page.evaluate(fetch()) et non requests ?
    - sec-fetch-dest/mode/site sont positionnés correctement (empty/cors/same-origin)
    - Angular lit XSRF-TOKEN cookie et l'envoie en X-XSRF-TOKEN header :
      on reproduit la même logique depuis le JS injecté.
    - Referer = ORIGIN automatiquement (même session).
    - Fingerprint TLS = vrai Chrome.
    """

    # JS injecté dans page.evaluate – reproduit Angular HttpClientXsrfModule
    _JS_FETCH = """
        async (url) => {
            // Reproduit Angular HttpClientXsrfModule :
            // lit XSRF-TOKEN dans les cookies et l'envoie en header X-XSRF-TOKEN
            const xsrf = (document.cookie.split('; ')
                .find(c => c.startsWith('XSRF-TOKEN=')) || '')
                .replace('XSRF-TOKEN=', '');

            let resp;
            try {
                resp = await fetch(url, {
                    method:      'GET',
                    credentials: 'include',
                    headers: {
                        'Accept':       'application/json, text/plain, */*',
                        'X-XSRF-TOKEN': xsrf,
                    },
                });
            } catch (err) {
                return { __status: -1, __error: String(err) };
            }

            if (!resp.ok) {
                return { __status: resp.status };
            }

            try {
                const data = await resp.json();
                return { __status: 200, data };
            } catch (err) {
                return { __status: 200, __parse_error: String(err) };
            }
        }
    """

    def __init__(self):
        from playwright.sync_api import sync_playwright
        self._pw      = sync_playwright().__enter__()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        self._ctx = self._browser.new_context(
            locale="fr-FR",
            timezone_id="Europe/Paris",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
            extra_http_headers={
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            },
        )
        self._ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
        """)
        self._page = self._ctx.new_page()

    def warm(self) -> None:
        """Navigate ORIGIN pour initialiser cookies + XSRF-TOKEN."""
        log("browser: navigation vers ORIGIN...")
        try:
            self._page.goto(ORIGIN, wait_until="domcontentloaded", timeout=45_000)
            time.sleep(3 + random.uniform(0, 2))
            cookies = {c["name"]: c["value"] for c in self._ctx.cookies()}
            if "XSRF-TOKEN" in cookies:
                log(f"session ok — XSRF-TOKEN={cookies['XSRF-TOKEN'][:12]}...")
            else:
                log(f"ATTENTION: XSRF-TOKEN absent — cookies: {list(cookies.keys())}")
        except Exception as e:
            log(f"browser warm error: {e}")

    def close(self) -> None:
        try:
            self._browser.close()
            self._pw.__exit__(None, None, None)
        except Exception:
            pass

    def fetch_json(self, offset: int):
        """
        Retourne :
          dict        -> succes
          None        -> echec definitif (skip)
          "__retry"   -> retry demande (re-warm + nouvel essai)
        """
        url = (
            f"{BASE_URL}"
            f"?status=resulted&offset={offset}&limit={LIMIT}&sort=DESC"
        )
        try:
            result = self._page.evaluate(self._JS_FETCH, url)
        except Exception as e:
            log(f"evaluate error @ {offset}: {e}")
            time.sleep(5)
            return "__retry"

        status = result.get("__status", 0)

        if status == 200:
            if "data" in result:
                return result["data"]
            log(f"parse error @ {offset}: {result.get('__parse_error')}")
            return None

        if status == 403:
            log(f"403 @ {offset} → re-warm session")
            self.warm()
            return "__retry"

        if status == 429:
            log(f"429 @ {offset} → pause 90s")
            time.sleep(90)
            return "__retry"

        if status == -1:
            log(f"network error @ {offset}: {result.get('__error')}")
            time.sleep(5)
            return "__retry"

        log(f"http {status} @ {offset}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  STOCKAGE LOCAL
# ─────────────────────────────────────────────────────────────────────────────
def save(data: dict, offset: int) -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUT / f"offset_{offset:07d}_{ts}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def exists(offset: int) -> bool:
    return any(OUT.glob(f"offset_{offset:07d}_*.json"))


# ─────────────────────────────────────────────────────────────────────────────
#  ETAT PARTAGE VIA GITHUB API
# ─────────────────────────────────────────────────────────────────────────────
GH_API  = "https://api.github.com"
GH_TOK  = os.environ.get("GITHUB_TOKEN", "")
GH_REPO = os.environ.get("GITHUB_REPOSITORY", "")

_STATE_WRITE_EVERY = 10   # ecrire l'etat tous les N offsets traites


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
        return {
            "sha":  d["sha"],
            "data": json.loads(base64.b64decode(d["content"]).decode()),
        }
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
    ap.add_argument("--delay",      type=float, default=3.0)
    ap.add_argument("--no-sync",    action="store_true")
    args = ap.parse_args()

    _JID   = args.job_id
    stride = args.total_jobs * LIMIT            # ex. 5*100 = 500

    last       = ((args.total - 1) // LIMIT) * LIMIT
    my_start   = args.start + args.job_id * LIMIT
    my_offsets = list(range(my_start, last + 1, stride))

    log(f"start={my_start}  stride={stride}  pages={len(my_offsets)}")

    # ── init ─────────────────────────────────────────────────────────────────
    browser = Browser()
    browser.warm()

    st   = read_state() if not args.no_sync else {"sha": None, "data": {}}
    done = fail = skip = 0

    # ── boucle principale ────────────────────────────────────────────────────
    for i, offset in enumerate(my_offsets, 1):

        if exists(offset):
            log(f"[{i}/{len(my_offsets)}] skip (deja present) off={offset}")
            skip += 1
            continue

        # Retry loop (max 3 tentatives par offset)
        data  = None
        tries = 0
        while tries < 3:
            result = browser.fetch_json(offset)
            if result == "__retry":
                tries += 1
                time.sleep(2 ** tries)   # backoff 2s / 4s / 8s
                continue
            data = result
            break

        if data is None:
            fail += 1
            log(f"[{i}/{len(my_offsets)}] FAIL off={offset}")
        else:
            path = save(data, offset)
            done += 1
            total_hint = (
                data.get("total", "?")
                if isinstance(data, dict)
                else len(data)
            )
            log(
                f"[{i}/{len(my_offsets)}] ok  "
                f"off={offset}  total={total_hint}  "
                f"→ {path.name}"
            )

        # Ecriture etat periodique
        if not args.no_sync and i % _STATE_WRITE_EVERY == 0:
            write_state(args.job_id, offset, st)

        if i < len(my_offsets):
            time.sleep(args.delay + random.uniform(0, 1.5))

    # Ecriture etat finale
    if not args.no_sync and my_offsets:
        write_state(args.job_id, my_offsets[-1], st)

    browser.close()
    log(f"termine — done={done}  fail={fail}  skip={skip}")


if __name__ == "__main__":
    main()

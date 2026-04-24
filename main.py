#!/usr/bin/env python3
"""
Scraper – pointdevente.parionssport.fdj.fr/v1/events
─────────────────────────────────────────────────────
Le endpoint /v1/events est servi comme une réponse document (sec-fetch-dest:
document, sec-fetch-mode: navigate). On utilise page.goto() directement sur
l'URL API — le navigateur envoie exactement les mêmes headers qu'un vrai
utilisateur qui tape l'URL, y compris datadome et le bon Referer.
NE PAS utiliser fetch() / XMLHttpRequest : ce sont des contextes XHR/cors
dont les sec-fetch-* sont différents et déclenchent les 403.
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
#  BROWSER
# ─────────────────────────────────────────────────────────────────────────────
class Browser:
    """
    Stratégie : page.goto(url_api) — navigation document, pas XHR.

    Le endpoint renvoie du JSON brut comme s'il était une page. Le browser
    navigue dessus, on lit le texte du body et on parse le JSON.

    Avantages vs fetch/evaluate :
    - sec-fetch-dest: document  (correct)
    - sec-fetch-mode: navigate  (correct)
    - sec-fetch-site: same-origin (on vient de ORIGIN, même domaine)
    - datadome cookie envoyé automatiquement
    - Referer = dernière page visitée (ORIGIN)
    - Aucun header XSRF nécessaire (pas de XHR Angular)
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
        """
        Navigation sur ORIGIN pour obtenir datadome + établir le Referer.
        Après ça, tous les goto() vers l'API partiront avec:
          Referer: https://www.pointdevente.parionssport.fdj.fr/grilles/resultats
          sec-fetch-site: same-origin
          Cookie: datadome=...
        """
        log("browser: warm sur ORIGIN...")
        try:
            self._page.goto(ORIGIN, wait_until="domcontentloaded", timeout=45_000)
            time.sleep(2 + random.uniform(0, 1.5))
            cookies = {c["name"]: c["value"] for c in self._ctx.cookies()}
            names   = list(cookies.keys())
            log(f"warm ok — cookies: {names}")
            if "datadome" not in cookies:
                log("ATTENTION: datadome absent apres warm")
        except Exception as e:
            log(f"warm error: {e}")

    def close(self) -> None:
        try:
            self._browser.close()
            self._pw.__exit__(None, None, None)
        except Exception:
            pass

    def fetch_json(self, offset: int):
        """
        Navigation directe vers l'URL API.
        Retourne : dict | None | "__retry"
        """
        url = f"{BASE_URL}?status=resulted&offset={offset}&limit={LIMIT}&sort=DESC"

        try:
            resp = self._page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        except Exception as e:
            log(f"goto error @ {offset}: {e}")
            time.sleep(3)
            return "__retry"

        status = resp.status if resp else 0

        if status == 200:
            try:
                # Le serveur renvoie du JSON brut — on lit le texte de la page
                raw = self._page.evaluate("() => document.body.innerText")
                data = json.loads(raw)
                return data
            except Exception as e:
                log(f"parse error @ {offset}: {e}")
                # Renavigue sur ORIGIN pour remettre le contexte correct
                self.warm()
                return "__retry"

        elif status == 403:
            log(f"403 @ {offset} → re-warm")
            self.warm()
            return "__retry"

        elif status == 429:
            log(f"429 @ {offset} → pause 90s")
            time.sleep(90)
            # Remettre le contexte document correct avant le retry
            self.warm()
            return "__retry"

        elif status == 500:
            log(f"500 @ {offset} → retry dans 5s")
            time.sleep(5)
            # Re-warm pour repartir d'un bon Referer
            self.warm()
            return "__retry"

        else:
            log(f"http {status} @ {offset}")
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
#  ETAT GITHUB
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
    ap.add_argument("--delay",      type=float, default=2.0)
    ap.add_argument("--no-sync",    action="store_true")
    args = ap.parse_args()

    _JID   = args.job_id
    stride = args.total_jobs * LIMIT

    last       = ((args.total - 1) // LIMIT) * LIMIT
    my_start   = args.start + args.job_id * LIMIT
    my_offsets = list(range(my_start, last + 1, stride))

    log(f"start={my_start}  stride={stride}  pages={len(my_offsets)}")

    browser = Browser()
    browser.warm()

    st   = read_state() if not args.no_sync else {"sha": None, "data": {}}
    done = fail = skip = 0

    for i, offset in enumerate(my_offsets, 1):

        if exists(offset):
            log(f"[{i}/{len(my_offsets)}] skip off={offset}")
            skip += 1
            continue

        # Retry loop (max 4 tentatives)
        data  = None
        tries = 0
        while tries < 4:
            result = browser.fetch_json(offset)
            if result == "__retry":
                tries += 1
                time.sleep(min(2 ** tries, 30))
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
                data.get("total", "?") if isinstance(data, dict)
                else len(data)
            )
            log(f"[{i}/{len(my_offsets)}] ok  off={offset}  total={total_hint}  → {path.name}")

        if not args.no_sync and i % _STATE_WRITE_EVERY == 0:
            write_state(args.job_id, offset, st)

        if i < len(my_offsets):
            time.sleep(args.delay + random.uniform(0, 1.5))

    if not args.no_sync and my_offsets:
        write_state(args.job_id, my_offsets[-1], st)

    browser.close()
    log(f"termine — done={done}  fail={fail}  skip={skip}")


if __name__ == "__main__":
    main()

"""
Polite Overpass API requests, shared by the network and basemap fetchers.

- One request at a time; before each request to a server with a /status endpoint
  (overpass-api.de), wait until it reports a free slot (status checks don't count
  against the quota). A 429 means our quota is used up: we wait for the slot instead
  of hammering.
- After a 504/500/timeout, back off 30, 60, 120... s (+ jitter) before asking the same
  server again; a different healthy server may be tried right away.
- A mirror that fails 3 times in a row is dropped for the rest of the run
  (kumi.systems answered tiny queries but returned 500 for every real one in 2026-10).

A copy of this file lives next to D:\\QGIS\\osm_basemap\\fetch_osm_basemap.py (not a git
repo); keep the two in sync.
"""
import random
import re
import time

import requests

SERVERS = ["https://overpass-api.de/api/interpreter",
           "https://overpass.kumi.systems/api/interpreter"]
MAX_FAILS = 3          # consecutive failures before a mirror is dropped (main server never)
_fails = {u: 0 for u in SERVERS}
_last_fail = {}        # server -> time of its last failure (for backoff)


def _host(url):
    return url.split('/')[2]


def wait_for_slot(url, headers, log, max_wait=600):
    """Block until the server reports a free slot (only servers with /api/status)."""
    if 'overpass-api.de' not in url:
        return
    waited = 0
    while waited < max_wait:
        try:
            st = requests.get(url.replace('/interpreter', '/status'), headers=headers, timeout=20).text
        except requests.RequestException:
            return  # status unreachable: just try the request
        m = re.search(r'(\d+) slots? available now', st)
        if m and int(m.group(1)) > 0:
            return
        secs = [int(s) for s in re.findall(r'in (-?\d+) seconds', st)]
        wait = max(5, min(secs) + 2) if secs else 15
        log(f"no free slot, wait {wait}s...")
        time.sleep(wait)
        waited += wait


def post(query, headers, timeout=180, attempts=6, log=None):
    """POST an Overpass query politely. Returns the parsed JSON, or None if all attempts fail."""
    log = log or (lambda msg: print(msg, end=' ', flush=True))
    for attempt in range(attempts):
        live = [u for u in SERVERS if u == SERVERS[0] or _fails[u] < MAX_FAILS]
        url = live[attempt % len(live)]
        if attempt > 0:
            log(f"retry {attempt} via {_host(url)}...")
        n = _fails[url]
        if n:  # this server failed last time: back off before asking it again
            due = _last_fail.get(url, 0) + min(240, 30 * 2 ** (n - 1)) * random.uniform(1.0, 1.3)
            if due > time.time():
                log(f"(wait {due - time.time():.0f}s)")
                time.sleep(due - time.time())
        wait_for_slot(url, headers, log)
        try:
            resp = requests.post(url, data={'data': query}, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                # A query that hit the server's time/memory limit returns 200 + an error remark
                remark = str(data.get('remark', ''))
                if 'error' in remark.lower():
                    log(f"partial result ({remark[:50]})...")
                else:
                    _fails[url] = 0
                    return data
            else:
                log(f"HTTP {resp.status_code}...")
        except requests.exceptions.Timeout:
            log("timeout...")
        except Exception as e:  # connection errors, invalid JSON
            log(f"error: {str(e)[:50]}...")
        _fails[url] += 1
        _last_fail[url] = time.time()
        if url != SERVERS[0] and _fails[url] == MAX_FAILS:
            log(f"(dropping {_host(url)} for this run)")
    return None

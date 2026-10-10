"""
Polite Overpass API requests, shared by the network and basemap fetchers.

- One request at a time on this machine: a lock file in the system temp folder is shared by
  every process using this module (e.g. two cities downloading in parallel take turns).
- Before each request to a server with a /status endpoint (overpass-api.de), wait until it
  reports a free slot (status checks don't count against the quota). A 429 means our quota
  is used up: we wait for the slot instead of hammering.
- After a 504/500/429/timeout, back off 60, 120, 240... s, max 10 min (+ jitter), or as long
  as a Retry-After header asks. The backoff state is shared between processes too, so one
  process's failure makes the others wait as well.
- A mirror that fails 3 times in a row is dropped for the rest of the run
  (kumi.systems answered tiny queries but returned 500 for every real one in 2026-10).
- The User-Agent names the project (USER_AGENT), whatever headers the caller passes.

A copy of this file lives next to D:\\QGIS\\osm_basemap\\fetch_osm_basemap.py (not a git
repo); keep the two in sync.
"""
import json
import os
import random
import re
import tempfile
import time
from pathlib import Path

import requests

SERVERS = ["https://overpass-api.de/api/interpreter",
           "https://overpass.kumi.systems/api/interpreter"]
USER_AGENT = 'transit-frequency-map/1.0 (+https://github.com/jacgeborys/transit-frequency)'
MAX_FAILS = 3          # consecutive failures before a mirror is dropped (main server never)
BACKOFF_BASE, BACKOFF_MAX = 60, 600   # s
_fails = {u: 0 for u in SERVERS}      # this process (mirror dropping)

_DIR = Path(tempfile.gettempdir()) / 'overpass_polite'
_LOCK = _DIR / 'request.lock'
_STATE = _DIR / 'backoff.json'        # server -> {"fails": n, "until": epoch s}


def _host(url):
    return url.split('/')[2]


def _headers(headers):
    h = dict(headers or {})
    h['User-Agent'] = USER_AGENT
    h.setdefault('Accept', '*/*')
    return h


class _MachineLock:
    """Exclusive lock file shared by all processes; a lock not refreshed for `stale` s (holder
    died) is taken over. Holders refresh it while waiting (_sleep); a request itself is
    shorter than `stale`."""

    def __init__(self, stale):
        self.stale = stale

    def __enter__(self):
        _DIR.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                fd = os.open(str(_LOCK), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                return self
            except FileExistsError:
                try:
                    if time.time() - _LOCK.stat().st_mtime > self.stale:
                        _LOCK.unlink()  # holder died mid-request
                        continue
                except FileNotFoundError:
                    continue
                time.sleep(2 + random.random())

    def __exit__(self, *exc):
        try:
            _LOCK.unlink()
        except FileNotFoundError:
            pass


def _sleep(secs):
    """Sleep while holding the lock, refreshing it so other processes don't take it over."""
    end = time.time() + secs
    while time.time() < end:
        try:
            os.utime(_LOCK)
        except OSError:
            pass
        time.sleep(min(20, max(0, end - time.time())))


def _read_state():
    try:
        return json.loads(_STATE.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}


def _write_state(state):
    tmp = _STATE.with_suffix('.tmp')
    tmp.write_text(json.dumps(state), encoding='utf-8')
    os.replace(tmp, _STATE)


def _record(url, ok, retry_after=None):
    """Update the shared backoff state after a request (call while holding the lock)."""
    state = _read_state()
    s = state.get(url, {'fails': 0, 'until': 0})
    if ok:
        s = {'fails': 0, 'until': 0}
    else:
        s['fails'] += 1
        wait = min(BACKOFF_MAX, BACKOFF_BASE * 2 ** (s['fails'] - 1)) * random.uniform(1.0, 1.3)
        if retry_after:
            wait = max(wait, retry_after)
        s['until'] = time.time() + wait
    state[url] = s
    _write_state(state)


def _retry_after(resp):
    try:
        return float(resp.headers.get('Retry-After', ''))
    except ValueError:
        return None


def wait_for_slot(url, headers, log, max_wait=600):
    """Block until the server reports a free slot (only servers with /api/status)."""
    if 'overpass-api.de' not in url:
        return
    waited = 0
    while waited < max_wait:
        try:
            st = requests.get(url.replace('/interpreter', '/status'),
                              headers=_headers(headers), timeout=20).text
        except requests.RequestException:
            return  # status unreachable: just try the request
        m = re.search(r'(\d+) slots? available now', st)
        if m and int(m.group(1)) > 0:
            return
        secs = [int(s) for s in re.findall(r'in (-?\d+) seconds', st)]
        wait = max(5, min(secs) + 2) if secs else 15
        log(f"no free slot, wait {wait}s...")
        _sleep(wait)
        waited += wait


def post(query, headers, timeout=180, attempts=6, log=None):
    """POST an Overpass query politely. Returns the parsed JSON, or None if all attempts fail."""
    log = log or (lambda msg: print(msg, end=' ', flush=True))
    for attempt in range(attempts):
        live = [u for u in SERVERS if u == SERVERS[0] or _fails[u] < MAX_FAILS]
        url = live[attempt % len(live)]
        if attempt > 0:
            log(f"retry {attempt} via {_host(url)}...")
        with _MachineLock(stale=timeout + 120):
            due = _read_state().get(url, {}).get('until', 0)
            if due > time.time():  # this server failed recently (for any of our processes)
                log(f"(wait {due - time.time():.0f}s)")
                _sleep(due - time.time())
            wait_for_slot(url, headers, log)
            ok, retry_after = False, None
            try:
                resp = requests.post(url, data={'data': query}, headers=_headers(headers),
                                     timeout=timeout)
                if resp.status_code == 200:
                    data = resp.json()
                    # A query that hit the server's time/memory limit returns 200 + an error remark
                    remark = str(data.get('remark', ''))
                    if 'error' in remark.lower():
                        log(f"partial result ({remark[:50]})...")
                    else:
                        ok = True
                else:
                    retry_after = _retry_after(resp)
                    log(f"HTTP {resp.status_code}...")
            except requests.exceptions.Timeout:
                log("timeout...")
            except Exception as e:  # connection errors, invalid JSON
                log(f"error: {str(e)[:50]}...")
            _record(url, ok, retry_after)
        if ok:
            _fails[url] = 0
            return data
        _fails[url] += 1
        if url != SERVERS[0] and _fails[url] == MAX_FAILS:
            log(f"(dropping {_host(url)} for this run)")
    return None

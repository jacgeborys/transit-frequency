"""
RAM guard for long chains: when available RAM drops below --low GB, suspend the largest
process of the chain (the script named by --match, or one of its worker processes); resume
suspended ones (largest-freed first) once more than --resume GB is available again.
A suspended process keeps its memory, but Windows can page it out, and it stops growing.
Milestones go to png/log_buildings_final.txt. Exits when no matching process has been seen
for --idle-exit seconds (after the chain started).

Usage (in a .cmd chain, before the heavy steps):
    start "" /b python _ram_guard.py --match 03_generate_isochrones_local 06_building_modes
"""
import argparse
import time
from datetime import datetime
from pathlib import Path

import psutil

LOG = Path(__file__).resolve().parent.parent / 'png' / 'log_buildings_final.txt'


def log(msg):
    line = f"{datetime.now():%d.%m.%Y %H:%M:%S} RAM_GUARD {msg}"
    print(line, flush=True)
    with open(LOG, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


def chain_procs(patterns):
    """Matching python processes and all their descendants (Pool workers)."""
    out = {}
    import os
    me = {os.getpid(), os.getppid()}   # the guard's own command line holds the patterns too
    for p in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if p.pid in me or '_ram_guard' in ' '.join(p.info['cmdline'] or []):
                continue
            cmd = ' '.join(p.info['cmdline'] or [])
            if 'python' in (p.info['name'] or '').lower() and any(s in cmd for s in patterns):
                out[p.pid] = p
                for c in p.children(recursive=True):
                    out[c.pid] = c
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return list(out.values())


def rss(p):
    try:
        return p.memory_info().rss
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--match', nargs='+', required=True)
    ap.add_argument('--low', type=float, default=1.0, help='suspend below this many GB available')
    ap.add_argument('--resume', type=float, default=3.0, help='resume above this many GB available')
    ap.add_argument('--interval', type=float, default=5)
    ap.add_argument('--idle-exit', type=float, default=600)
    args = ap.parse_args()
    suspended = []          # (process, rss when suspended)
    last_seen = time.time()
    log(f"start (suspend < {args.low:g} GB, resume > {args.resume:g} GB, watching {', '.join(args.match)})")
    while True:
        avail = psutil.virtual_memory().available / 2**30
        procs = chain_procs(args.match)
        if procs:
            last_seen = time.time()
        elif time.time() - last_seen > args.idle_exit:
            break
        suspended = [(p, r) for p, r in suspended if p.is_running()]
        susp_pids = {p.pid for p, _ in suspended}
        running = [p for p in procs if p.pid not in susp_pids]
        if avail < args.low and len(running) > 1:   # keep at least one process going
            p = max(running, key=rss)
            try:
                r = rss(p)
                p.suspend()
                suspended.append((p, r))
                log(f"{avail:.1f} GB free: suspended pid {p.pid} ({r / 2**30:.1f} GB), "
                    f"{len(suspended)} suspended")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        elif suspended and not running and avail > args.low + 0.3:
            # Everything is suspended: memory held by other programs (or by the suspended processes
            # themselves) may never free up to --resume, which deadlocked a chain on 2026-10-25.
            # Resume the smallest so work goes on; it is suspended again if RAM runs out.
            p, r = min(suspended, key=lambda t: t[1])
            try:
                p.resume()
                log(f"{avail:.1f} GB free, all chain work suspended: resumed pid {p.pid} ({r / 2**30:.1f} GB)")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            suspended.remove((p, r))
        elif suspended and avail > args.resume:
            # resume the one that fits: smallest first when tight, so work continues
            p, r = min(suspended, key=lambda t: t[1])
            if avail - r / 2**30 > args.low or len(suspended) == len(procs):
                try:
                    p.resume()
                    log(f"{avail:.1f} GB free: resumed pid {p.pid} ({r / 2**30:.1f} GB)")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                suspended.remove((p, r))
        time.sleep(args.interval)
    for p, _ in suspended:
        try:
            p.resume()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    log("exit (chain finished)")


if __name__ == '__main__':
    main()

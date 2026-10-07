"""Block until at least N GB of RAM is available (used by overnight batch chains).

Usage: python _wait_ram.py 5        (checks every 60 s, prints when it starts waiting)
"""
import sys
import time

import psutil

need = float(sys.argv[1]) if len(sys.argv) > 1 else 4.0
first = True
while psutil.virtual_memory().available / 2**30 < need:
    if first:
        print(f"waiting for {need:g} GB available "
              f"(now {psutil.virtual_memory().available / 2**30:.1f} GB)...", flush=True)
        first = False
    time.sleep(60)

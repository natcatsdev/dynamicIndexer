#!/usr/bin/env python3
from __future__ import annotations
import os, sys, subprocess, time, pathlib

BASE   = pathlib.Path(__file__).parent
AUTH   = BASE / "authLooperBackend.py"
INDEX  = BASE / "indexLooper.py"

AUTH_LOCK  = "/tmp/authscript.lock"
INDEX_LOCK = "/tmp/indexscript.lock"

ENV = {**os.environ, "AUTH_LOCK_FILE":  AUTH_LOCK,
                     "INDEX_LOCK_FILE": INDEX_LOCK}

def wait_for_lock(path: str):
    while pathlib.Path(path).exists():
        time.sleep(1)

def run(script: pathlib.Path):
    proc = subprocess.Popen([sys.executable, str(script)], env=ENV)
    return proc

def main():
    # 1) run Auth and wait
    p = run(AUTH);  p.wait()           # guarantees lock disappears
    wait_for_lock(AUTH_LOCK)

    # 2) run Inscription
    run(INDEX).wait()

if __name__ == "__main__":
    main()

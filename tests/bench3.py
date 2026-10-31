#!/usr/bin/env python3
"""Cold-cache thread scaling: every config reads files never touched before."""
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = "/mnt/d/Pictures"
QUICK = 64 * 1024


def collect(n_max=60000):
    out, dirs = [], [ROOT]
    while dirs and len(out) < n_max:
        d = dirs.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    if e.is_dir(follow_symlinks=False):
                        dirs.append(e.path)
                    else:
                        try:
                            s = e.stat(follow_symlinks=False).st_size
                        except OSError:
                            continue
                        if s > 100 * 1024:
                            out.append((e.path, s))
        except OSError:
            pass
    random.seed(11)
    random.shuffle(out)
    return out


def head64(path):
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            return len(fh.read(min(QUICK, size)))
    except OSError:
        return 0


def fullread(path):
    n = 0
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            while True:
                b = os.read(fd, 8 * 1024 * 1024)
                if not b:
                    break
                n += len(b)
        finally:
            os.close(fd)
    except OSError:
        pass
    return n


def chunks(seq, n):
    return [seq[i:i + n] for i in range(0, len(seq), n)]


def main():
    pool = collect()
    print(f"collected {len(pool)} files >100KB", flush=True)

    # --- cold head64: 4 disjoint sets of 400 files ---
    heads = [p for p, _ in pool[:1600]]
    print("\nCOLD head-64KB (disjoint files per config):", flush=True)
    for w, part in zip((8, 16, 32, 48), chunks(heads, 400)):
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=w) as ex:
            list(ex.map(head64, part, chunksize=2))
        dt = max(time.time() - t0, 1e-9)
        print(f"  workers={w:<3} {len(part)/dt:8.1f} files/s  {dt:6.1f}s", flush=True)

    # --- cold fullread: 4 disjoint sets of 40 files ---
    big = [(p, s) for p, s in pool[1600:2600] if s > 500 * 1024]
    print("\nCOLD full read 8MB chunks (disjoint files per config):", flush=True)
    for w, part in zip((8, 16, 32, 48), chunks(big, 40)):
        files = [p for p, _ in part]
        nbytes = sum(s for _, s in part)
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=w) as ex:
            list(ex.map(fullread, files, chunksize=1))
        dt = max(time.time() - t0, 1e-9)
        print(f"  workers={w:<3} {len(files)/dt:7.1f} files/s  {nbytes/1e6/dt:7.1f} MB/s  {dt:6.1f}s",
              flush=True)


if __name__ == "__main__":
    main()

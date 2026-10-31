#!/usr/bin/env python3
"""Benchmark thread scaling for head-read and full-read on the 9p mount."""
import hashlib
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = "/mnt/d/Pictures"
QUICK = 64 * 1024


def collect(n_max=30000):
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
                        if s > 10 * 1024:
                            out.append((e.path, s))
        except OSError:
            pass
    random.seed(7)
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
                b = os.read(fd, 4 * 1024 * 1024)
                if not b:
                    break
                n += len(b)
        finally:
            os.close(fd)
    except OSError:
        pass
    return n


def bench(label, fn, files, workers):
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(fn, files, chunksize=2))
    dt = max(time.time() - t0, 1e-9)
    print(f"  {label:<14} workers={workers:<3} {len(files)/dt:8.1f} files/s  {dt:6.1f}s",
          flush=True)
    return len(files) / dt


def main():
    pool = collect()
    print(f"collected {len(pool)} candidate files >10KB", flush=True)
    sample400 = [p for p, _ in pool[:400]]
    sample_big = [p for p, s in pool[:400] if s > 500 * 1024][:64]
    big_bytes = sum(os.path.getsize(p) for p in sample_big)
    print(f"big sample: {len(sample_big)} files, {big_bytes/1e6:.0f} MB total\n", flush=True)

    print("HEAD-ONLY 64KB read (quick md5 phase):", flush=True)
    best = 0
    for w in (8, 16, 32, 48, 64):
        r = bench("head64", head64, sample400, w)
        best = max(best, r)
    print(f"  -> best {best:.0f} files/s; 755k files would take {755115/best/60:.0f} min\n",
          flush=True)

    print("FULL sequential read (phase 4):", flush=True)
    for w in (8, 16, 32, 48):
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=w) as pool:
            list(pool.map(fullread, sample_big, chunksize=1))
        dt = max(time.time() - t0, 1e-9)
        print(f"  fullread      workers={w:<3} {big_bytes/1e6/dt:8.1f} MB/s  {dt:6.1f}s",
              flush=True)


if __name__ == "__main__":
    main()

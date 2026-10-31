#!/usr/bin/env python3
"""Micro-benchmark for hashing strategies on the 9p/drvfs mount (real sample)."""
import hashlib
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = "/mnt/d/Pictures"
QUICK = 64 * 1024
CHUNK = 4 * 1024 * 1024


def sample_files(n=400):
    pool = []
    count = 0
    t0 = time.time()
    for dirpath, dirnames, filenames in os.walk(ROOT):
        for f in filenames:
            p = os.path.join(dirpath, f)
            try:
                s = os.path.getsize(p)
            except OSError:
                continue
            count += 1
            if s > 10 * 1024:
                pool.append((p, s))
    print(f"walked {count} files in {time.time()-t0:.0f}s, "
          f"{len(pool)} files >10KB")
    random.seed(42)
    sample = random.sample(pool, min(n, len(pool)))
    return [p for p, _ in sample], [s for _, s in sample]


def head_only(path):
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            return fh.read(min(QUICK, size))
    except OSError:
        return b""


def head_tail(path):
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            if size <= 2 * QUICK:
                return fh.read()
            h = fh.read(QUICK)
            if size > QUICK:
                fh.seek(size - QUICK)
            return h + fh.read(QUICK)
    except OSError:
        return b""


def full_md5(path):
    h = hashlib.md5()
    n = 0
    try:
        with open(path, "rb") as fh:
            while True:
                b = fh.read(CHUNK)
                if not b:
                    break
                h.update(b)
                n += len(b)
        return n
    except OSError:
        return 0


def run(label, fn, files, workers):
    with ThreadPoolExecutor(max_workers=workers) as pool:
        t0 = time.time()
        list(pool.map(fn, files, chunksize=4))
        dt = max(time.time() - t0, 1e-6)
    r = len(files) / dt
    print(f"  {label:<22} workers={workers:<3} {r:7.1f} files/s   {dt:6.1f}s")
    return r


def main():
    files, sizes = sample_files()
    total_mb = sum(sizes) / 1e6
    sizes_sorted = sorted(sizes)
    print(f"sample: {len(files)} files, avg {total_mb/len(files):.0f} KB, "
          f"median {sizes_sorted[len(sizes)//2]//1024} KB, total {total_mb:.0f} MB\n")

    best = (0, None, None)
    full_best = (0, None)
    for workers in (8, 16, 32, 48):
        for label, fn in (("head-only 64K", head_only), ("head+tail 64K", head_tail),
                          ("full md5", full_md5)):
            r = run(label, fn, files, workers)
            if label == "full md5":
                if r > full_best[0]:
                    full_best = (r, workers)
            elif r > best[0]:
                best = (r, label, workers)
        print()
    print(f"best quick strategy : {best[1]} @ {best[2]} workers -> {best[0]:.1f} files/s")
    print(f"  projected quick phase (755k files): {755115/best[0]/60:.0f} min")
    print(f"best full md5       : @ {full_best[1]} workers -> {full_best[0]:.1f} files/s")
    print(f"  {full_best[0]*(total_mb/len(files))/1000:.0f} MB/s  "
          f"(full md5 phase cost depends on how many pass the quick filter)")


if __name__ == "__main__":
    main()

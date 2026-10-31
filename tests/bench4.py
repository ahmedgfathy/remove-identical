#!/usr/bin/env python3
"""Does path order or thread count change the cold small-read ceiling?"""
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = "/mnt/d/Pictures"
LO, HI = 100 * 1024, 1024 * 1024


def collect(n_max=400000):
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
                        if LO <= s <= HI:
                            out.append(e.path)
        except OSError:
            pass
    random.seed(3)
    random.shuffle(out)
    return out


def head64(path):
    try:
        with open(path, "rb") as fh:
            return len(fh.read(65536))
    except OSError:
        return 0


def run(label, files, workers):
    t0 = time.time()
    if workers == 1:
        for f in files:
            head64(f)
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(head64, files, chunksize=1))
    dt = max(time.time() - t0, 1e-9)
    print(f"  {label:<26} workers={workers}  {len(files)/dt:7.1f} files/s  {dt:6.1f}s",
          flush=True)


def main():
    pool = collect()
    print(f"collected {len(pool)} files in {LO//1024}-{HI//1024} KB range", flush=True)
    sets = [pool[i:i + 240] for i in (0, 240, 480, 720, 960, 1200)]
    run("A sorted, sequential", sorted(sets[0]), 1)
    run("B random, sequential", sets[1], 1)
    run("C sorted, 4 threads", sorted(sets[2]), 4)
    run("D random, 4 threads", sets[3], 4)
    run("E sorted, 8 threads", sorted(sets[4]), 8)
    run("F random, 2 threads", sets[5], 2)


if __name__ == "__main__":
    main()

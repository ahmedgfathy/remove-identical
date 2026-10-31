#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
remove_duplicates.py -- find 100% identical files, keep ONE copy, move the rest.

HOW IT PROVES TWO FILES ARE THE SAME (100% sure pipeline):
  1. same file size                 (from the scan, free)
  2. byte-for-byte comparison       (the absolute proof, done chunk by chunk;
                                     a MD5 collision cannot survive it)
  3. full MD5 of every duplicate    (computed while reading, reported per group)
Only files that pass ALL checks are treated as duplicates.

HOW IT AVOIDS WASTING TIME (important on slow network/drvfs mounts):
  * ONE parallel directory scan (pass 1 + pass 2 merged).
  * Each candidate file is opened EXACTLY ONCE.  Files of equal size are
    compared "in lockstep": every open file reads the next 64 KiB block, the
    block is compared byte-wise between all of them, and files that differ are
    dropped immediately (they cost one 64 KiB block, not a full read).
    Matching files keep reading until EOF -> their full MD5 is complete too.
  * No separate "quick hash" pass (that would open every file twice).
  * Results are checkpointed to a cache file: if the run is interrupted,
    rerunning skips every group that is already verified (resume).

KEEPING / MOVING:
  * Exactly ONE file of each duplicate group stays (default: the OLDEST --
    creation/birth time when the filesystem provides it, else modification
    time; ties go to the file closest to the root).
  * All other copies are MOVED (never deleted) into a new folder named with
    the run's date/time, e.g. /mnt/d/Pictures/duplicates_20261003_153045/
    Relative sub-folders are preserved; name clashes get _dup2, _dup3 ...
  * Safe by default: dry run.  Use --apply to really move files.

USAGE:
  python3 remove_duplicates.py                 # dry run (nothing moved)
  python3 remove_duplicates.py --apply         # really move the duplicates
  python3 remove_duplicates.py --apply --keep newest
  python3 remove_duplicates.py --images-only   # only image extensions
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import shutil
import signal
import stat as stat_module
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime

# --------------------------------------------------------------------------
# configuration
# --------------------------------------------------------------------------

CHUNK = 64 * 1024              # compare block == 9p msize (one round trip)
PROGRESS_EVERY = 2.0           # seconds between progress lines
DEFAULT_SCAN_WORKERS = 16      # parallel directory scan (stat is latency bound)
DEFAULT_READ_WORKERS = 4       # 4 threads already saturate cold reads (~32/s)
DEFAULT_WINDOW = 1024          # max files opened together in one group window

STOP = threading.Event()       # set on Ctrl+C / SIGTERM so workers stop at the
                               # next safe point instead of draining the queue

IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".jpe", ".jfif", ".jif", ".jp2", ".j2k", ".jxl",
    ".png", ".apng", ".gif", ".bmp", ".dib", ".ico", ".cur", ".ani",
    ".svg", ".svgz", ".webp", ".avif", ".heic", ".heif", ".avifs",
    ".tif", ".tiff", ".iff", ".dcx", ".pcx", ".pnm", ".pbm", ".pgm", ".ppm",
    ".tga", ".qoi", ".dds", ".exr", ".hdr", ".sgi", ".xpm", ".xwd",
    ".psd", ".psb", ".ai", ".eps", ".cdr", ".wmf", ".emf",
    ".raw", ".dng", ".cr2", ".cr3", ".nef", ".arw", ".srw", ".raf", ".orf",
    ".rw2", ".pef", ".mrw", ".nrw", ".erf", ".kdc", ".dcr", ".mos",
    ".threefr", ".ari", ".fff", ".iiq", ".mef", ".rwl", ".jxr", ".wdp",
}


def log(msg: str = "") -> None:
    print(msg, flush=True)


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024.0 or unit == "PB":
            return f"{int(n)} {unit}" if unit == "B" else f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n:.2f} PB"


def fmt_seconds(s: float) -> str:
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


# --------------------------------------------------------------------------
# progress
# --------------------------------------------------------------------------

class Progress:
    """Tracks files opened / bytes read and prints an ETA line."""

    def __init__(self, label: str, total_files: int, total_groups: int = 0) -> None:
        self.label = label
        self.total = total_files
        self.total_groups = total_groups
        self.groups_done = 0
        self.opened = 0
        self.bytes = 0
        self.errors = 0
        self.cached_files = 0
        self.start = time.time()
        self._last = 0.0

    def tick(self, opened: int = 0, nbytes: int = 0, errors: int = 0,
             cached: int = 0, force: bool = False) -> None:
        self.opened += opened
        self.bytes += nbytes
        self.errors += errors
        self.cached_files += cached
        now = time.time()
        if force or now - self._last >= PROGRESS_EVERY:
            self._last = now
            elapsed = max(now - self.start, 1e-6)
            done = self.opened + self.cached_files
            rate = self.opened / elapsed
            pct = 100.0 * done / self.total if self.total else 100.0
            eta = ""
            if rate > 0 and self.total > done:
                eta = f"  ETA {fmt_seconds((self.total - done) / rate)}"
            groups = ""
            if self.total_groups:
                groups = f"  groups {self.groups_done:,}/{self.total_groups:,}"
            log(f"  [{self.label}] {done:,}/{self.total:,} files ({pct:4.1f}%)"
                f"{groups}  read {human(self.bytes)}  {rate:,.1f} files/s  "
                f"elapsed {fmt_seconds(elapsed)}{eta}")


# --------------------------------------------------------------------------
# scan (single parallel pass: path -> (size, mtime_ns))
# --------------------------------------------------------------------------

def parallel_scan(root: str, excluded: set[str], images_only: bool,
                  auto_skip_dup_dirs: bool, workers: int):
    """ONE pass over the tree: returns {path: (size, mtime_ns)}, files, bytes.

    Uses two pools: one walks directories, one stats the files (stat is a
    ~2 ms 9p round trip, so it must run in parallel even *within* a single
    directory -- big flat folders are common).  Both are bounded so memory
    stays flat no matter how large the folder is.
    """
    from collections import deque

    stat_workers = max(workers, 8)
    dir_workers = max(4, min(8, workers))
    max_stats = stat_workers * 8
    max_dirs = dir_workers * 8

    metas: dict[str, tuple[int, int]] = {}
    total_files = 0
    total_bytes = 0
    total_dirs = 0
    t0 = time.time()
    last = [t0]

    def list_dir(path: str):
        subdirs, files = [], []
        try:
            with os.scandir(path) as it:
                for e in it:
                    try:
                        if e.is_dir(follow_symlinks=False):
                            subdirs.append(e.path)
                        elif e.is_file(follow_symlinks=False):
                            files.append(e.path)
                    except OSError:
                        continue
        except OSError:
            pass
        return subdirs, files

    def stat_one(path: str):
        st = os.stat(path, follow_symlinks=False)
        return path, st.st_size, st.st_mtime_ns

    def wanted(path: str) -> bool:
        if images_only and os.path.splitext(path)[1].lower() not in IMAGE_EXTENSIONS:
            return False
        return True

    deferred_dirs: deque[str] = deque([root])
    deferred_files: deque[str] = deque()
    inflight_dirs: dict = {}
    inflight_stats: dict = {}

    def submit_dirs(pool) -> None:
        while deferred_dirs and len(inflight_dirs) < max_dirs:
            fut = pool.submit(list_dir, deferred_dirs.popleft())
            inflight_dirs[fut] = True

    def submit_stats(pool) -> None:
        while deferred_files and len(inflight_stats) < max_stats:
            path = deferred_files.popleft()
            fut = pool.submit(stat_one, path)
            inflight_stats[fut] = path

    def progress(force: bool = False) -> None:
        now = time.time()
        if not force and now - last[0] < PROGRESS_EVERY:
            return
        last[0] = now
        rate = total_files / max(now - t0, 1e-6)
        log(f"  [scan] {total_files:,} files in {total_dirs:,} dirs "
            f"({rate:,.0f}/s)  {human(total_bytes)}  elapsed {fmt_seconds(now - t0)}")

    with ThreadPoolExecutor(max_workers=dir_workers) as dpool, \
            ThreadPoolExecutor(max_workers=stat_workers) as spool:
        submit_dirs(dpool)
        while deferred_dirs or deferred_files or inflight_dirs or inflight_stats:
            futures = set(inflight_dirs) | set(inflight_stats)
            if not futures:
                submit_dirs(dpool)
                submit_stats(spool)
                futures = set(inflight_dirs) | set(inflight_stats)
                if not futures:
                    break
            done, _pending = wait(futures, return_when=FIRST_COMPLETED)
            for fut in done:
                if fut in inflight_dirs:
                    inflight_dirs.pop(fut, None)
                    subdirs, files = fut.result()
                    total_dirs += 1
                    for sd in subdirs:
                        if sd in excluded:
                            continue
                        if auto_skip_dup_dirs and \
                                os.path.basename(sd).startswith("duplicates_"):
                            continue
                        deferred_dirs.append(sd)
                    for p in files:
                        if wanted(p):
                            deferred_files.append(p)
                else:
                    inflight_stats.pop(fut, None)
                    try:
                        path, size, mtime_ns = fut.result()
                    except OSError:
                        continue
                    metas[path] = (size, mtime_ns)
                    total_files += 1
                    total_bytes += size
                    progress()
            submit_dirs(dpool)
            submit_stats(spool)

    progress(force=True)
    return metas, total_files, total_bytes


def build_groups(metas: dict[str, tuple[int, int]], trust_mtime: bool):
    """Split the scan into size groups that could contain duplicates."""
    by_size: dict[int, list[tuple[str, int, int]]] = {}
    for path, (size, mtime_ns) in metas.items():
        by_size.setdefault(size, []).append((path, size, mtime_ns))

    skipped_mtime = 0
    groups = []
    for size, members in by_size.items():
        if len(members) < 2:
            continue
        if trust_mtime:
            counts: dict[tuple[int, int], int] = {}
            for _p, s, m in members:
                counts[(s, m)] = counts.get((s, m), 0) + 1
            filtered = [mem for mem in members if counts[(mem[1], mem[2])] > 1]
            skipped_mtime += len(members) - len(filtered)
            members = filtered
        if len(members) > 1:
            groups.append((size, members))

    # heaviest first: cost ~= opens (~40 ms each) + bytes (~20 MB/s)
    groups.sort(key=lambda g: (-(len(g[1]) * (1.0 + g[0] * 1.25e-6)), -len(g[1]), g[0]))
    total_candidates = sum(len(m) for _, m in groups)
    return groups, total_candidates, skipped_mtime


# --------------------------------------------------------------------------
# lockstep compare: the heart of the tool
# --------------------------------------------------------------------------

def lockstep_window(states, prog: Progress):
    """Compare up to len(states) files of EQUAL size in one pass.

    All files are opened together; every round each open file reads its next
    64 KiB block and the files are split into sub-groups by *byte equality*
    of that block.  Sub-groups of size 1 are dropped immediately (cost: one
    block), groups that reach EOF together are byte-identical clusters
    (their full MD5 is complete).  Dropped singletons are returned with the
    MD5 of their consumed prefix so a later window can re-join them with
    files that share the same prefix.

    Returns (clusters, singletons, md5s, errors):
      clusters   : list of lists of paths (>= 2, byte-identical, fully read)
      singletons : list of (path, size, mtime_ns, prefix_len, prefix_md5)
      md5s       : path -> full md5 hexdigest (only cluster members)
      errors     : unreadable paths (never moved)
    """
    clusters, singletons, md5s, errors = [], [], {}, []
    opened = []  # [state, fh, md5, consumed]
    for state in states:
        try:
            fh = open(state[0], "rb")
        except OSError:
            errors.append(state[0])
            continue
        opened.append([state, fh, hashlib.md5(), 0])
        prog.tick(opened=1)

    active = [opened]
    while active:
        if STOP.is_set():                    # interrupted: close everything, stop
            for members in active + [opened]:
                for member in members:
                    try:
                        member[1].close()
                    except OSError:
                        pass
            raise KeyboardInterrupt
        bucket = active.pop()
        if not bucket:
            continue
        by_chunk: dict[bytes, list] = {}
        broken = []
        for member in bucket:
            try:
                block = member[1].read(CHUNK)
            except OSError:
                broken.append(member)
                continue
            prog.tick(nbytes=len(block))
            member[3] += len(block)
            member[2].update(block)
            by_chunk.setdefault(block, []).append(member)

        for member in broken:                       # I/O error mid-read
            try:
                member[1].close()
            except OSError:
                pass
            errors.append(member[0][0])
            prog.tick(errors=1)

        for block, grp in by_chunk.items():
            if block == b"":                         # EOF -> identical
                if len(grp) == 1:
                    singletons.append(_park(grp[0]))
                else:
                    for member in grp:
                        md5s[member[0][0]] = member[2].hexdigest()
                        try:
                            member[1].close()
                        except OSError:
                            pass
                    clusters.append([m[0][0] for m in grp])
            elif len(grp) == 1:
                singletons.append(_park(grp[0]))
            else:
                active.append(grp)                   # still equal: keep reading

    return clusters, singletons, md5s, errors


def _park(member):
    """Close a dropped singleton and remember its consumed-prefix digest."""
    state, fh, md5, consumed = member
    try:
        fh.close()
    except OSError:
        pass
    return (state[0], state[1], state[2], consumed, md5.hexdigest())


def process_group(size: int, members, window: int, prog: Progress) -> dict:
    """Verify one size-group; returns a cache record (also used as report data)."""
    clusters, md5s, errors = [], {}, []
    frontier = list(members)                 # [(path, size, mtime_ns)]
    seen_frontiers: set[frozenset] = set()

    while len(frontier) > 1:
        if STOP.is_set():
            raise KeyboardInterrupt
        parked = []
        for i in range(0, len(frontier), window):
            win = frontier[i:i + window]
            cl, sn, md, er = lockstep_window(win, prog)
            clusters.extend(cl)
            md5s.update(md)
            errors.extend(er)
            parked.extend(sn)

        # re-join dropped files that share an identical prefix
        by_prefix: dict[tuple[int, str], list] = {}
        for path, sz, mt, plen, phex in parked:
            by_prefix.setdefault((plen, phex), []).append((path, sz, mt))
        frontier = [m for grp in by_prefix.values() if len(grp) > 1 for m in grp]

        sig = frozenset(m[0] for m in frontier)
        if sig and sig in seen_frontiers:     # md5-collision paranoia guard
            log(f"  WARNING: no progress in group size={size}, stopping early")
            break
        seen_frontiers.add(sig)

    return {
        "sig": group_sig(members),
        "size": size,
        "clusters": clusters,
        "md5": md5s,
        "errors": errors,
    }


def group_sig(members) -> str:
    h = hashlib.sha256()
    for path, size, mtime_ns in sorted(members):
        h.update(f"{path}|{size}|{mtime_ns}\n".encode("utf-8", "surrogateescape"))
    return h.hexdigest()


# --------------------------------------------------------------------------
# checkpoint cache (makes interrupted runs resumable)
# --------------------------------------------------------------------------

class Cache:
    def __init__(self, path: str) -> None:
        self.path = path            # "" == disabled
        self.records: dict[str, dict] = {}
        self.hits = 0
        self.writes = 0
        self._fh = None
        self._pending = 0
        if path and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                            if "sig" in rec and "clusters" in rec:
                                self.records[rec["sig"]] = rec
                        except json.JSONDecodeError:
                            continue
            except OSError:
                pass

    def get(self, sig: str) -> dict | None:
        if not self.path:
            return None
        rec = self.records.get(sig)
        if rec is not None:
            self.hits += 1
        return rec

    def put(self, rec: dict) -> None:
        if not self.path:
            return
        if self._fh is None:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self._fh = open(self.path, "a", encoding="utf-8")
        self._fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self.records[rec["sig"]] = rec
        self.writes += 1
        # flush every group: a checkpoint that is not flushed is progress
        # that could be lost if the process dies
        self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
            self._fh = None


def cache_file_for(root: str) -> str:
    digest = hashlib.md5(os.path.abspath(root).encode()).hexdigest()[:16]
    base = os.path.join(os.path.expanduser("~/.cache"), "remove-duplicates")
    return os.path.join(base, f"{digest}.jsonl")


# --------------------------------------------------------------------------
# keep / move
# --------------------------------------------------------------------------

def creation_time(path: str) -> float:
    """Creation (birth) time when available, else modification time."""
    try:
        st = os.stat(path)
    except OSError:
        return 0.0
    bt = getattr(st, "st_birthtime", None)
    return float(bt) if bt is not None else st.st_mtime


def choose_keeper(cluster: list[str], mode: str) -> str:
    def depth(p: str) -> int:
        return p.count(os.sep)

    if mode == "path":
        return min(cluster)
    if mode == "newest":
        return max(cluster, key=lambda p: (creation_time(p), -depth(p), p))
    return min(cluster, key=lambda p: (creation_time(p), depth(p), p))


def unique_dest(dest_root: str, rel_path: str, taken: set[str]) -> str:
    cand = os.path.join(dest_root, rel_path)
    if cand not in taken and not os.path.exists(cand):
        taken.add(cand)
        return cand
    base, ext = os.path.splitext(rel_path)
    n = 2
    while True:
        cand = os.path.join(dest_root, f"{base}_dup{n}{ext}")
        if cand not in taken and not os.path.exists(cand):
            taken.add(cand)
            return cand
        n += 1


def raise_fd_limit() -> int:
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        want = 65536
        if soft < want:
            resource.setrlimit(resource.RLIMIT_NOFILE, (min(want, hard), hard))
            soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        return soft
    except (ValueError, OSError):
        try:
            return resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        except Exception:
            return 256


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Find 100% identical files (byte-for-byte + MD5), keep one "
                    "copy, move the rest into a dated folder.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=("examples:\n"
                "  python3 remove_duplicates.py\n"
                "  python3 remove_duplicates.py --apply\n"
                "  python3 remove_duplicates.py --apply --keep newest --workers 4\n"
                "  python3 remove_duplicates.py --images-only --apply\n"))
    p.add_argument("--root", default="/mnt/d/Pictures", help="folder to scan (default: %(default)s)")
    p.add_argument("--dest", default=None,
                   help="folder that receives the duplicates "
                        "(default: duplicates_<YYYYmmdd_HHMMSS> inside root)")
    p.add_argument("--apply", action="store_true",
                   help="actually MOVE the files (default is a safe dry run)")
    p.add_argument("--keep", choices=("oldest", "newest", "path"), default="oldest",
                   help="which copy stays in place (default: %(default)s)")
    p.add_argument("--images-only", action="store_true",
                   help="only consider known image extensions (default: all types)")
    p.add_argument("--workers", type=int, default=DEFAULT_READ_WORKERS,
                   help="parallel read/compare threads (default: %(default)s, "
                        "4 saturates a cold drvfs/9p mount)")
    p.add_argument("--scan-workers", type=int, default=DEFAULT_SCAN_WORKERS,
                   help="parallel directory scan threads (default: %(default)s)")
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW,
                   help="max files compared simultaneously (default: %(default)s)")
    p.add_argument("--trust-mtime", action="store_true",
                   help="SKIP files whose (size, modification time) is unique -- "
                        "3-10x faster but MAY MISS duplicates that carry a "
                        "different timestamp (default: off, complete scan)")
    p.add_argument("--report", default=None, help="path of the report file")
    p.add_argument("--no-cache", action="store_true",
                   help="ignore/disable the resume checkpoint cache")
    p.add_argument("--clear-cache", action="store_true",
                   help="delete the checkpoint cache for this root before running")
    p.add_argument("--scan-duplicate-folders", action="store_true",
                   help="also scan folders named duplicates_* (default: skipped)")
    p.add_argument("--exclude", action="append", default=[], metavar="DIR",
                   help="folder to exclude (repeatable)")
    return p


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    started = datetime.now()
    root = os.path.abspath(args.root)
    if not os.path.isdir(root):
        log(f"ERROR: root folder does not exist: {root}")
        return 2

    dest_name = args.dest or f"duplicates_{started.strftime('%Y%m%d_%H%M%S')}"
    dest_root = dest_name if os.path.isabs(dest_name) else os.path.join(root, dest_name)
    dest_root = os.path.abspath(dest_root)
    if dest_root == root:
        log("ERROR: --dest must be different from --root")
        return 2

    excluded: set[str] = {dest_root}
    for e in args.exclude:
        excluded.add(os.path.abspath(e) if os.path.isabs(e) else os.path.join(root, e))

    fd_soft = raise_fd_limit()
    window = max(8, min(args.window, (fd_soft - 256) // max(args.workers, 1)))

    mode = "APPLY (files will be moved)" if args.apply else "DRY RUN (nothing will be moved)"
    log("=" * 78)
    log(f"duplicate remover v2  |  started {started:%Y-%m-%d %H:%M:%S}")
    log(f"root    : {root}")
    log(f"dest    : {dest_root}")
    log(f"mode    : {mode}")
    log(f"keep    : {args.keep}  |  filter: {'image extensions' if args.images_only else 'ALL file types'}"
        f"  |  read workers: {args.workers}  window: {window}")
    log("=" * 78)

    # checkpoint cache ------------------------------------------------------
    cache_path = "" if args.no_cache else cache_file_for(root)
    if args.clear_cache and cache_path:
        try:
            os.remove(cache_path)
        except OSError:
            pass
    cache = Cache(cache_path)
    log(f"cache   : {cache.path or '(disabled)'}  ({len(cache.records):,} verified groups stored)")

    # report ----------------------------------------------------------------
    if args.report:
        report_path = os.path.abspath(args.report)
    elif args.apply:
        os.makedirs(dest_root, exist_ok=True)
        report_path = os.path.join(dest_root, "duplicates_report.txt")
    else:
        report_path = os.path.abspath(
            f"duplicates_report_{started.strftime('%Y%m%d_%H%M%S')}.txt")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    report_fh = open(report_path, "w", encoding="utf-8", errors="replace")

    def report(line: str) -> None:
        report_fh.write(line + "\n")

    report(f"# duplicate report  {started:%Y-%m-%d %H:%M:%S}")
    report(f"# root={root}")
    report(f"# dest={dest_root}")
    report(f"# mode={'apply' if args.apply else 'dry-run'} keep={args.keep}")
    report("# proof: same size + byte-for-byte equal + full MD5 per group")
    report("")

    clusters_all: list[list[str]] = []
    md5_all: dict[str, str] = {}
    error_files: list[str] = []

    interrupted = False
    try:
        # ---------------- phase 1: single parallel scan ---------------------
        log("\n[1/3] Scanning (one parallel pass over all folders)...")
        metas, total_files, total_bytes = parallel_scan(
            root, excluded, args.images_only, not args.scan_duplicate_folders,
            args.scan_workers)
        log(f"  -> {total_files:,} files, {human(total_bytes)} scanned")

        groups, total_candidates, skipped_mtime = build_groups(metas, args.trust_mtime)
        log(f"  -> {len(groups):,} size groups with >1 file, "
            f"{total_candidates:,} candidate files")
        if args.trust_mtime and skipped_mtime:
            log(f"  -> --trust-mtime: skipped {skipped_mtime:,} files whose "
                f"(size,mtime) is unique (may miss some duplicates)")
        del metas
        if not groups:
            log("\nNo duplicates possible: every file has a unique size.")
            report("RESULT: no duplicate files found.")
            report_fh.close()
            cache.close()
            log(f"\nReport: {report_path}")
            return 0

        # ---------------- phase 2: verify (checkpointed) --------------------
        log(f"\n[2/3] Verifying {total_candidates:,} candidate files "
            f"(1 open per file, byte-compare + MD5 in one pass)...")
        prog = Progress("verify", total_candidates, len(groups))

        # schedule: heaviest first
        todo = []
        for size, members in groups:
            rec = cache.get(group_sig(members))
            if rec is not None:
                prog.tick(cached=len(members))
                prog.groups_done += 1
                clusters_all.extend(rec["clusters"])
                md5_all.update(rec.get("md5", {}))
                error_files.extend(rec.get("errors", []))
            else:
                todo.append((size, members))

        cached_groups = cache.hits
        log(f"  -> {len(todo):,} groups to verify, {cached_groups:,} from cache")

        # NOTE: explicit pool (not `with`) so an interrupt cancels the queue
        # instead of waiting for every queued group to finish.
        pool = ThreadPoolExecutor(max_workers=args.workers)
        try:
            futures = {}
            todo_iter = iter(todo)

            def refill() -> None:
                for size, members in todo_iter:
                    fut = pool.submit(process_group, size, members, window, prog)
                    futures[fut] = (size, members)
                    if len(futures) >= args.workers * 4:
                        break

            refill()
            while futures:
                done, _pending = wait(set(futures), return_when=FIRST_COMPLETED)
                for fut in done:
                    size, members = futures.pop(fut)
                    try:
                        rec = fut.result()
                    except KeyboardInterrupt:
                        raise
                    except Exception as exc:              # noqa: BLE001
                        log(f"  ERROR verifying group size={size}: {exc}")
                        prog.tick(opened=0, force=True)
                        continue
                    if not args.no_cache:
                        cache.put(rec)
                    for cl in rec["clusters"]:
                        clusters_all.append(cl)
                    md5_all.update(rec["md5"])
                    error_files.extend(rec.get("errors", []))
                    prog.groups_done += 1
                    prog.tick()
                refill()
        finally:
            pool.shutdown(wait=not STOP.is_set(), cancel_futures=STOP.is_set())
        prog.tick(force=True)
        log(f"  -> {len(clusters_all):,} duplicate groups found, "
            f"{prog.errors:,} unreadable files (left in place)")

        # ---------------- phase 3: keep one, move the rest -------------------
        error_set = set(error_files)
        cleaned: list[list[str]] = []
        for cluster in clusters_all:
            cl = [p for p in cluster if p not in error_set]
            if len(cl) >= 2:
                cleaned.append(cl)
        clusters_all = cleaned

        moves: list[tuple[str, str, str]] = []
        for cluster in clusters_all:
            keeper = choose_keeper(cluster, args.keep)
            for p in cluster:
                if p != keeper:
                    moves.append((p, os.path.relpath(p, root), keeper))

        dup_files = len(moves)
        moved_bytes = 0
        for src, _rel, _k in moves:
            try:
                moved_bytes += os.path.getsize(src)
            except OSError:
                pass

        log("\n" + "=" * 78)
        log(f"RESULT: {len(clusters_all):,} duplicate groups, "
            f"{dup_files:,} extra copies")
        log(f"        {total_files - dup_files:,} files stay, "
            f"{dup_files:,} files {'to be ' if not args.apply else ''}moved "
            f"({human(moved_bytes)})")
        log("=" * 78)

        for cluster in clusters_all:
            keeper = choose_keeper(cluster, args.keep)
            try:
                size_str = str(os.path.getsize(keeper))
            except OSError:
                size_str = "?"
            report("")
            report(f"GROUP copies={len(cluster)} size={size_str} "
                   f"md5={md5_all.get(keeper, '?')}")
            for p in sorted(cluster):
                report(f"  {'KEEP' if p == keeper else 'MOVE'}  {p}")

        if error_files:
            report("")
            for p in sorted(set(error_files)):
                report(f"UNREADABLE  {p}")

        if args.apply and moves:
            log("\nMoving duplicate files...")
            prog2 = Progress("moving", len(moves))
            taken: set[str] = set()
            ok = failed = 0
            for src, rel, _keeper in moves:
                target = unique_dest(dest_root, rel, taken)
                try:
                    os.makedirs(os.path.dirname(target), exist_ok=True)
                    try:
                        os.replace(src, target)
                    except OSError:
                        shutil.move(src, target)
                except OSError as exc:
                    failed += 1
                    report(f"MOVE FAILED  {src} -> {target} : {exc}")
                    log(f"\n  MOVE FAILED: {src} ({exc})")
                    prog2.tick(force=True)
                    continue
                report(f"MOVED  {src}  ->  {target}")
                ok += 1
                prog2.tick()
            prog2.tick(force=True)
            log(f"\n  moved {ok:,} files into {dest_root}"
                + (f"  ({failed} FAILED)" if failed else ""))
        elif moves:
            log("\nDry run -- first 20 files that WOULD be moved:")
            for src, rel, _keeper in moves[:20]:
                log(f"  {src}\n      -> {os.path.join(dest_root, rel)}")
            if len(moves) > 20:
                log(f"  ... and {len(moves) - 20:,} more (see report)")

        report("")
        report(f"# finished {datetime.now():%Y-%m-%d %H:%M:%S}")
        report(f"# groups={len(clusters_all)} moved_files={dup_files} "
               f"moved_bytes={moved_bytes}")
        report_fh.close()
        cache.close()

        log(f"\nReport: {report_path}")
        log(f"Checkpoint: {cache.writes:,} groups saved "
            f"({cache.path if not args.no_cache else 'cache disabled'})")
        if not args.apply and moves:
            log("This was a DRY RUN. Re-run with --apply to really move them "
                "(verified groups are reused from the checkpoint, it will be fast):")
            log(f"  python3 {os.path.abspath(__file__)} --apply --keep {args.keep}"
                + (" --images-only" if args.images_only else ""))
        return 0

    except KeyboardInterrupt:
        interrupted = True
        report_fh.write("\n# INTERRUPTED -- partial results (resume by rerunning)\n")
        for cluster in clusters_all:
            keeper = choose_keeper(cluster, args.keep)
            report(f"GROUP copies={len(cluster)} size=? md5={md5_all.get(keeper, '?')}")
            for p in sorted(cluster):
                report(f"  {'KEEP' if p == keeper else 'MOVE'}  {p}")
        report_fh.close()
        cache.close()
        log("\nINTERRUPTED. Already-verified groups are checkpointed:")
        log(f"  {cache.writes:,} groups saved to {cache.path}")
        log("Rerun the same command to continue where it left off.")
        log(f"Partial report: {report_path}")
        return 130
    finally:
        if not interrupted:
            try:
                report_fh.close()
                cache.close()
            except Exception:                              # noqa: BLE001
                pass


def _sig_interrupt(_signum, _frame):
    """Ctrl+C / SIGTERM: stop workers at the next safe point and abort."""
    STOP.set()
    raise KeyboardInterrupt


if __name__ == "__main__":
    try:
        signal.signal(signal.SIGINT, _sig_interrupt)
        signal.signal(signal.SIGTERM, _sig_interrupt)
    except (ValueError, OSError):
        pass
    sys.exit(main())

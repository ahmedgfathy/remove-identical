#!/usr/bin/env python3
"""Build a test tree and verify remove_duplicates.py end to end."""
import hashlib
import os
import shutil
import subprocess
import sys

BASE = "/home/xinreal/remove-identical/tests/fixture"
SCRIPT = "/home/xinreal/remove-identical/remove_duplicates.py"


def w(path, data: bytes):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def run(*args):
    r = subprocess.run([sys.executable, "-u", SCRIPT, *args],
                       capture_output=True, text=True)
    print(r.stdout)
    if r.stderr:
        print(r.stderr, file=sys.stderr)
    assert r.returncode == 0, f"exit={r.returncode}"
    return r.stdout


def cache_path_for(root):
    h = hashlib.md5(root.encode()).hexdigest()[:16]
    return os.path.expanduser(f"~/.cache/remove-duplicates/{h}.jsonl")


def main():
    shutil.rmtree(BASE, ignore_errors=True)
    os.remove(cache_path_for(BASE)) if os.path.exists(cache_path_for(BASE)) else None
    for f in os.listdir("."):
        if f.startswith("duplicates_report_"):
            os.remove(f)

    # A: exact duplicates, 3 copies, different dirs + extensions
    a = bytes(range(256)) * 40                      # 10 KiB
    w(f"{BASE}/photos/a.jpg", a)
    w(f"{BASE}/photos/sub/b.png", a)
    w(f"{BASE}/photos/sub/deep/c.jpeg", a)

    # B: same size, one byte differs -> must NOT be flagged
    b = b"X" * 5000
    w(f"{BASE}/photos/b1.bin", b)
    w(f"{BASE}/photos/b2.bin", b[:-1] + b"Y")

    # C: large file (> 64 KiB so it is read in multiple blocks), 2 copies
    c = os.urandom(300 * 1024)
    w(f"{BASE}/big/c1.dat", c)
    w(f"{BASE}/other/c2.dat", c)

    # C2: large file, same first block, DIFFERENT later -> must NOT be flagged
    c2 = bytearray(os.urandom(300 * 1024))
    c2[-10:] = b"ZZZZZZZZZZ"
    w(f"{BASE}/big/d1.dat", bytes(c2))
    c2b = bytearray(c2)
    c2b[100 * 1024] ^= 0xFF                          # differs in block 2
    w(f"{BASE}/other/d2.dat", bytes(c2b))

    # D: unique file
    w(f"{BASE}/photos/unique.jpg", b"unique-content-123")

    # E: unicode + spaces, same name in two dirs, identical
    e = "héllo wörld ".encode() * 100
    w(f"{BASE}/photos/测试 图.jpg", e)
    w(f"{BASE}/other/测试 图.jpg", e)

    # F: identical content, SAME mtime -> tiebreak by shallowest path (alpha)
    f_data = b"F" * 2048
    w(f"{BASE}/zeta/f.jpg", f_data)
    w(f"{BASE}/alpha/f.jpg", f_data)
    os.utime(f"{BASE}/zeta/f.jpg", (1000000, 1000000))
    os.utime(f"{BASE}/alpha/f.jpg", (1000000, 1000000))

    # G: empty files, 3 copies (size 0)
    w(f"{BASE}/e1.txt", b"")
    w(f"{BASE}/e2.txt", b"")
    w(f"{BASE}/sub/e3.txt", b"")

    # H: symlink to a duplicate (must be ignored)
    os.symlink(f"{BASE}/photos/a.jpg", f"{BASE}/link_to_a.jpg")

    # I: an old duplicates_* folder must be excluded
    w(f"{BASE}/duplicates_20250101_000000/old.jpg", a)

    # ---- dry run 1 (fresh, cold cache) ----------------------------------
    out = run("--root", BASE)
    assert "DRY RUN" in out
    assert os.path.exists(f"{BASE}/photos/a.jpg"), "dry run must not move anything"
    # groups: A(3), C(2), C2?? -> C2 same size! (300KiB both) so C+C2 share size group
    #         E(2), F(2), empty(3)  -> A,C,E,F,empty = 5 groups, 7 dup copies
    assert "5 duplicate groups" in out, out
    assert "7 extra copies" in out, out
    moved_section = out.split("WOULD be moved")[1]
    assert "b1.bin" not in moved_section, "near-identical wrongly flagged"
    assert "d1.dat" not in moved_section, "same-head-different-body wrongly flagged"
    assert os.path.exists(cache_path_for(BASE)), "checkpoint cache not written"
    cache_lines = sum(1 for _ in open(cache_path_for(BASE), encoding="utf-8"))
    # 6 size-groups verified: A, B(near-identical), C+C2, E, F, empty
    assert cache_lines == 6, cache_lines

    # ---- dry run 2 (must come from checkpoint and be fast) --------------
    out = run("--root", BASE)
    assert "0 groups to verify" in out, out
    assert "6 from cache" in out, out
    assert "5 duplicate groups" in out and "7 extra copies" in out, out

    # ---- apply (also fully cached) ---------------------------------------
    out = run("--root", BASE, "--apply")
    assert "APPLY (files will be moved)" in out
    assert "0 groups to verify" in out, out
    assert "6 from cache" in out, out
    assert "5 duplicate groups" in out and "7 extra copies" in out, out

    dup_dirs = [d for d in os.listdir(BASE)
                if d.startswith("duplicates_") and d != "duplicates_20250101_000000"]
    assert len(dup_dirs) == 1, dup_dirs
    D = os.path.join(BASE, dup_dirs[0])

    def exists(p):
        return os.path.exists(p)

    a_left = sum(exists(p) for p in [f"{BASE}/photos/a.jpg", f"{BASE}/photos/sub/b.png",
                                     f"{BASE}/photos/sub/deep/c.jpeg"])
    assert a_left == 1, f"group A: {a_left} copies left"
    c_left = sum(exists(p) for p in [f"{BASE}/big/c1.dat", f"{BASE}/other/c2.dat"])
    assert c_left == 1, "group C"
    assert sum(exists(p) for p in [f"{BASE}/big/d1.dat", f"{BASE}/other/d2.dat"]) == 2, \
        "group C2 must stay untouched"
    e_left = sum(exists(p) for p in [f"{BASE}/photos/测试 图.jpg", f"{BASE}/other/测试 图.jpg"])
    assert e_left == 1, "group E"
    assert exists(f"{BASE}/alpha/f.jpg") and not exists(f"{BASE}/zeta/f.jpg"), "group F"
    g_left = sum(exists(p) for p in [f"{BASE}/e1.txt", f"{BASE}/e2.txt", f"{BASE}/sub/e3.txt"])
    assert g_left == 1, "group G"
    assert exists(f"{BASE}/photos/b1.bin") and exists(f"{BASE}/photos/b2.bin"), "group B"
    assert exists(f"{BASE}/photos/unique.jpg"), "unique file moved!"
    assert os.path.islink(f"{BASE}/link_to_a.jpg"), "symlink touched"
    assert exists(f"{BASE}/duplicates_20250101_000000/old.jpg"), "old dup folder touched"

    files_moved = []
    for dp, _dn, fn in os.walk(D):
        for n in fn:
            p = os.path.join(dp, n)
            if not p.endswith("duplicates_report.txt"):
                files_moved.append(p)
    assert len(files_moved) == 7, [os.path.relpath(p, BASE) for p in files_moved]
    rels = {os.path.relpath(p, D) for p in files_moved}
    assert "photos/sub/b.png" in rels or "photos/sub/deep/c.jpeg" in rels, rels
    assert any(r.endswith("测试 图.jpg") for r in rels), rels

    report = open(os.path.join(D, "duplicates_report.txt"), encoding="utf-8").read()
    assert report.count("MOVED") == 7, report.count("MOVED")
    assert "md5=" in report, "report must contain MD5 per group"
    # md5 of the empty file group must be the well-known empty digest
    assert "d41d8cd98f00b204e9800998ecf8427e" in report, "empty-file md5 missing"

    # ---- rerun after apply: leftover files re-verified (cache miss), no dups
    out = run("--root", BASE)
    assert "0 duplicate groups" in out, out

    print("ALL TESTS PASSED")
    print("moved files:")
    for p in sorted(files_moved):
        print("   ", os.path.relpath(p, BASE))


if __name__ == "__main__":
    main()

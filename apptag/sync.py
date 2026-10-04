"""Sinkronisasi kamus: ambil v2fly/domain-list-community, bangun kamus, bandingkan dengan versi terbaru di ClickHouse,
simpan bila berubah, lalu reload DICTIONARY ClickHouse. Consumer memuat versi baru sendiri tanpa restart.

Sumber dipin ke commit: SHA diambil dengan `git ls-remote`, lalu tarball commit itu yang diunduh, sehingga setiap versi
kamus bisa dilacak ke commit upstream yang persis sama.
"""
from __future__ import annotations

import io
import logging
import subprocess
import tarfile
import tempfile
import urllib.request
from pathlib import Path

from . import dictionary as D
from . import store

REPO = "https://github.com/v2fly/domain-list-community"
log = logging.getLogger("apptag.sync")


def upstream_sha() -> str:
    out = subprocess.run(["git", "ls-remote", REPO, "refs/heads/master"], capture_output=True, text=True, timeout=60, check=True).stdout
    return out.split()[0]


def fetch(sha: str, dest: Path) -> Path:
    url = f"https://codeload.github.com/v2fly/domain-list-community/tar.gz/{sha}"
    with urllib.request.urlopen(url, timeout=120) as r:
        data = r.read()
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
        tf.extractall(dest, filter="data")
    return next(dest.iterdir()) / "data"


def run(cfg: store.CHConfig, taxonomy: Path, local: Path, v2fly_dir: Path | None = None, upstream: str | None = None) -> dict:
    client = store.connect(cfg)
    store.migrate(client, cfg)
    with tempfile.TemporaryDirectory() as tmp:
        if v2fly_dir is None:
            sha = upstream_sha()
            v2fly_dir, upstream = fetch(sha, Path(tmp)), f"v2fly/domain-list-community@{sha[:12]}"
        new = D.build(Path(v2fly_dir), taxonomy, local, upstream or str(v2fly_dir))
    cur = store.latest_version(client)
    if cur == new.version:
        log.info("dictionary unchanged (%s)", cur)
        return {"changed": False, "version": cur}
    old = store.load_dictionary(client, cur) if cur else None
    df = D.diff(old, new)
    store.save_dictionary(client, cfg, new, df)
    log.info("dictionary %s -> %s: +%d -%d moved %d", cur, new.version, len(df["added"]), len(df["removed"]), len(df["moved"]))
    return {"changed": True, "version": new.version, "previous": cur, "added": len(df["added"]), "removed": len(df["removed"]),
            "moved": len(df["moved"]), "rules": len(new.rules), "upstream": new.upstream}

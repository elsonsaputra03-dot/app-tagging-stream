"""apptag: build | eval | sync | produce | consume."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from . import dictionary as D, evaluate as E, generate as G, match as M, store, stream, sync

def dict_dir() -> Path:
    """Folder kamus (taxonomy.yaml, local_id.yaml): APPTAG_DICT_DIR, lalu ./dictionary, lalu folder repo.
    Saat terpasang sebagai paket (mis. di container), kode ada di site-packages, bukan di repo: versi awal mencari
    relatif terhadap kode dan gagal di Docker (ditemukan saat docker compose pertama kali dijalankan)."""
    for c in (os.getenv("APPTAG_DICT_DIR"), Path.cwd() / "dictionary", Path(__file__).resolve().parent.parent / "dictionary"):
        if c and (Path(c) / "taxonomy.yaml").exists():
            return Path(c)
    raise FileNotFoundError("dictionary folder not found; set APPTAG_DICT_DIR to the folder containing taxonomy.yaml")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="apptag", description="Streaming app tagging: hostnames -> applications")
    ap.add_argument("--v2fly", type=Path, help="local v2fly data dir (default: fetch the latest upstream commit)")
    ap.add_argument("--taxonomy", type=Path, help="default: <dictionary folder>/taxonomy.yaml")
    ap.add_argument("--local", type=Path, help="default: <dictionary folder>/local_id.yaml")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("eval", help="precision/recall on labelled synthetic events"); e.add_argument("--n", type=int, default=100_000)
    sy = sub.add_parser("sync", help="build the dictionary and store a new version in ClickHouse if it changed")
    sy.add_argument("--every", type=float, help="repeat every N seconds (scheduler mode)")
    p = sub.add_parser("produce", help="send synthetic events to Kafka")
    p.add_argument("--n", type=int, default=100_000); p.add_argument("--rate", type=float, default=2000)
    c = sub.add_parser("consume", help="tag events from Kafka into ClickHouse")
    c.add_argument("--max", type=int); c.add_argument("--idle-stop", type=float); c.add_argument("--reload-every", type=float, default=30)
    a = ap.parse_args(argv)
    a.taxonomy = a.taxonomy or dict_dir() / "taxonomy.yaml"
    a.local = a.local or dict_dir() / "local_id.yaml"
    logging.basicConfig(level=logging.INFO if a.verbose else logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    bootstrap, cfg = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"), store.CHConfig()

    if a.cmd == "sync":
        while True:
            try:
                print(json.dumps(sync.run(cfg, a.taxonomy, a.local, a.v2fly)), flush=True)
            except Exception as exc:  # noqa: BLE001 - mode terjadwal: catat dan coba lagi pada putaran berikutnya
                if not a.every:
                    raise
                logging.error("sync failed: %s", exc)
            if not a.every:
                return 0
            import time; time.sleep(a.every)
    if a.cmd == "consume":
        print(json.dumps(stream.consume(bootstrap, store.connect(cfg), max_events=a.max, idle_stop=a.idle_stop, reload_every=a.reload_every))); return 0
    d = D.build(a.v2fly, a.taxonomy, a.local) if a.v2fly else store.load_dictionary(store.connect(cfg), store.latest_version(store.connect(cfg)))
    if a.cmd == "eval":
        print(json.dumps(E.evaluate(M.Matcher(d), G.events(d, a.n)), indent=1)); return 0
    if a.cmd == "produce":
        print(json.dumps({"sent": stream.produce(bootstrap, d, a.n, a.rate)})); return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())

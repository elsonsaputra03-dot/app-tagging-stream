"""Event hostname sintetis berlabel, dibangkitkan dari kamus. Tidak ada data pelanggan: ID pengguna dan lokasi acak.

Campuran host (bisa diatur) meniru kondisi lapangan:
  exact     nama host persis dari kamus                       -> harus ditandai benar
  subdomain subdomain/shard dari domain kamus (r5---sn-xxx..) -> harus ditandai benar
  variant   domain baru milik aplikasi, belum ada di kamus     -> diharapkan tertangkap fuzzy
  lookalike merek + kata pancingan di domain lain              -> TIDAK boleh diberi aplikasi
  unknown   domain lain (termasuk kata mirip merek: grabbag)   -> TIDAK boleh diberi aplikasi
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from .dictionary import Dictionary

MIX = {"exact": 0.10, "subdomain": 0.66, "variant": 0.05, "variant_hard": 0.03, "lookalike": 0.02, "unknown": 0.11, "unknown_hard": 0.03}
# variant_hard dan unknown_hard memakai pola yang TIDAK dipakai saat merancang aturan fuzzy, supaya evaluasi tidak hanya
# mengukur konsistensi kode dengan generatornya sendiri (versi awal evaluasi memberi 100%/100% karena hal itu).
REGIONS = ["Jawa Timur", "DKI Jakarta", "Jawa Barat", "Sumatera Utara", "Kalimantan Timur", "Sulawesi Selatan", "Bali"]
WORDS = ["kopi", "batik", "rumah", "pasar", "kebun", "laut", "gunung", "warung", "sinar", "jaya", "maju", "data", "kota",
         "berita", "toko", "motor", "kereta", "bunga", "langit", "tanah", "grabbag", "lineart", "danamon", "spotlight", "appletree"]
BAIT = ["login", "verify", "account", "promo", "bonus", "gift", "free", "support"]
PREFIX = ["api", "www", "m", "static", "img{n}", "edge-{n}-sg", "cdn{n}", "r{n}---sn-{r}", "v{n}", "media", "upload"]


def _prefix(rnd: random.Random) -> str:
    p = rnd.choice(PREFIX)
    return p.format(n=rnd.randint(1, 40), r="".join(rnd.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8)))


def events(d: Dictionary, n: int, seed: int = 7, start: datetime | None = None, mix: dict | None = None):
    rnd, mix = random.Random(seed), mix or MIX
    start = start or datetime(2026, 10, 1, tzinfo=timezone.utc)
    by_app: dict[str, dict] = {}
    for r in d.rules:
        a = by_app.setdefault(r.app, {"full": [], "domain": [], "category": r.category})
        if r.kind in ("full", "domain"):
            a[r.kind].append(r.pattern)
    exact_owner = {r.pattern: r.app for r in d.rules if r.kind == "full"}
    domain_owner = {r.pattern: r.app for r in d.rules if r.kind == "domain"}

    def owner(host: str) -> str | None:
        """Label kebenaran untuk host yang dibentuk dari kamus = aturan paling spesifik yang berlaku. Contoh nyata:
        static.siege-amazon.com adalah subdomain amazon, tetapi v2fly punya aturan full: khusus yang menandainya primevideo."""
        if host in exact_owner:
            return exact_owner[host]
        labels = host.split(".")
        return next((domain_owner[".".join(labels[i:])] for i in range(len(labels)) if ".".join(labels[i:]) in domain_owner), None)

    apps = sorted(by_app)
    weights = [1 / (i + 1) ** 0.9 for i in range(len(apps))]           # popularitas ala Zipf
    rnd.shuffle(apps)
    brandable = [a for a in apps if len(a) >= 5 and a.isalpha()]
    kinds, kw = list(mix), list(mix.values())
    for i in range(n):
        kind = rnd.choices(kinds, kw)[0]
        app = rnd.choices(apps, weights)[0]
        a = by_app[app]
        if kind == "exact" and not a["full"]:
            kind = "subdomain"
        if kind in ("variant", "variant_hard", "lookalike", "unknown_hard") and app not in brandable:
            app = rnd.choice(brandable); a = by_app[app]
        if kind == "exact":
            host = rnd.choice(a["full"]); truth = owner(host)
        elif kind == "subdomain":
            host = f"{_prefix(rnd)}.{rnd.choice(a['domain'])}"; truth = owner(host)
        elif kind == "variant":
            host, truth = f"{_prefix(rnd)}.{app}-{rnd.choice(['cdn', 'static', 'edge', 'media', 'assets'])}.{rnd.choice(['net', 'com', 'io'])}", app
        elif kind == "variant_hard":       # domain sah milik aplikasi dengan pola yang tidak dirancang untuk fuzzy
            host, truth = f"{rnd.choice(['', 'www.', 'm.'])}{app}{rnd.choice(['tv', 'hd', 'play', 'now', 'go', 'plus'])}.{rnd.choice(['com', 'net', 'id'])}", app
        elif kind == "unknown_hard":       # situs pihak ketiga yang memakai nama merek: bukan aplikasinya
            host, truth = f"{rnd.choice(['', 'www.'])}{app}-{rnd.choice(['news', 'review', 'tips', 'wiki', 'guide', 'fans', 'tricks'])}.{rnd.choice(['com', 'net', 'id', 'blog'])}", None
        elif kind == "lookalike":
            b = rnd.sample(BAIT, 2)
            host, truth = f"{app}-{b[0]}-{b[1]}.{rnd.choice(['xyz', 'top', 'info', 'site'])}", None
        else:
            host, truth = f"{rnd.choice(['', 'www.', 'cdn.'])}{rnd.choice(WORDS)}{rnd.choice(WORDS)}.{rnd.choice(['com', 'id', 'co.id', 'net', 'org'])}", None
        yield {"ts": (start + timedelta(milliseconds=i * 37)).isoformat(), "subscriber": f"u{rnd.randint(1, 50_000):06d}",
               "region": rnd.choice(REGIONS), "host": host, "bytes": int(rnd.lognormvariate(10, 1.6)),
               "proto": rnd.choice(["dns", "tls-sni", "http"]), "truth_app": truth, "truth_kind": kind}

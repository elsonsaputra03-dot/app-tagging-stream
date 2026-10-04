"""Pencocokan hostname -> aplikasi, berurutan dari yang paling pasti:

1. exact  : nama host persis (aturan full:)
2. suffix : domain atau subdomainnya; yang terpanjang (paling spesifik) menang, mis. googlevideo.com sebelum google.com
3. regexp : pola dari kamus
4. keyword: substring dari kamus
5. fuzzy  : host yang tidak dikenal tetapi memuat merek yang khas (mis. 'tokopedia-static.net'); confidence lebih rendah dan
            bisa dimatikan. Merek = nama aplikasi atau nama domain utuh dari kamus (bukan potongan kata), minimal 5 huruf,
            sehingga 'grab', 'line', 'dana' tidak memicu fuzzy (terlalu sering muncul di kata lain, mis. 'grabbag').
6. lookalike: memuat merek, tetapi di domain lain bersama kata pancingan (login, verify, promo, ...). Pola phishing klasik:
            tidak diberi aplikasi, ditandai untuk ditinjau.
Host yang tidak cocok sama sekali ditandai 'unknown' dan masuk antrean tinjauan, tidak dipaksakan ke kategori.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache

import tldextract

from .dictionary import Dictionary

_EXTRACT = tldextract.TLDExtract(suffix_list_urls=())        # daftar public suffix bawaan paket, tanpa akses jaringan
_SPLIT = re.compile(r"[^a-z]+")
MIN_BRAND_TOKEN = 5
GENERIC = {"cloud", "static", "media", "video", "images", "image", "cdn", "assets", "api", "login", "account", "online",
           "mobile", "store", "games", "music", "global", "service", "services", "content", "update", "download", "secure",
           "access", "support", "status", "events", "photos", "player", "stream", "social", "search", "health", "studio"}
BAIT = {"login", "signin", "verify", "verification", "account", "secure", "update", "promo", "gift", "free", "bonus",
        "reward", "claim", "support", "help", "wallet", "payment", "billing", "unlock", "prize"}


INFRA = {"cdn", "static", "img", "image", "images", "media", "assets", "edge", "api", "apis", "video", "content", "files",
         "upload", "dl", "download", "cache", "stream", "app", "apps", "web", "mobile", "global", "usercontent"}


REGION = {"asia", "sea", "sg", "id", "us", "eu", "jp", "kr", "global", "intl", "apac", "my", "th", "ph", "vn"}


def _infra(rest: str) -> bool:
    return rest.isdigit() or rest in INFRA


@dataclass(frozen=True)
class Tag:
    app: str | None
    category: str | None
    method: str            # exact | suffix | regexp | keyword | fuzzy | unknown
    confidence: float
    rule: str | None = None


class Matcher:
    def __init__(self, d: Dictionary, fuzzy: bool = True):
        self.version, self.fuzzy = d.version, fuzzy
        self.exact = {r.pattern: r for r in d.rules if r.kind == "full"}
        self.suffix = {r.pattern: r for r in d.rules if r.kind == "domain"}
        self.regex = [(re.compile(r.pattern), r) for r in d.rules if r.kind == "regexp"]
        self.keyword = [r for r in d.rules if r.kind == "keyword"]
        tokens: dict[str, set] = {}
        for r in d.rules:
            if r.kind in ("full", "domain"):
                sld = _EXTRACT(r.pattern).domain                  # nama domain utuh, bukan potongan kata
                for t in (sld, r.app):
                    if len(t) >= MIN_BRAND_TOKEN and t.isalpha() and t not in GENERIC:
                        tokens.setdefault(t, set()).add((r.app, r.category))
        # token dari nama domain yang dimiliki lebih dari satu aplikasi bersifat ambigu dan tidak dipakai...
        self.brand = {t: next(iter(a)) for t, a in tokens.items() if len(a) == 1}
        # ...tetapi nama aplikasi selalu milik aplikasinya sendiri ('google' juga muncul di youtube.googleapis.com)
        cats = {r.app: r.category for r in d.rules}
        for a, c in cats.items():
            if len(a) >= MIN_BRAND_TOKEN and a.isalpha():
                self.brand[a] = (a, c)
        self.apps = {a for a in cats if a in self.brand}
        self.tag = lru_cache(maxsize=200_000)(self._tag)

    @staticmethod
    def normalize(host: str) -> str:
        h = (host or "").strip().lower().rstrip(".")
        if h.startswith("[") or h.count(":") > 1:          # literal IPv6
            return h
        return h.split(":", 1)[0]

    def _tag(self, host: str) -> Tag:
        h = self.normalize(host)
        if not h:
            return Tag(None, None, "unknown", 0.0)
        r = self.exact.get(h)
        if r:
            return Tag(r.app, r.category, "exact", 1.0, f"full:{r.pattern}")
        labels = h.split(".")
        for i in range(len(labels)):                        # dari yang terpanjang: a.b.c.com, b.c.com, c.com, com
            # label terakhir ikut dicek: v2fly memuat TLD milik merek (.youtube, .amazon, .gmail), ditemukan saat evaluasi
            r = self.suffix.get(".".join(labels[i:]))
            if r:
                return Tag(r.app, r.category, "suffix", 0.98, f"domain:{r.pattern}")
        for rx, r in self.regex:
            if rx.search(h):
                return Tag(r.app, r.category, "regexp", 0.95, f"regexp:{r.pattern}")
        for r in self.keyword:
            if r.pattern in h:
                return Tag(r.app, r.category, "keyword", 0.8, f"keyword:{r.pattern}")
        if self.fuzzy:
            ext = _EXTRACT(h)
            words = [w for w in _SPLIT.split(f"{ext.subdomain}.{ext.domain}") if w]
            dom = [w for w in _SPLIT.split(ext.domain) if w]
            # merek sebagai kata terpisah di nama domain hanya diterima bila kata lain di nama domain itu kata infrastruktur
            # atau kode wilayah ('google-edge', 'vidio-cdn', 'mobilelegends-asia'); 'netflix-news' adalah situs pihak ketiga.
            hits = {self.brand[w] for w in words if w in self.brand
                    and all(x == w or _infra(x) or x in REGION or x in BAIT for x in dom)}
            # merek menempel pada kata infrastruktur, mis. 'tokopediastatic', 'spotifycdn', 'cdngoogle', 'vidio2'.
            # Sisa kata harus kata infrastruktur atau angka: 'appletree' bukan Apple (ditemukan saat evaluasi).
            hits |= {self.brand[a] for w in words for a in self.apps
                     if w != a and ((w.startswith(a) and _infra(w[len(a):])) or (w.endswith(a) and _infra(w[:-len(a)])))}
            if len(hits) == 1:
                app, cat = next(iter(hits))
                if BAIT & set(words):
                    return Tag(None, None, "lookalike", 0.0, f"brand:{app}")
                return Tag(app, cat, "fuzzy", 0.6, "brand-token")
        return Tag(None, None, "unknown", 0.0)

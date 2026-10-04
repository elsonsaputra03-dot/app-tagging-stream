"""Kamus hostname -> aplikasi dari v2fly/domain-list-community (MIT) ditambah daftar lokal Indonesia.

Format v2fly per file: baris biasa = domain beserta subdomainnya; full: = nama host persis; regexp: = pola;
keyword: = substring; include: = gabungkan daftar lain; atribut @ads/@cn di akhir baris diabaikan untuk penamaan aplikasi.

Satu domain bisa muncul di lebih dari satu daftar (mis. daftar payung 'google' meng-include layanan lain). Aturan resolusinya:
daftar spesifik (yang tidak meng-include daftar lain) menang atas daftar payung; konflik antar daftar spesifik dicatat.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

KINDS = ("full", "domain", "regexp", "keyword")


@dataclass
class Rule:
    kind: str          # full | domain | regexp | keyword
    pattern: str
    app: str
    category: str
    source: str        # v2fly | local


@dataclass
class Dictionary:
    rules: list[Rule]
    version: str
    conflicts: list[dict] = field(default_factory=list)
    upstream: str = ""

    def to_json(self) -> str:
        return json.dumps({"version": self.version, "upstream": self.upstream,
                           "rules": [r.__dict__ for r in self.rules], "conflicts": self.conflicts}, sort_keys=True)

    @staticmethod
    def from_json(text: str) -> "Dictionary":
        d = json.loads(text)
        return Dictionary([Rule(**r) for r in d["rules"]], d["version"], d.get("conflicts", []), d.get("upstream", ""))


def parse_list(text: str) -> dict:
    """Satu file daftar v2fly -> {'full': set, 'domain': set, 'regexp': list, 'keyword': list, 'include': list}."""
    out = {"full": set(), "domain": set(), "regexp": [], "keyword": [], "include": []}
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        line = re.sub(r"\s+@\S+", "", line).strip()          # atribut seperti @ads, @cn
        kind, _, val = line.partition(":") if re.match(r"^(full|domain|regexp|keyword|include):", line) else ("domain", "", line)
        val = val.strip().lower() if kind != "regexp" else val.strip()
        if kind in ("full", "domain"):
            out[kind].add(val.rstrip("."))
        elif kind in ("regexp", "keyword", "include"):
            out[kind].append(val.split(":", 1)[0] if kind == "include" else val)
    return out


def _resolve(name: str, lists: dict, seen: frozenset = frozenset()) -> dict:
    """Ikuti include secara rekursif (dengan perlindungan siklus)."""
    base = lists.get(name)
    if base is None or name in seen:
        return {"full": set(), "domain": set(), "regexp": [], "keyword": []}
    res = {"full": set(base["full"]), "domain": set(base["domain"]), "regexp": list(base["regexp"]), "keyword": list(base["keyword"])}
    for inc in base["include"]:
        sub = _resolve(inc, lists, seen | {name})
        for k in res:
            res[k] = res[k] | sub[k] if isinstance(res[k], set) else res[k] + sub[k]
    return res


def build(v2fly_data: Path, taxonomy: Path, local: Path, upstream: str = "") -> Dictionary:
    tax = yaml.safe_load(taxonomy.read_text())
    raw = {f.name: parse_list(f.read_text(encoding="utf-8", errors="replace")) for f in Path(v2fly_data).iterdir() if f.is_file()}
    owners: dict[tuple, list] = {}                                    # (kind, pattern) -> [(app, category, umbrella)]
    for app, cat in tax["v2fly"].items():
        if app not in raw:
            continue
        umbrella = bool(raw[app]["include"])
        res = _resolve(app, raw)
        for kind in KINDS:
            for p in res[kind]:
                owners.setdefault((kind, p), []).append((app, cat, umbrella, "v2fly"))
    loc = yaml.safe_load(local.read_text())
    for app, d in loc["apps"].items():
        for p in d["domains"]:
            owners.setdefault(("domain", p.lower()), []).append((app, d["category"], False, "local"))
    rules, conflicts = [], []
    for (kind, p), cands in sorted(owners.items()):
        specific = [c for c in cands if not c[2]] or cands
        apps = sorted({c[0] for c in specific})
        if len(apps) > 1:
            conflicts.append({"kind": kind, "pattern": p, "apps": apps})
        app, cat, _, src = sorted(specific)[0]
        rules.append(Rule(kind, p, app, cat, src))
    digest = hashlib.sha256("\n".join(f"{r.kind}|{r.pattern}|{r.app}|{r.category}" for r in rules).encode()).hexdigest()[:12]
    return Dictionary(rules, digest, conflicts, upstream)


def diff(old: Dictionary | None, new: Dictionary) -> dict:
    """Perubahan antar versi: aturan baru, terhapus, dan yang berpindah aplikasi/kategori."""
    o = {(r.kind, r.pattern): r for r in (old.rules if old else [])}
    n = {(r.kind, r.pattern): r for r in new.rules}
    moved = [{"kind": k[0], "pattern": k[1], "from": f"{o[k].app}/{o[k].category}", "to": f"{n[k].app}/{n[k].category}"}
             for k in o.keys() & n.keys() if (o[k].app, o[k].category) != (n[k].app, n[k].category)]
    return {"from": old.version if old else None, "to": new.version,
            "added": sorted(f"{k[0]}:{k[1]}" for k in n.keys() - o.keys()),
            "removed": sorted(f"{k[0]}:{k[1]}" for k in o.keys() - n.keys()), "moved": sorted(moved, key=lambda x: x["pattern"])}

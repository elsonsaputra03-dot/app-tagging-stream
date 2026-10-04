"""Kualitas tagging terhadap label generator: precision/recall keseluruhan dan per jenis host, serta deteksi lookalike."""
from __future__ import annotations

from collections import Counter, defaultdict

from .match import Matcher


def evaluate(m: Matcher, evs) -> dict:
    by_kind = defaultdict(Counter); methods = Counter(); tp = fp = fn = 0
    for e in evs:
        t = m.tag(e["host"]); truth = e["truth_app"]; methods[t.method] += 1
        k = by_kind[e["truth_kind"]]; k["n"] += 1
        if t.app is not None and t.app == truth:
            tp += 1; k["correct"] += 1
        elif t.app is not None:
            fp += 1; k["wrong_app"] += 1
        else:
            k["no_app"] += 1
            if truth is not None:
                fn += 1
        if e["truth_kind"] == "lookalike" and t.method == "lookalike":
            k["flagged_lookalike"] += 1
    prec = tp / (tp + fp) if tp + fp else 0.0; rec = tp / (tp + fn) if tp + fn else 0.0
    return {"precision": round(prec, 4), "recall": round(rec, 4), "methods": dict(methods),
            "by_kind": {k: dict(v) for k, v in sorted(by_kind.items())}}

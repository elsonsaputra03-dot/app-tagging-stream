"""Snapshot metrik dari stack yang sedang berjalan (Kafka + ClickHouse) ke satu file JSON untuk halaman portofolio.

Kafka: partisi topic, offset awal/akhir, offset yang di-commit consumer group, dan lag per partisi.
ClickHouse: throughput per menit, latensi dari event dikirim sampai tersimpan, trafik per kategori/aplikasi, metode tagging,
antrean tinjauan, akurasi terhadap label sintetis, riwayat kamus, serta ukuran & kompresi tabel dari system.parts.
Tidak ada ID pelanggan di file ini; event-nya sintetis.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition

from .stream import TOPIC


def kafka(bootstrap: str, group: str = "apptag-tagger", topic: str = TOPIC, extra: dict | None = None) -> dict:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False, **(extra or {})})
    try:
        md = c.list_topics(topic, timeout=10)
        parts = sorted(md.topics[topic].partitions) if topic in md.topics else []
        committed = {tp.partition: tp.offset for tp in c.committed([TopicPartition(topic, p) for p in parts], timeout=10)}
        rows = []
        for p in parts:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            co = committed.get(p, -1)
            rows.append({"partition": p, "low": lo, "high": hi, "committed": co if co >= 0 else None,
                         "lag": (hi - co) if co >= 0 else hi - lo})
        return {"topic": topic, "group": group, "brokers": len(md.brokers), "partitions": rows,
                "messages": sum(r["high"] - r["low"] for r in rows), "lag": sum(r["lag"] for r in rows)}
    finally:
        c.close()


def clickhouse(client, window_min: int = 30) -> dict:
    q = lambda sql, **p: client.query(sql, parameters=p).result_rows
    w = {"w": window_min}
    total, uniq, first, last = q("SELECT count(), uniqExact(event_id), min(ts), max(ts) FROM tagged_events")[0]
    per_min = q("""SELECT toStartOfMinute(ts) m, count(), sum(bytes) FROM tagged_events
                   WHERE ts >= (SELECT max(ts) FROM tagged_events) - INTERVAL {w:UInt32} MINUTE GROUP BY m ORDER BY m""", **w)
    lat = q("""SELECT quantiles(0.5, 0.95, 0.99)(dateDiff('millisecond', ts, ingested_at)) FROM tagged_events
               WHERE ts >= (SELECT max(ts) FROM tagged_events) - INTERVAL {w:UInt32} MINUTE""", **w)[0][0]
    acc = q("""SELECT countIf(app != '' AND app = ifNull(truth_app, '')) / greatest(countIf(app != ''), 1),
                      countIf(app != '' AND app = truth_app) / greatest(countIf(truth_app IS NOT NULL), 1) FROM tagged_events""")[0]
    parts = q("""SELECT table, sum(rows), sum(data_compressed_bytes), sum(data_uncompressed_bytes), count()
                 FROM system.parts WHERE database = currentDatabase() AND active GROUP BY table ORDER BY sum(data_uncompressed_bytes) DESC""")
    return {
        "version": q("SELECT version()")[0][0],
        "events": {"total": total, "unique": uniq, "duplicates": total - uniq, "first": str(first), "last": str(last)},
        "per_minute": [{"minute": str(m), "events": n, "bytes": b} for m, n, b in per_min],
        "latency_ms": {"p50": lat[0], "p95": lat[1], "p99": lat[2]},
        "accuracy": {"precision": round(acc[0], 4), "recall": round(acc[1], 4)},
        "methods": [{"method": m, "events": n} for m, n in q("SELECT method, sum(events) n FROM traffic_minute GROUP BY method ORDER BY n DESC")],
        "categories": [{"category": c_, "events": n, "bytes": b} for c_, n, b in
                       q("SELECT category, sum(events), sum(bytes) FROM traffic_minute WHERE category != '' GROUP BY category ORDER BY sum(bytes) DESC")],
        "top_apps": [{"app": a, "category": c_, "events": n, "bytes": b} for a, c_, n, b in
                     q("SELECT app, any(category), sum(events), sum(bytes) FROM traffic_minute WHERE app != '' GROUP BY app ORDER BY sum(bytes) DESC LIMIT 12")],
        "review_queue": [{"method": m, "host": h, "hits": n, "bytes": b, "hint": hint} for m, h, n, b, hint in
                         q("SELECT method, host, sum(hits), sum(bytes), any(hint) FROM review_queue GROUP BY method, host ORDER BY sum(bytes) DESC LIMIT 12")],
        "review_queue_hosts": q("SELECT uniqExact(host) FROM review_queue")[0][0],
        "dictionary": [{"built_at": str(t), "version": v, "upstream": u, "rules": r, "added": a, "removed": rm, "moved": mv}
                       for t, v, u, r, a, rm, mv in q("SELECT built_at, version, upstream, rules, added, removed, moved FROM dict_versions ORDER BY built_at DESC LIMIT 5")],
        "storage": [{"table": t, "rows": r, "compressed": cb, "uncompressed": ub, "parts": n} for t, r, cb, ub, n in parts],
    }


def build(bootstrap: str, client, window_min: int = 30, extra: dict | None = None) -> dict:
    return {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "window_minutes": window_min,
            "note": "Snapshot of a local docker compose run. Events are synthetic; no subscriber data.",
            "kafka": kafka(bootstrap, extra=extra), "clickhouse": clickhouse(client, window_min)}


def write(doc: dict, path) -> None:
    from pathlib import Path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(doc, indent=1, default=str), encoding="utf-8")

"""Kafka: producer event sintetis dan consumer tagging -> ClickHouse.

Semantik: at-least-once. Consumer meng-commit offset hanya setelah batch berhasil masuk ClickHouse; bila gagal, offset
tidak di-commit dan batch dibaca ulang. Akibatnya event bisa tercatat dua kali setelah crash di antara insert dan commit;
event_id disimpan agar duplikat bisa dideteksi. Commit hanya dilakukan bila batch berisi pesan: commit tanpa offset yang
tersimpan gagal dengan _NO_OFFSET (ditemukan saat uji dengan mock cluster librdkafka).
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, Producer

from . import generate, store
from .dictionary import Dictionary
from .match import Matcher

log = logging.getLogger("apptag.stream")
TOPIC = "dns_events"


def produce(bootstrap: str, d: Dictionary, total: int, rate: float = 0.0, seed: int = 7, topic: str = TOPIC, extra: dict | None = None) -> int:
    """Kirim event berlabel; key = subscriber, sehingga event satu pengguna masuk partisi yang sama (urutan per pengguna terjaga)."""
    p = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "enable.idempotence": True, **(extra or {})})
    sent, t0 = 0, time.monotonic()
    for e in generate.events(d, total, seed=seed, start=datetime.now().astimezone()):
        e["event_id"] = uuid.uuid4().hex
        p.produce(topic, key=e["subscriber"], value=json.dumps(e).encode())
        sent += 1
        if sent % 1000 == 0:
            p.poll(0)
        if rate:
            ahead = sent / rate - (time.monotonic() - t0)
            if ahead > 0:
                time.sleep(ahead)
    p.flush(30)
    return sent


class Tagger:
    """Consumer + matcher yang memuat ulang kamus dari ClickHouse ketika ada versi baru (tanpa restart)."""

    def __init__(self, client, reload_every: float = 30.0, fuzzy: bool = True):
        self.client, self.reload_every, self.fuzzy = client, reload_every, fuzzy
        self.matcher: Matcher | None = None
        self.checked = 0.0
        self.reloads = 0

    def maybe_reload(self, force: bool = False) -> None:
        if not force and time.monotonic() - self.checked < self.reload_every:
            return
        self.checked = time.monotonic()
        v = store.latest_version(self.client)
        if v is None:
            raise RuntimeError("no dictionary version in ClickHouse yet; run `apptag sync` first")
        if self.matcher is None or self.matcher.version != v:
            self.matcher = Matcher(store.load_dictionary(self.client, v), fuzzy=self.fuzzy)
            self.reloads += 1
            log.info("dictionary loaded: %s", v)

    def rows(self, events: list[dict]) -> list[list]:
        m, out = self.matcher, []
        for e in events:
            t = m.tag(e["host"])
            out.append([e["event_id"], datetime.fromisoformat(e["ts"]), e["subscriber"], e["region"], e["host"], int(e["bytes"]),
                        e["proto"], t.app or "", t.category or "", t.method, t.confidence, t.rule or "", m.version,
                        e.get("truth_app"), e.get("truth_kind", "")])
        return out


def consume(bootstrap: str, client, group: str = "apptag-tagger", topic: str = TOPIC, batch: int = 1000, max_events: int | None = None,
            idle_stop: float | None = None, reload_every: float = 30.0, extra: dict | None = None) -> dict:
    tg = Tagger(client, reload_every)
    tg.maybe_reload(force=True)
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "auto.offset.reset": "earliest", "enable.auto.commit": False,
                  **(extra or {})})
    state = {"assigned_at": None}

    def on_assign(consumer, partitions):
        state["assigned_at"] = time.monotonic()

    # waktu idle dihitung sejak partisi diterima: rebalance consumer group bisa makan beberapa detik, dan versi awal
    # berhenti sebelum membaca satu pesan pun (ditemukan oleh test integrasi)
    c.subscribe([topic], on_assign=on_assign)
    done, batches, last = 0, 0, time.monotonic()
    try:
        while max_events is None or done < max_events:
            msgs = c.consume(batch, timeout=1.0)
            good = []
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("kafka error: %s", m.error())
                    continue
                good.append(json.loads(m.value()))
            tg.maybe_reload()
            if not good:
                started = state["assigned_at"]
                if idle_stop and started is not None and time.monotonic() - max(last, started) > idle_stop:
                    break
                continue
            store.insert_events(client, tg.rows(good))       # gagal -> pengecualian -> tidak di-commit -> dibaca ulang
            c.commit(asynchronous=False)
            done += len(good); batches += 1; last = time.monotonic()
    finally:
        c.close()
    return {"events": done, "batches": batches, "dictionary_reloads": tg.reloads, "version": tg.matcher.version if tg.matcher else None}

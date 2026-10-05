"""Integrasi: Kafka (mock cluster librdkafka, di dalam proses) -> consumer tagging -> ClickHouse (server sungguhan).

Butuh ClickHouse di CH_HOST:CH_PORT (CI memakai service container; lokal: `docker compose up clickhouse`). Dilewati bila tidak ada.
"""
import shutil
import uuid
from pathlib import Path

import pytest

from apptag import dictionary as D, store, stream, sync

HERE = Path(__file__).parent
ROOT = HERE.parent
TAX, LOCAL = ROOT / "dictionary/taxonomy.yaml", ROOT / "dictionary/local_id.yaml"
MOCK = {"test.mock.num.brokers": 3}


def _ch_available() -> bool:
    try:
        store.connect(store.CHConfig(db="default")).command("SELECT 1"); return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _ch_available(), reason="ClickHouse not reachable")


@pytest.fixture()
def env(tmp_path):
    from confluent_kafka import Producer
    holder = Producer(MOCK)                               # menghidupkan mock cluster selama fixture aktif
    bootstrap = ",".join(f"{b.host}:{b.port}" for b in holder.list_topics(timeout=5).brokers.values())
    cfg = store.CHConfig(db=f"apptag_test_{uuid.uuid4().hex[:8]}")
    client = store.connect(cfg)
    yield {"bootstrap": bootstrap, "cfg": cfg, "client": client, "tmp": tmp_path, "holder": holder}
    client.command(f"DROP DATABASE IF EXISTS {cfg.db} SYNC")


def _q(client, sql):
    return client.query(sql).result_rows


def test_end_to_end_kafka_to_clickhouse(env):
    c, cfg = env["client"], env["cfg"]
    r = sync.run(cfg, TAX, LOCAL, HERE / "fixtures/v2fly/data", "fixture")
    assert r["changed"] and r["rules"] > 4000
    assert sync.run(cfg, TAX, LOCAL, HERE / "fixtures/v2fly/data", "fixture")["changed"] is False       # idempoten
    d = store.load_dictionary(c, r["version"])
    sent = stream.produce(env["bootstrap"], d, 5000, seed=5)
    out = stream.consume(env["bootstrap"], c, idle_stop=3)
    assert sent == out["events"] == _q(c, "SELECT count() FROM tagged_events")[0][0] == 5000
    assert _q(c, "SELECT sum(events) FROM traffic_minute")[0][0] == 5000                                  # MV per menit lengkap
    assert _q(c, "SELECT uniqExact(event_id) FROM tagged_events")[0][0] == 5000
    # antrean tinjauan: hanya unknown & lookalike, dengan petunjuk merek untuk lookalike
    q = dict(_q(c, "SELECT method, sum(hits) FROM review_queue GROUP BY method"))
    assert set(q) == {"unknown", "lookalike"}
    assert _q(c, "SELECT count() FROM review_queue WHERE method = 'lookalike' AND hint NOT LIKE 'brand:%'")[0][0] == 0
    # akurasi live dari label kebenaran event sintetis: tidak ada aplikasi yang salah
    wrong = _q(c, "SELECT count() FROM tagged_events WHERE app != '' AND app != ifNull(truth_app, '')")[0][0]
    assert wrong == 0
    # DICTIONARY ClickHouse bisa dipakai langsung di SQL
    assert _q(c, f"SELECT dictGet('{cfg.db}.host_rules', 'app', tuple('domain', 'googlevideo.com'))")[0][0] == "youtube"


def test_dictionary_hot_reload_without_restart(env):
    c, cfg, tmp = env["client"], env["cfg"], env["tmp"]
    v1 = sync.run(cfg, TAX, LOCAL, HERE / "fixtures/v2fly/data", "fixture")["version"]
    tg = stream.Tagger(c, reload_every=0)
    tg.maybe_reload(force=True)
    assert tg.matcher.tag("cdn.vidio-static.org").method == "fuzzy"
    # upstream berubah: daftar lokal mendapat domain baru untuk vidio
    local2 = tmp / "local2.yaml"
    local2.write_text(LOCAL.read_text().replace("domains: [vidio.com]", "domains: [vidio.com, vidio-static.org]"))
    r2 = sync.run(cfg, TAX, local2, HERE / "fixtures/v2fly/data", "fixture+1")
    assert r2["changed"] and r2["added"] == 1 and r2["previous"] == v1
    tg.maybe_reload()                                                       # consumer yang sama, tanpa restart
    t = tg.matcher.tag("cdn.vidio-static.org")
    assert (t.app, t.method, tg.matcher.version, tg.reloads) == ("vidio", "suffix", r2["version"], 2)
    diff = _q(c, f"SELECT added, removed, moved FROM dict_versions WHERE version = '{r2['version']}'")[0]
    assert diff == (1, 0, 0)


def test_at_least_once_no_loss_when_clickhouse_insert_fails(env, monkeypatch):
    c, cfg = env["client"], env["cfg"]
    v = sync.run(cfg, TAX, LOCAL, HERE / "fixtures/v2fly/data", "fixture")["version"]
    d = store.load_dictionary(c, v)
    stream.produce(env["bootstrap"], d, 3000, seed=9)
    real, calls = store.insert_events, {"n": 0}

    def flaky(client, rows):
        calls["n"] += 1
        if calls["n"] == 2:                                                 # batch kedua gagal (mis. ClickHouse restart)
            raise ConnectionError("simulated ClickHouse outage")
        return real(client, rows)

    monkeypatch.setattr(store, "insert_events", flaky)
    with pytest.raises(ConnectionError):
        stream.consume(env["bootstrap"], c, batch=500, idle_stop=3)
    monkeypatch.setattr(store, "insert_events", real)
    stream.consume(env["bootstrap"], c, batch=500, idle_stop=3)               # "restart": lanjut dari offset terakhir yang di-commit
    total, uniq = _q(c, "SELECT count(), uniqExact(event_id) FROM tagged_events")[0]
    assert uniq == 3000                                                       # tidak ada event yang hilang
    assert total >= uniq                                                      # duplikat mungkin (at-least-once) dan terdeteksi lewat event_id


def test_event_time_follows_the_wall_clock(env):
    """Regresi: waktu event dulu berjalan 11x lebih cepat dari jam dinding pada 300 event/detik."""
    import time
    from datetime import datetime, timezone
    c, cfg = env["client"], env["cfg"]
    v = sync.run(cfg, TAX, LOCAL, HERE / "fixtures/v2fly/data", "fixture")["version"]
    t0 = datetime.now(timezone.utc)
    stream.produce(env["bootstrap"], store.load_dictionary(c, v), 600, rate=300)
    t1 = datetime.now(timezone.utc)
    stream.consume(env["bootstrap"], c, idle_stop=3)
    lo, hi, lag_p95 = _q(c, "SELECT min(ts), max(ts), quantile(0.95)(dateDiff('millisecond', ts, ingested_at)) FROM tagged_events")[0]
    assert t0.replace(tzinfo=None) <= lo.replace(tzinfo=None) and hi.replace(tzinfo=None) <= t1.replace(tzinfo=None)   # dalam jendela kirim
    assert lag_p95 >= 0                                                       # tidak ada event "dari masa depan"

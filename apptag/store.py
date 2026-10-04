"""Lapisan ClickHouse: skema, insert, dan kamus ber-versi.

Tabel:
  tagged_events      setiap event + hasil tagging + versi kamus yang dipakai (TTL 30 hari)
  traffic_minute     agregat per menit per aplikasi/kategori/metode (materialized view, SummingMergeTree)
  review_queue       host 'unknown' dan 'lookalike' untuk ditinjau: jumlah hit, byte, pertama/terakhir terlihat (MV)
  dict_versions      riwayat versi kamus beserta ringkasan diff dari versi sebelumnya
  dict_rules         aturan per versi; view dict_rules_current = versi terbaru
  host_rules         DICTIONARY ClickHouse dari dict_rules_current (full/domain), untuk lookup di SQL; di-reload setelah sync
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass

import clickhouse_connect

from .dictionary import Dictionary, Rule


@dataclass
class CHConfig:
    host: str = os.getenv("CH_HOST", "localhost")
    port: int = int(os.getenv("CH_PORT", "8123"))
    user: str = os.getenv("CH_USER", "default")
    password: str = os.getenv("CH_PASSWORD", "")
    db: str = os.getenv("CH_DB", "apptag")


def connect(cfg: CHConfig):
    admin = clickhouse_connect.get_client(host=cfg.host, port=cfg.port, username=cfg.user, password=cfg.password)
    admin.command(f"CREATE DATABASE IF NOT EXISTS {cfg.db}")
    return clickhouse_connect.get_client(host=cfg.host, port=cfg.port, username=cfg.user, password=cfg.password, database=cfg.db)


def ddl(cfg: CHConfig) -> list[str]:
    db = cfg.db
    return [
        f"""CREATE TABLE IF NOT EXISTS {db}.tagged_events (
              event_id String, ts DateTime64(3, 'UTC'), subscriber String, region LowCardinality(String), host String,
              bytes UInt64, proto LowCardinality(String), app LowCardinality(String), category LowCardinality(String),
              method LowCardinality(String), confidence Float32, rule String, dict_version LowCardinality(String),
              truth_app Nullable(String), truth_kind LowCardinality(String), ingested_at DateTime64(3, 'UTC') DEFAULT now64(3))
            ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY (ts, event_id) TTL toDateTime(ts) + INTERVAL 30 DAY""",
        f"""CREATE TABLE IF NOT EXISTS {db}.traffic_minute (minute DateTime('UTC'), category LowCardinality(String),
              app LowCardinality(String), method LowCardinality(String), events UInt64, bytes UInt64)
            ENGINE = SummingMergeTree ORDER BY (minute, category, app, method)""",
        f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.traffic_minute_mv TO {db}.traffic_minute AS
            SELECT toStartOfMinute(ts) AS minute, category, app, method, count() AS events, sum(bytes) AS bytes
            FROM {db}.tagged_events GROUP BY minute, category, app, method""",
        f"""CREATE TABLE IF NOT EXISTS {db}.review_queue (host String, method LowCardinality(String),
              hits SimpleAggregateFunction(sum, UInt64), bytes SimpleAggregateFunction(sum, UInt64),
              first_seen SimpleAggregateFunction(min, DateTime64(3, 'UTC')), last_seen SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
              hint SimpleAggregateFunction(any, String))
            ENGINE = AggregatingMergeTree ORDER BY (method, host)""",
        f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.review_queue_mv TO {db}.review_queue AS
            SELECT host, method, count() AS hits, sum(bytes) AS bytes, min(ts) AS first_seen, max(ts) AS last_seen, any(rule) AS hint
            FROM {db}.tagged_events WHERE method IN ('unknown', 'lookalike') GROUP BY host, method""",
        f"""CREATE TABLE IF NOT EXISTS {db}.dict_versions (version String, upstream String, built_at DateTime64(3, 'UTC') DEFAULT now64(3),
              rules UInt32, conflicts UInt32, added UInt32, removed UInt32, moved UInt32, diff String)
            ENGINE = MergeTree ORDER BY built_at""",
        f"""CREATE TABLE IF NOT EXISTS {db}.dict_rules (version String, kind LowCardinality(String), pattern String,
              app LowCardinality(String), category LowCardinality(String), source LowCardinality(String))
            ENGINE = MergeTree ORDER BY (version, kind, pattern)""",
        f"""CREATE VIEW IF NOT EXISTS {db}.dict_rules_current AS
            SELECT kind, pattern, app, category FROM {db}.dict_rules
            WHERE version = (SELECT argMax(version, built_at) FROM {db}.dict_versions) AND kind IN ('full', 'domain')""",
        f"""CREATE DICTIONARY IF NOT EXISTS {db}.host_rules (kind String, pattern String, app String, category String)
            PRIMARY KEY kind, pattern
            SOURCE(CLICKHOUSE(QUERY 'SELECT kind, pattern, app, category FROM {db}.dict_rules_current'
                              USER '{cfg.user}' PASSWORD '{cfg.password}'))
            LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0)""",
    ]


def migrate(client, cfg: CHConfig) -> None:
    for q in ddl(cfg):
        client.command(q)


EVENT_COLS = ["event_id", "ts", "subscriber", "region", "host", "bytes", "proto", "app", "category", "method", "confidence",
              "rule", "dict_version", "truth_app", "truth_kind"]


def insert_events(client, rows: list[list]) -> None:
    if rows:
        client.insert("tagged_events", rows, column_names=EVENT_COLS)


def latest_version(client) -> str | None:
    r = client.query("SELECT argMax(version, built_at), count() FROM dict_versions").result_rows[0]
    return r[0] if r[1] else None


def load_dictionary(client, version: str) -> Dictionary:
    rows = client.query("SELECT kind, pattern, app, category, source FROM dict_rules WHERE version = {v:String}",
                        parameters={"v": version}).result_rows
    up = client.query("SELECT any(upstream) FROM dict_versions WHERE version = {v:String}", parameters={"v": version}).result_rows[0][0]
    return Dictionary([Rule(*r) for r in rows], version, [], up)


def save_dictionary(client, cfg: CHConfig, d: Dictionary, df: dict) -> None:
    """Simpan versi baru: aturan dulu, lalu baris versi (yang membuat versi itu 'terbaru'), lalu reload DICTIONARY ClickHouse."""
    client.insert("dict_rules", [[d.version, r.kind, r.pattern, r.app, r.category, r.source] for r in d.rules],
                  column_names=["version", "kind", "pattern", "app", "category", "source"])
    client.insert("dict_versions", [[d.version, d.upstream, len(d.rules), len(d.conflicts), len(df["added"]), len(df["removed"]),
                                     len(df["moved"]), json.dumps(df)]],
                  column_names=["version", "upstream", "rules", "conflicts", "added", "removed", "moved", "diff"])
    client.command(f"SYSTEM RELOAD DICTIONARY {cfg.db}.host_rules")

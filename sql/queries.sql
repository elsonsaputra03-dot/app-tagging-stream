-- Query contoh (jalankan di http://localhost:8123/play, user apptag / apptag)

-- Trafik per kategori, 15 menit terakhir
SELECT category, sum(events) AS n_events, formatReadableSize(sum(bytes)) AS volume
FROM apptag.traffic_minute WHERE minute >= now() - INTERVAL 15 MINUTE AND category != ''
GROUP BY category ORDER BY sum(bytes) DESC;

-- Porsi trafik per metode tagging (berapa yang butuh fuzzy, berapa yang tidak dikenal)
SELECT method, sum(events) AS n_events, round(100 * sum(events) / (SELECT sum(events) FROM apptag.traffic_minute), 2) AS pct
FROM apptag.traffic_minute GROUP BY method ORDER BY n_events DESC;

-- Antrean tinjauan: host tidak dikenal dengan trafik terbesar, dan domain tiruan beserta merek yang ditiru
SELECT method, host, sum(hits) AS n_hits, formatReadableSize(sum(bytes)) AS volume, any(hint) AS hint, max(last_seen) AS last_seen
FROM apptag.review_queue GROUP BY method, host ORDER BY sum(bytes) DESC LIMIT 20;

-- Akurasi live dari label kebenaran yang dibawa event sintetis (hanya ada karena datanya sintetis)
SELECT countIf(app != '' AND app = ifNull(truth_app, '')) / countIf(app != '') AS precision,
       countIf(app != '' AND app = truth_app) / countIf(truth_app IS NOT NULL) AS recall
FROM apptag.tagged_events WHERE ts >= now() - INTERVAL 1 HOUR;

-- Riwayat versi kamus dan perubahannya
SELECT built_at, version, upstream, rules, added, removed, moved FROM apptag.dict_versions ORDER BY built_at DESC;

-- Lookup langsung di SQL lewat DICTIONARY ClickHouse
SELECT dictGet('apptag.host_rules', 'app', tuple('domain', 'googlevideo.com')) AS app;

-- Duplikat akibat at-least-once (event_id sama tercatat lebih dari sekali)
SELECT count() - uniqExact(event_id) AS duplicate_rows FROM apptag.tagged_events;

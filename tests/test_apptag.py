"""Test inti: parser v2fly, resolusi daftar payung, pencocokan, fuzzy & lookalike, diff kamus, dan evaluasi berlabel."""
from pathlib import Path

import pytest

from apptag import dictionary as D, evaluate as E, generate as G, match as M

HERE = Path(__file__).parent
ROOT = HERE.parent


@pytest.fixture(scope="module")
def d():
    return D.build(HERE / "fixtures/v2fly/data", ROOT / "dictionary/taxonomy.yaml", ROOT / "dictionary/local_id.yaml", "fixture")


@pytest.fixture(scope="module")
def m(d):
    return M.Matcher(d)


def test_parse_v2fly_formats():
    p = D.parse_list("# komentar\nexample.com\nfull:www.example.org @ads\nregexp:^r\\d+\\.example\\.net$\nkeyword:exmpl\ninclude:other:@cn\n\nCDN.Example.com.\n")
    assert p["domain"] == {"example.com", "cdn.example.com"} and p["full"] == {"www.example.org"}
    assert p["regexp"] == ["^r\\d+\\.example\\.net$"] and p["keyword"] == ["exmpl"] and p["include"] == ["other"]


def test_specific_list_beats_umbrella(d):
    owner = {r.pattern: r.app for r in d.rules if r.kind == "domain"}
    assert owner["youtube.com"] == "youtube" and owner["googlevideo.com"] == "youtube"     # 'google' meng-include youtube
    assert owner["tokopedia.com"] == "tokopedia" and next(r for r in d.rules if r.pattern == "tokopedia.com").source == "local"


@pytest.mark.parametrize("host,app,method", [
    ("www.youtube.com", "youtube", "suffix"),
    ("r5---sn-a5mekn7z.googlevideo.com", "youtube", "suffix"),
    ("WWW.YouTube.com.", "youtube", "suffix"),                 # huruf besar dan titik di akhir
    ("api.tokopedia.com:443", "tokopedia", "suffix"),          # port
    ("api.youtube", "youtube", "suffix"),                      # TLD milik merek (dulu terlewat)
    ("images.tokopedia-static.net", "tokopedia", "fuzzy"),
    ("tokopediastatic.net", "tokopedia", "fuzzy"),
    ("mobilelegends-asia.net", "mobilelegends", "fuzzy"),
    ("grabbag.com", None, "unknown"),                          # 'grab' terlalu pendek untuk fuzzy
    ("appletree.com", None, "unknown"),                        # merek + kata biasa bukan aplikasinya
    ("netflix-news.com", None, "unknown"),                     # situs pihak ketiga
    ("netflix-account-verify.xyz", None, "lookalike"),         # pola phishing
    ("", None, "unknown"),
])
def test_tagging(m, host, app, method):
    t = m.tag(host)
    assert (t.app, t.method) == (app, method)


def test_fuzzy_can_be_disabled(d):
    assert M.Matcher(d, fuzzy=False).tag("images.tokopedia-static.net").method == "unknown"


def test_dictionary_diff_added_removed_moved(tmp_path):
    def build(files):
        data = tmp_path / str(len(list(tmp_path.iterdir()))) / "data"; data.mkdir(parents=True)
        for n, txt in files.items():
            (data / n).write_text(txt)
        tax = data.parent / "tax.yaml"; tax.write_text("v2fly:\n  alpha: video\n  beta: social\n")
        loc = data.parent / "loc.yaml"; loc.write_text("apps: {}\n")
        return D.build(data, tax, loc)
    v1 = build({"alpha": "alpha.com\nshared.net\n", "beta": "beta.com\n"})
    v2 = build({"alpha": "alpha.com\nalpha-cdn.net\n", "beta": "beta.com\nshared.net\n"})
    df = D.diff(v1, v2)
    assert df["from"] == v1.version and df["to"] == v2.version and v1.version != v2.version
    assert df["added"] == ["domain:alpha-cdn.net"] and df["removed"] == []
    assert df["moved"] == [{"kind": "domain", "pattern": "shared.net", "from": "alpha/video", "to": "beta/social"}]
    assert D.diff(v2, v2) == {"from": v2.version, "to": v2.version, "added": [], "removed": [], "moved": []}


def test_dictionary_json_roundtrip(d):
    back = D.Dictionary.from_json(d.to_json())
    assert back.version == d.version and len(back.rules) == len(d.rules)


def test_evaluation_on_labelled_events(d, m):
    r = E.evaluate(m, G.events(d, 20_000, seed=3))
    k = r["by_kind"]
    assert r["precision"] >= 0.999                                         # konservatif: hampir tidak pernah salah aplikasi
    assert k["unknown_hard"].get("wrong_app", 0) == 0 and k["unknown"].get("wrong_app", 0) == 0
    assert k["lookalike"]["flagged_lookalike"] == k["lookalike"]["n"]
    assert k["subdomain"]["correct"] == k["subdomain"]["n"] and k["exact"]["correct"] == k["exact"]["n"]
    assert k["variant"]["correct"] == k["variant"]["n"]
    assert k["variant_hard"].get("correct", 0) < 0.2 * k["variant_hard"]["n"]     # batasan yang disengaja & terdokumentasi
    r0 = E.evaluate(M.Matcher(d, fuzzy=False), G.events(d, 20_000, seed=3))
    assert r["recall"] > r0["recall"] + 0.03                                 # fuzzy menaikkan recall


def test_dictionary_folder_found_outside_the_repo(tmp_path, monkeypatch):
    """Seperti di container: kode terpasang di tempat lain dan proses berjalan dari folder lain."""
    from apptag import cli
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("APPTAG_DICT_DIR", str(ROOT / "dictionary"))
    assert cli.dict_dir() == ROOT / "dictionary"
    monkeypatch.setenv("APPTAG_DICT_DIR", str(tmp_path / "nowhere"))
    (tmp_path / "dictionary").mkdir(); (tmp_path / "dictionary/taxonomy.yaml").write_text("v2fly: {}\n")
    assert cli.dict_dir() == tmp_path / "dictionary"                    # ./dictionary dipakai bila env tidak valid

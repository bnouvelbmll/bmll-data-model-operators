"""The Lab observer: which files count, what the summary says, what gets written. No storage is touched."""

import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from dm_operators.observers import lab  # noqa: E402

LOCATION = "s3://{L2_ACCESS_POINT_ALIAS}/Equity-L1/{mic}/{yyyy}/{mm}/{dd}/l1-{mic}-{yyyy}{mm}{dd}.parquet"


def key(mic, y, m, d):
    return f"Equity-L1/{mic}/{y}/{m:02}/{d:02}/l1-{mic}-{y}{m:02}{d:02}.parquet"


def test_a_location_template_gives_the_folder_to_list_and_a_regex_for_its_files():
    bucket, prefix, regex = lab.parse_location(LOCATION)
    assert bucket == "{L2_ACCESS_POINT_ALIAS}" and prefix == "Equity-L1/"
    m = regex.match(key("XLON", 2026, 9, 8))
    assert m["mic"] == "XLON" and (m["y"], m["m"], m["d"]) == ("2026", "09", "08")
    assert not regex.match("Equity-L1/XLON/2026/09/08/_manifest.json") and not regex.match("Equity-L1/XLON/2026/09/08/l1-XNAS-20260908.parquet")
    with pytest.raises(ValueError):
        lab.parse_location("snowflake://x")


def test_a_summary_counts_files_mics_days_and_gaps_and_ignores_what_is_not_a_data_file():
    _, prefix, regex = lab.parse_location(LOCATION)
    objects = [(key("XLON", 2026, 9, 7), 100), (key("XLON", 2026, 9, 9), 300), (key("XNAS", 2026, 9, 7), 50), (key("XNAS", 2026, 9, 10), 50),
               ("Equity-L1/XLON/2026/09/07/_SUCCESS", 0), ("Equity-L1/readme.txt", 5), (key("XPAR", 2026, 13, 1) if False else "Equity-L1/XPAR/2026/13/01/l1-XPAR-20261301.parquet", 9)]
    s = lab.summarise(objects, regex, "s3://{L2_ACCESS_POINT_ALIAS}/Equity-L1/")
    assert (s["files"], s["total_bytes"], s["mics"], s["days"]) == (4, 500, 2, 3)
    assert (s["first_date"], s["last_date"], s["gap_days"]) == ("2026-09-07", "2026-09-10", 1)   # the 8th has no file anywhere
    assert s["by_mic"][0] == {"mic": "XLON", "files": 2, "first_date": "2026-09-07", "last_date": "2026-09-09", "days": 2}
    assert s["listing_truncated"] is False and s["by_mic_truncated"] is False


def test_the_per_mic_list_is_capped_and_a_limit_hit_is_said():
    _, prefix, regex = lab.parse_location(LOCATION)
    objects = [(key(f"M{i:03}", 2026, 9, 1), 1) for i in range(10)]
    s = lab.summarise(objects, regex, prefix, limit=10, max_by_mic=3)
    assert len(s["by_mic"]) == 3 and s["by_mic_truncated"] and s["listing_truncated"], "ten objects seen with a limit of ten: a lower bound"
    assert lab.summarise([], regex, prefix) == {"prefix": prefix, "files": 0, "total_bytes": 0, "mics": 0, "days": 0, "listing_truncated": False}


def test_the_access_point_alias_comes_from_the_environment_or_the_observation_fails_clearly():
    assert lab.resolve_bucket("{L2_ACCESS_POINT_ALIAS}", {"L2_ACCESS_POINT_ALIAS": "ap-1-alias"}) == "ap-1-alias"
    with pytest.raises(RuntimeError, match="L2_ACCESS_POINT_ALIAS is not set"):
        lab.resolve_bucket("{L2_ACCESS_POINT_ALIAS}", {})


@pytest.fixture
def model(tmp_path, monkeypatch):
    asset = tmp_path / "crd/assets/equity/asset-equity-l1.yaml"
    asset.parent.mkdir(parents=True)
    asset.write_text(yaml.safe_dump({"id": "equity.l1", "materialisations": [
        {"name": "snowflake", "destination": {"backend": "snowflake", "location": "DB.PUBLIC.L1"}},
        {"name": "parquet", "destination": {"backend": "parquet", "location": LOCATION}}]}))
    monkeypatch.setattr(lab, "STATUS_DIR", tmp_path / "crd/status")
    return tmp_path, asset


def test_only_parquet_materialisations_in_s3_are_observed_and_the_record_keeps_what_other_observers_wrote(model):
    root, asset = model
    found = list(lab.parquet_materialisations([asset]))
    assert [(m["subject"], m["name"]) for m in found] == [("equity.l1", "parquet")]
    existing = root / "crd/status/equity.l1.parquet.yaml"
    existing.parent.mkdir(parents=True)
    existing.write_text(yaml.safe_dump({"id": "x", "subject": "equity.l1", "schemaschemer_path": "keep/me", "error": "old"}))
    fake = lambda bucket, prefix, limit: iter([(key("XLON", 2026, 9, 7), 10)])
    lab_block = lab.observe(found[0], 100, lister=fake, env={"L2_ACCESS_POINT_ALIAS": "ap-alias"})
    path = lab.write_status(found[0], lab_block, None)
    doc = yaml.safe_load(path.read_text())
    assert doc["id"] == "status.equity.l1.parquet" and doc["lab"]["files"] == 1 and doc["checked_by"] == "observe-lab-datasets"
    assert doc["schemaschemer_path"] == "keep/me" and "error" not in doc, "an earlier error is cleared by a good observation; other fields stay"
    assert lab_block["prefix"] == "s3://{L2_ACCESS_POINT_ALIAS}/Equity-L1/", "the record never carries the resolved alias"


def test_an_unreachable_dataset_is_recorded_as_an_error_and_does_not_stop_the_others(model, monkeypatch, capsys):
    root, asset = model
    monkeypatch.chdir(root)
    monkeypatch.delenv("L2_ACCESS_POINT_ALIAS", raising=False)
    assert lab.main(["--assets", "crd/assets/**/*.yaml"]) == 0
    doc = yaml.safe_load((root / "crd/status/equity.l1.parquet.yaml").read_text())
    assert "L2_ACCESS_POINT_ALIAS is not set" in doc["error"] and "lab" not in doc

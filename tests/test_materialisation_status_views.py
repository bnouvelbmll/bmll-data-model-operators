"""Customer views (samples, trial windows) are checked against the product of the asset they are cut from."""

import importlib.util
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("update_materialisation_status", ROOT / "scripts/update_materialisation_status.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def asset(name: str) -> dict:
    return yaml.safe_load(next((ROOT / "crd/assets").rglob(f"{name}.yaml")).read_text())


def test_trial_view_inherits_the_product_of_its_parent():
    parent = asset("asset-equity-l1")
    assert parent["product_id"]
    assert mod._lineage_product_id(asset("asset-equity-l1-trial-amer-6mo")) == parent["product_id"]
    assert mod._lineage_product_id(asset("asset-equity-l1-sample")) == parent["product_id"]


def test_declared_product_wins_and_unrelated_assets_get_none():
    assert mod._lineage_product_id({"id": "x", "product_id": "product.mine"}) == "product.mine"
    assert mod._lineage_product_id({"id": "y"}) is None
    # a derived asset that changes the data (not just filters it) is not a view of the same dataset
    assert mod._lineage_product_id({"id": "z", "lineage": {"inputs": [{"asset_id": "equity.l1"}], "transform": {"properties": ["aggregate"]}}}) is None


def test_every_trial_and_sample_asset_resolves_to_a_product():
    unresolved = []
    for path in (ROOT / "crd/assets").rglob("*.yaml"):
        doc = yaml.safe_load(path.read_text())
        if isinstance(doc, dict) and ("-trial-" in doc.get("id", "") or doc.get("id", "").endswith("-sample")):
            if not mod._lineage_product_id(doc):
                unresolved.append(doc["id"])
    assert not unresolved, f"views whose parent has no product: {unresolved}"

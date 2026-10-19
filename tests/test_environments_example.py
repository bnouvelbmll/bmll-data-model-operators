"""The registry example only names known executors and capabilities, holds no secret, and every operator can run somewhere in it."""

import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
registry = yaml.safe_load((ROOT / "environments.example.yaml").read_text())["environments"]
schema = json.loads((ROOT / "contract/operator.schema.json").read_text())
manifests = [yaml.safe_load(p.read_text()) for folder in ("contract", "manifests") for p in sorted((ROOT / folder).glob("*/operator.yaml"))]


def test_known_executors_and_capabilities_and_no_secret():
    capability = re.compile(schema["definitions"]["capability"]["pattern"])
    for name, env in registry.items():
        assert env["executor"] in {"kubernetes", "ssh", "spcs-job", "process"}, name
        assert all(capability.match(c) for c in env["capabilities"]), name
    text = json.dumps(registry).lower()
    assert "password" not in text and "token" not in text.replace("credentials_secret", ""), "the registry names connections, never secrets"


def test_every_available_operator_fits_some_environment():
    for op in manifests:
        if op["status"] != "available":
            continue
        assert any(set(op.get("requires", [])) <= set(e["capabilities"]) and (not op.get("requires_any") or set(op["requires_any"]) & set(e["capabilities"]))
                   for e in registry.values()), f"{op['name']} fits no example environment"

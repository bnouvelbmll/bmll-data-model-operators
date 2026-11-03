"""Publish execution evidence to the data-model results service using its HTTP contract.

The service owns PostgreSQL and artifact storage; workers never receive database
credentials. Uploads are retried with stable run IDs and immutable contents.
Model-file outputs are retained as downloadable proposals and status observations,
without writing them back into the model's declared specification.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path, PurePosixPath

import yaml

STATUS_KINDS = {"MaterialisationStatus", "TestSuiteStatus", "DeliveryTimeStatus"}
STATUS_DIRECTORIES = {"status": "MaterialisationStatus", "test-status": "TestSuiteStatus", "delivery-status": "DeliveryTimeStatus"}


def configured():
    return bool(os.environ.get("DATAMODEL_RESULTS_URL"))


def call(method, path, payload=None, file=None):
    base = os.environ["DATAMODEL_RESULTS_URL"].rstrip("/")
    token = os.environ.get("DATAMODEL_RESULTS_WRITE_TOKEN", "")
    if not token:
        raise RuntimeError("DATAMODEL_RESULTS_WRITE_TOKEN is required")
    # Artifacts are bounded by the service and streamed by urllib from a binary file.
    for attempt in range(3):
        stream = open(file, "rb") if file else None
        try:
            data = stream if stream else json.dumps(payload, default=str).encode()
            headers = {"Authorization": "Bearer " + token,
                       "Content-Type": "application/octet-stream" if stream else "application/json"}
            if stream:
                headers["Content-Length"] = str(Path(file).stat().st_size)
            request = urllib.request.Request(base + path, data=data, headers=headers, method=method)
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code < 500 or attempt == 2:
                raise RuntimeError(f"Results service rejected {method} with HTTP {exc.code}") from None
        except (urllib.error.URLError, TimeoutError):
            if attempt == 2:
                raise RuntimeError("Results service is unreachable; result was not published") from None
        finally:
            if stream:
                stream.close()
        time.sleep(2 ** attempt)


def register(request):
    payload = dict(request)
    if os.environ.get("DAGSTER_RUN_ID"):
        payload["dagster_run_id"] = os.environ["DAGSTER_RUN_ID"]
    return call("POST", "/v1/runs", payload)


def observations(files, finished_at):
    """Translate recognized status CRDs; arbitrary model files remain proposals."""
    records = []
    for file in files:
        doc = yaml.safe_load(file.get("content") or "")
        parts = PurePosixPath(file["path"]).parts
        directory_kind = STATUS_DIRECTORIES.get(parts[1]) if len(parts) >= 3 and parts[0] == "crd" else None
        kind = doc.get("kind", directory_kind) if isinstance(doc, dict) else None
        if kind not in STATUS_KINDS:
            continue
        if not doc.get("id"):
            raise ValueError("A status observation requires its CRD id")
        records.append({"kind": kind, "id": doc["id"],
                        "subject": doc.get("subject") or doc.get("product_id") or doc.get("suite") or doc["id"],
                        "observed_at": str(doc.get("checked_at") or finished_at),
                        "error": doc.get("error") or None, "document": doc})
    return records


def publish(request, result, artifacts_dir):
    """Upload all files first, then atomically publish result and status objects."""
    root = Path(artifacts_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    body = result.to_json() if hasattr(result, "to_json") else json.dumps(result, default=str)
    value = json.loads(body)
    finished = value.get("finished_at") or dt.datetime.now(dt.timezone.utc).isoformat()
    manifest = list(value.get("artifacts", []))
    # Reserved delivery paths do not overlap ordinary generator output directories.
    delivery = root / "_result"
    delivery.mkdir(exist_ok=True)
    (delivery / "result.json").write_text(body)
    manifest.append("_result/result.json")
    for file in value.get("files", []):
        rel = PurePosixPath(file["path"])
        if rel.is_absolute() or ".." in rel.parts:
            raise ValueError("Invalid model-file output path")
        target = delivery / "proposed" / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(file["content"] or "")
        manifest.append(target.relative_to(root).as_posix())
    manifest = sorted(set(manifest))
    for relative in manifest:
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise ValueError("Artifact output is missing or escapes its directory")
        endpoint = f"/v1/runs/{request['id']}/artifacts/" + urllib.parse.quote(relative, safe="/")
        call("PUT", endpoint, file=path)
    completion = {k: value[k] for k in ("status", "exit_code", "message", "log", "output") if k in value}
    completion.update(finished_at=finished, artifacts=manifest, observations=observations(value.get("files", []), finished))
    call("POST", f"/v1/runs/{request['id']}/complete", completion)
    public = os.environ.get("DATAMODEL_RESULTS_PUBLIC_URL", "").rstrip("/")
    return f"{public}/results?run={request['id']}"

"""What the controller decides, with no cluster in sight: how a JobRun becomes a Job, and how a finished Job becomes a status."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from dm_operators.runner import JobError, check_capabilities, load_operator, validate_request
from dm_operators.runner.core import PACKAGE_ROOT

# The data model's operator contract (schemas and the generators it declares), as of the revision this image was built with.
# The Job itself runs against the model's own operators/ at the pinned sha; this copy is only for refusing early.
CONTRACT = Path(os.environ.get("CONTRACT_DIR") or PACKAGE_ROOT / "contract")

GROUP, VERSION, PLURAL = "datamodel.bmll.io", "v1alpha1", "jobruns"
LABEL = "datamodel.bmll.io/jobrun"
_RESULT_BYTES = 900 * 1024  # a ConfigMap holds 1 MiB


def job_name(run_name: str) -> str:
    return f"jobrun-{run_name}"[:63].rstrip("-")


def full_request(name: str, spec: dict, created: str | None) -> dict:
    """The runner's request, completed from what a person writes in a JobRun (`id`, `requested_*` are filled in)."""
    request = {
        "id": name, "requested_by": "kubectl", "requested_at": created or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "deliver": {"mode": "report"}, **spec,
    }
    validate_request(request, CONTRACT)
    return request


def environment_for(request: dict, registry: dict) -> dict:
    envs = (registry or {}).get("environments", {})
    env = envs.get(request["environment"])
    if env is None:
        raise JobError(f"There is no environment {request['environment']!r}; this cluster offers: {', '.join(sorted(envs)) or 'none'}.")
    if env.get("executor") != "kubernetes":
        raise JobError(f"Environment {request['environment']!r} is run by the {env.get('executor')!r} executor, which this controller does not provide; it only runs kubernetes environments.")
    return env


def plan(name: str, spec: dict, created: str | None, registry: dict) -> tuple[dict, dict, dict]:
    """Everything that can be refused before a Job exists: returns (request, operator, environment) or raises JobError."""
    request = full_request(name, spec, created)
    env = environment_for(request, registry)
    operator = load_operator(request["operator"], CONTRACT)
    check_capabilities(operator, env.get("capabilities", []))
    return request, operator, env


def build_job(request: dict, operator: dict, env: dict, *, namespace: str, default_image: str, default_repo_url: str, ttl_seconds: int = 3600) -> dict:
    """A Job that checks the model out at the pinned sha and runs the runner on it. The request carries no secrets; credentials arrive from the environment's Secret."""
    name = job_name(request["id"])
    image = env.get("image") or default_image
    repo_url = env.get("model_repo_url") or default_repo_url
    timeout = request.get("timeout_seconds") or operator.get("timeout_seconds", 600)
    labels = {LABEL: request["id"], "app.kubernetes.io/managed-by": "datamodel-operators"}
    mounts = [{"name": "model", "mountPath": "/model"}, {"name": "work", "mountPath": "/work"}]
    secret = env.get("credentials_secret")
    security = {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}, "runAsNonRoot": True}
    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "namespace": namespace, "labels": labels},
        "spec": {
            "backoffLimit": 0, "activeDeadlineSeconds": int(timeout) + 120, "ttlSecondsAfterFinished": ttl_seconds,
            "template": {"metadata": {"labels": labels}, "spec": {
                "restartPolicy": "Never", "automountServiceAccountToken": False,
                "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000, "runAsNonRoot": True},
                "initContainers": [{
                    "name": "checkout", "image": image, "imagePullPolicy": "IfNotPresent", "securityContext": security, "volumeMounts": mounts,
                    "env": [{"name": "HOME", "value": "/work"}, {"name": "MODEL_REPO_URL", "value": repo_url}, {"name": "SHA", "value": request["model"]["sha"]}],
                    # the sha may be a branch tip or a snapshot only reachable by name: ask for it explicitly when the clone lacks it
                    "command": ["sh", "-ec", 'git clone --quiet --no-checkout "$MODEL_REPO_URL" /model/repo; cd /model/repo; '
                                             'git cat-file -e "$SHA^{commit}" 2>/dev/null || git fetch --quiet origin "$SHA"; git checkout --quiet --detach "$SHA"'],
                }],
                "containers": [{
                    "name": "run", "image": image, "imagePullPolicy": "IfNotPresent", "securityContext": security, "volumeMounts": mounts, "workingDir": "/app",
                    "env": [{"name": "HOME", "value": "/work"}, {"name": "JOBRUN_REQUEST", "value": json.dumps(request)},
                            {"name": "CAPABILITIES", "value": ",".join(env.get("capabilities", []))}, {"name": "TMPDIR", "value": "/work"}],
                    **({"envFrom": [{"secretRef": {"name": secret, "optional": True}}]} if secret else {}),
                    # the runner prints one JSON result on stdout, which the controller reads from the pod's log
                    "command": ["sh", "-c", 'printf %s "$JOBRUN_REQUEST" | python3 -m dm_operators.runner run - --repo /model/repo --operators-dir /model/repo/operators '
                                            '--capabilities "$CAPABILITIES" --artifacts /work/artifacts'],
                    "resources": env.get("resources") or {"requests": {"cpu": "100m", "memory": "256Mi"}, "limits": {"memory": "2Gi"}},
                }],
                "volumes": [{"name": "model", "emptyDir": {}}, {"name": "work", "emptyDir": {}}],
            }},
        },
    }


def phase_of(job_status: dict | None) -> str:
    """Running until the Job says otherwise. A Job that failed before the runner could answer is Failed with no result."""
    s = job_status or {}
    if s.get("succeeded"):
        return "Succeeded"
    if s.get("failed") or any(c.get("type") == "Failed" and c.get("status") == "True" for c in s.get("conditions") or []):
        return "Failed"
    return "Running"


def parse_result(log: str) -> dict | None:
    """The runner's stdout is one JSON document; tolerate noise before it."""
    for start in [m.start() for m in re.finditer(r"^\{", log, re.M)]:
        try:
            doc = json.loads(log[start:])
        except ValueError:
            continue
        if isinstance(doc, dict) and "status" in doc:
            return doc
    return None


def summarise(result: dict | None, phase: str, log: str = "") -> dict:
    """What goes into `status`: small enough for etcd. The whole result is kept in a ConfigMap."""
    if result is None:
        tail = [line for line in log.splitlines() if line.strip()][-15:]
        return {"phase": "Failed", "message": "The job ended without a result from the runner.", "log": tail}
    ok = result["status"] == "succeeded"
    return {
        "phase": "Succeeded" if ok else "Failed",
        "message": result.get("message", ""), "exitCode": result.get("exit_code"), "log": (result.get("log") or [])[-15:],
        "files": [f["path"] for f in result.get("files", [])], "artifacts": result.get("artifacts", []),
        "outputBytes": len(result.get("output", "")), "startedAt": result.get("started_at"), "finishedAt": result.get("finished_at"),
    }


def result_configmap(run_name: str, namespace: str, result: dict) -> dict:
    """The full result for whoever delivers it; file contents are dropped first if it would not fit."""
    body = json.dumps(result)
    truncated = len(body.encode()) > _RESULT_BYTES
    if truncated:
        body = json.dumps({**result, "files": [{"path": f["path"], "content": None} for f in result.get("files", [])], "output": "", "truncated": True})
    return {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": f"{job_name(run_name)}-result", "namespace": namespace, "labels": {LABEL: run_name}}, "data": {"result.json": body}}

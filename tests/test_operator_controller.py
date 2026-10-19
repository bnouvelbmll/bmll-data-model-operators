"""The controller's decisions (no cluster): what is refused, what Job is built, how a finished Job is read; and the chart that ships it."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from dm_operators.controller import logic  # noqa: E402
from dm_operators.runner import JobError  # noqa: E402

SHA = "a" * 40
REGISTRY = {"environments": {
    "cluster": {"executor": "kubernetes", "capabilities": ["snowflake", "product-tests"], "credentials_secret": "dm-secrets"},
    "lab": {"executor": "ssh", "capabilities": ["lab"]},
    "bare": {"executor": "kubernetes", "capabilities": []},
}}


def spec(**kw):
    return {"operator": "build-sql", "model": {"repo": "x/y", "sha": SHA}, "environment": "cluster", **kw}


def test_a_jobrun_is_completed_and_validated_with_the_runners_schema():
    request, operator, env = logic.plan("run-sql-0001", spec(), "2026-10-09T10:00:00Z", REGISTRY)
    assert request["id"] == "run-sql-0001" and request["requested_by"] == "kubectl" and request["deliver"] == {"mode": "report"}
    assert operator["name"] == "build-sql" and env["executor"] == "kubernetes"


@pytest.mark.parametrize("name, kw, message", [
    ("run-sql-0001", {"environment": "nowhere"}, "no environment 'nowhere'"),
    ("run-sql-0001", {"environment": "lab"}, "'ssh' executor"),
    ("run-sql-0001", {"operator": "coverage-map"}, "only planned"),
    ("run-sql-0001", {"operator": "observe-materialisations", "parameters": {"assets": []}, "environment": "bare"}, "needs one of"),
    ("x", {}, "Invalid job request"),                       # a name the request schema does not accept
    ("run-sql-0001", {"model": {"repo": "x", "sha": "abc"}}, "Invalid job request"),
    ("run-sql-0001", {"parameters": {"nope": 1}}, "no parameter"),
])
def test_what_cannot_run_is_refused_before_a_job_exists(name, kw, message):
    s = spec(**kw)
    with pytest.raises(JobError, match=message):
        request, operator, _ = logic.plan(name, s, None, REGISTRY)
        from dm_operators.runner import render_command
        render_command(operator, request.get("parameters"))


def build(**kw):
    request, operator, env = logic.plan("run-sql-0001", spec(**kw), "2026-10-09T10:00:00Z", REGISTRY)
    return request, logic.build_job(request, operator, env, namespace="dm", default_image="img:1", default_repo_url="git://origin/model.git")


def test_the_job_checks_out_the_pinned_sha_and_runs_the_runner_with_only_what_it_needs():
    request, job = build()
    pod = job["spec"]["template"]["spec"]
    init, main = pod["initContainers"][0], pod["containers"][0]
    assert job["metadata"]["name"] == "jobrun-run-sql-0001" and job["metadata"]["labels"][logic.LABEL] == "run-sql-0001"
    assert {e["name"]: e["value"] for e in init["env"]}["SHA"] == SHA and "git://origin/model.git" in json.dumps(init["env"])
    assert "git checkout --quiet --detach" in init["command"][2], "the manifests the runner reads (--operators-dir) must be on disk at the sha"
    assert "dm_operators.runner run" in main["command"][2] and "--operators-dir /model/repo/operators" in main["command"][2]
    assert json.loads(next(e["value"] for e in main["env"] if e["name"] == "JOBRUN_REQUEST")) == request
    assert main["envFrom"] == [{"secretRef": {"name": "dm-secrets", "optional": True}}], "credentials come from the environment's Secret"
    assert pod["automountServiceAccountToken"] is False and pod["securityContext"]["runAsNonRoot"] is True
    assert main["securityContext"]["allowPrivilegeEscalation"] is False
    assert job["spec"]["backoffLimit"] == 0 and job["spec"]["activeDeadlineSeconds"] == 180 + 120
    assert "token" not in json.dumps(request).lower() and "password" not in json.dumps(request).lower()


def test_a_request_timeout_and_long_names_are_respected():
    _, job = build(timeout_seconds=30)
    assert job["spec"]["activeDeadlineSeconds"] == 150
    assert len(logic.job_name("r" * 100)) <= 63 and not logic.job_name("r" * 100).endswith("-")


def test_the_phase_follows_the_job():
    assert logic.phase_of(None) == "Running" and logic.phase_of({"active": 1}) == "Running"
    assert logic.phase_of({"succeeded": 1}) == "Succeeded"
    assert logic.phase_of({"failed": 1}) == "Failed"
    assert logic.phase_of({"conditions": [{"type": "Failed", "status": "True", "reason": "DeadlineExceeded"}]}) == "Failed"


def test_the_runners_answer_is_found_in_a_noisy_log_and_summarised_small():
    result = {"request_id": "r", "status": "succeeded", "exit_code": 0, "message": "", "log": [f"l{i}" for i in range(40)],
              "output": "x" * 100, "files": [{"path": "crd/status/a.yaml", "content": "id: a"}], "artifacts": ["dist/sql/m.json"]}
    log = "warning: something\n" + json.dumps(result, indent=2)
    assert logic.parse_result(log) == result
    assert logic.parse_result("no json here") is None and logic.parse_result('{"x": 1}') is None
    summary = logic.summarise(result, "Succeeded")
    assert summary["phase"] == "Succeeded" and len(summary["log"]) == 15 and summary["files"] == ["crd/status/a.yaml"] and summary["outputBytes"] == 100
    assert logic.summarise({**result, "status": "failed", "exit_code": 1, "message": "exited with 1"}, "Failed")["phase"] == "Failed"
    lost = logic.summarise(None, "Failed", "fatal: repository not found\n")
    assert lost["phase"] == "Failed" and "without a result" in lost["message"] and lost["log"] == ["fatal: repository not found"]


def test_a_huge_result_keeps_its_shape_but_drops_file_contents_to_fit_a_configmap():
    big = {"status": "succeeded", "files": [{"path": "a.yaml", "content": "y" * 1_000_000}], "output": "z"}
    cm = logic.result_configmap("run-sql-0001", "dm", big)
    stored = json.loads(cm["data"]["result.json"])
    assert stored["truncated"] and stored["files"] == [{"path": "a.yaml", "content": None}]
    assert len(cm["data"]["result.json"]) < 1024 * 1024
    small = logic.result_configmap("run-sql-0001", "dm", {"status": "succeeded", "files": []})
    assert "truncated" not in json.loads(small["data"]["result.json"])


# ------------------------------------------------------------------------------------------------------------------ chart
CHART = ROOT / "deploy/helm/datamodel-operators"
HELM = os.environ.get("HELM") or shutil.which("helm") or None


@pytest.mark.skipif(not HELM, reason="helm not installed")
def test_the_chart_renders_a_namespaced_controller_with_no_cluster_wide_rights():
    out = subprocess.run([HELM, "template", "dm", str(CHART), "-n", "dm", "--set", "modelRepoUrl=git://o/m.git"], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    docs = [d for d in yaml.safe_load_all(out.stdout) if d]
    kinds = sorted(d["kind"] for d in docs)
    assert kinds == ["ConfigMap", "Deployment", "Role", "RoleBinding", "ServiceAccount"], kinds
    assert not any(k.startswith("Cluster") for k in kinds)
    registry = yaml.safe_load(next(d for d in docs if d["kind"] == "ConfigMap")["data"]["environments.yaml"])
    assert registry["environments"]["cluster"]["executor"] == "kubernetes"
    role = next(d for d in docs if d["kind"] == "Role")
    verbs = {(g, r): set(rule["verbs"]) for rule in role["rules"] for g in rule["apiGroups"] for r in rule["resources"]}
    assert "create" in verbs[("batch", "jobs")] and "create" not in verbs[("", "pods")], "the controller creates Jobs, never pods or secrets"
    assert ("", "secrets") not in verbs, "it never reads Secrets: the Job pods mount them"
    deploy = next(d for d in docs if d["kind"] == "Deployment")
    assert deploy["spec"]["template"]["spec"]["containers"][0]["command"][:3] == ["kopf", "run", "--standalone"]


def test_the_crd_mirrors_the_request_schema():
    crd = yaml.safe_load((CHART / "crds/jobrun.yaml").read_text())
    props = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]
    request = json.loads((ROOT / "contract/jobrun.schema.json").read_text())["properties"]
    # everything a person may write in a JobRun is a request field; the controller fills in id, requested_*
    assert set(props) <= set(request), set(props) - set(request)
    assert set(request) - set(props) == {"id", "requested_at"}
    assert props["model"]["properties"]["sha"]["pattern"] == request["model"]["properties"]["sha"]["pattern"]


def test_the_controller_module_imports_when_its_dependencies_are_present():
    pytest.importorskip("kopf")
    pytest.importorskip("kubernetes")
    import dm_operators.controller.main  # noqa: F401

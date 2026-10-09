"""The runner: parameters bind, requests are refused before anything starts, and the process executor runs in a throwaway checkout."""

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from operators.runner import JobError, check_capabilities, load_operator, render_command, run_process  # noqa: E402
from operators.runner.process import job_environment  # noqa: E402

GIT = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t", "PATH": __import__("os").environ["PATH"]}


def git(cwd, *a):
    return subprocess.run(["git", *a], cwd=cwd, env=GIT, check=True, capture_output=True, text=True).stdout.strip()


def request(sha, operator="demo", **kw):
    return {"id": "run-test-0001", "operator": operator, "model": {"repo": "x/y", "sha": sha}, "environment": "local",
            "deliver": {"mode": "report"}, "requested_by": "t", "requested_at": "2026-10-09T10:00:00Z", **kw}


@pytest.fixture
def world(tmp_path):
    """A model repository with one script, and an operators folder with a small operator that writes a status file."""
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    (repo / "scripts/demo.py").write_text(
        "import os, sys, pathlib\n"
        "pathlib.Path('crd/status').mkdir(parents=True, exist_ok=True)\n"
        "pathlib.Path('crd/status/s.yaml').write_text('args: ' + repr(sys.argv[1:]) + '\\nsecret: ' + repr(os.environ.get('GITHUB_TOKEN')) + '\\nkept: ' + repr(os.environ.get('DEMO_KEY')) + '\\n')\n"
        "pathlib.Path('crd/other.yaml').write_text('not declared')\n"
        "print('hello'); sys.exit(int(os.environ.get('DEMO_EXIT', '0')))\n")
    (repo / "scripts/slow.py").write_text("import time; time.sleep(30)\n")
    git(tmp_path, "init", "-q", str(repo))
    git(repo, "add", "-A"); git(repo, "commit", "-q", "-m", "seed")
    ops = tmp_path / "operators"
    for name, body in {
        "demo": {"description": "A demo operator for the tests.", "entrypoint": {"command": ["python3", "scripts/demo.py", "--assets", "{assets}", "--limit={limit}"]},
                 "parameters": [{"name": "assets", "type": "list", "required": True}, {"name": "limit", "type": "integer", "default": 5}],
                 "requires": ["snowflake"], "credentials": ["DEMO_KEY"], "outputs": [{"kind": "model-files", "paths": ["crd/status/*.yaml"]}]},
        "slow": {"description": "Takes too long on purpose.", "entrypoint": {"command": ["python3", "scripts/slow.py"]}, "timeout_seconds": 1, "outputs": [{"kind": "report"}]},
        "ghost": {"description": "Runs a program nobody installed.", "entrypoint": {"command": ["no-such-program-xyz"]}, "outputs": [{"kind": "report"}]},
        "later": {"description": "Not implemented yet, only planned.", "status": "planned", "entrypoint": {"command": ["x"]}, "outputs": [{"kind": "report"}]},
    }.items():
        (ops / name).mkdir(parents=True)
        (ops / name / "operator.yaml").write_text(yaml.safe_dump({"name": name, "status": "available", **body}))
    for f in ("operator.schema.json", "jobrun.schema.json"):
        (ops / f).write_text((ROOT / "operators" / f).read_text())
    return repo, ops, git(repo, "rev-parse", "HEAD")


def test_parameters_bind_defaults_lists_and_embedded_values(world):
    _, ops, _ = world
    op = load_operator("demo", ops)
    assert render_command(op, {"assets": ["a.yaml", "b.yaml"]}) == ["python3", "scripts/demo.py", "--assets", "a.yaml", "b.yaml", "--limit=5"]
    assert render_command(op, {"assets": [], "limit": 9})[-2:] == ["--assets", "--limit=9"]
    for given, message in (({}, "required"), ({"assets": "a"}, "must be a list"), ({"assets": [], "limit": True}, "must be a integer"), ({"assets": [], "x": 1}, "no parameter")):
        with pytest.raises(JobError, match=message):
            render_command(op, given)


def test_capabilities_are_checked_before_anything_starts(world):
    _, ops, sha = world
    op = load_operator("demo", ops)
    check_capabilities(op, ["snowflake", "github"])
    with pytest.raises(JobError, match="needs snowflake"):
        check_capabilities(op, ["databricks"])
    with pytest.raises(JobError, match="needs snowflake"):
        run_process(request(sha, parameters={"assets": []}), world[0], capabilities=["lab"], operators_dir=ops)


def test_invalid_and_planned_requests_are_refused(world):
    repo, ops, sha = world
    with pytest.raises(JobError, match="only planned"):
        run_process(request(sha, "later"), repo, operators_dir=ops)
    with pytest.raises(JobError, match="no operator called"):
        run_process(request(sha, "nope"), repo, operators_dir=ops)
    with pytest.raises(JobError, match="Invalid job request"):
        run_process({**request(sha), "token": "s3cret"}, repo, operators_dir=ops)


def test_the_job_sees_only_what_its_manifest_declares_and_declared_outputs_come_back(world):
    repo, ops, sha = world
    env = {"PATH": __import__("os").environ["PATH"], "GITHUB_TOKEN": "gh-secret", "DEMO_KEY": "k", "OTHER": "x"}
    result = run_process(request(sha, parameters={"assets": ["a.yaml"]}), repo, operators_dir=ops, parent_env=env)
    assert result.status == "succeeded" and result.exit_code == 0 and "hello" in result.log
    assert [f["path"] for f in result.files] == ["crd/status/s.yaml"], "only declared outputs; crd/other.yaml is not model output"
    body = result.files[0]["content"]
    assert "gh-secret" not in body and "secret: None" in body and "kept: 'k'" in body
    assert "['--assets', 'a.yaml', '--limit=5']" in body
    assert job_environment(load_operator("demo", ops), env).keys() == {"PATH", "DEMO_KEY"}


def test_the_callers_checkout_is_untouched_and_the_run_is_pinned_to_the_sha(world):
    repo, ops, sha = world
    (repo / "scripts/demo.py").write_text("raise SystemExit('edited after the pinned revision')\n")  # uncommitted edit
    before = git(repo, "status", "--porcelain")
    result = run_process(request(sha, parameters={"assets": []}), repo, operators_dir=ops)
    assert result.status == "succeeded", "ran the committed script, not the edited one"
    assert git(repo, "status", "--porcelain") == before and not (repo / "crd").exists()
    assert len(git(repo, "worktree", "list").splitlines()) == 1, "the throwaway checkout is removed"


def test_failures_timeouts_and_missing_programs_are_results_not_crashes(world):
    repo, ops, sha = world
    failed = run_process(request(sha, parameters={"assets": []}), repo, operators_dir=ops, parent_env={"PATH": __import__("os").environ["PATH"], "DEMO_EXIT": "3"})
    assert failed.status == "succeeded", "DEMO_EXIT is not declared, so the job never sees it"
    slow = run_process(request(sha, "slow"), repo, operators_dir=ops)
    assert slow.status == "timed-out" and "did not finish within 1s" in slow.message
    ghost = run_process(request(sha, "ghost"), repo, operators_dir=ops)
    assert ghost.status == "failed" and "not installed" in ghost.message


def test_cli_prints_a_result_and_refuses_an_unrunnable_request(world):
    repo, ops, sha = world
    ok = subprocess.run([sys.executable, "-m", "operators.runner", "run", "-", "--repo", str(repo)], input=json.dumps(request(sha, "later")),
                        cwd=ROOT, capture_output=True, text=True)
    assert ok.returncode == 2 and json.loads(ok.stdout)["status"] == "refused"
    real = subprocess.run([sys.executable, "-m", "operators.runner", "run", "-", "--repo", str(ROOT), "--capabilities", "lab"],
                          input=json.dumps(request(git(ROOT, "rev-parse", "HEAD"), "observe-materialisations", parameters={"assets": []})), cwd=ROOT, capture_output=True, text=True)
    assert real.returncode == 2 and "needs" in json.loads(real.stdout)["message"]

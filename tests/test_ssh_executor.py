"""The ssh executor, with a stand-in for `ssh` that runs the far-end command on this machine."""

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from dm_operators.runner import JobError  # noqa: E402
from dm_operators.runner.ssh import build_bundle, run_ssh, ssh_command  # noqa: E402

SHA_FILE = "0" * 40


def git(cwd, *a):
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t", "PATH": os.environ["PATH"], "HOME": str(cwd)}
    return subprocess.run(["git", *a], cwd=cwd, env=env, check=True, capture_output=True, text=True).stdout.strip()


def request(sha, operator="lab-demo", **kw):
    return {"id": "run-ssh-0001", "operator": operator, "model": {"repo": "x/y", "sha": sha}, "environment": "lab", "deliver": {"mode": "pull-request"},
            "requested_by": "t", "requested_at": "2026-10-19T10:00:00Z", **kw}


@pytest.fixture
def world(tmp_path):
    repo = tmp_path / "model"
    (repo / "scripts").mkdir(parents=True); (repo / "operators/lab-demo").mkdir(parents=True); (repo / "operators/art").mkdir(parents=True)
    (repo / "scripts/demo.py").write_text(
        "import os, pathlib\npathlib.Path('crd/status').mkdir(parents=True, exist_ok=True)\n"
        "pathlib.Path('crd/status/lab.yaml').write_text('secret: ' + repr(os.environ.get('GITHUB_TOKEN')) + '\\nhome_set: ' + repr('PYTHONPATH' in os.environ) + '\\n')\n"
        "pathlib.Path('crd/untouched.yaml').write_text('not declared')\nprint('listing done')\n")
    for f in ("operator.schema.json", "jobrun.schema.json"):
        (repo / "operators" / f).write_text((ROOT / "contract" / f).read_text())
    (repo / "operators/lab-demo/operator.yaml").write_text(yaml.safe_dump({
        "name": "lab-demo", "status": "available", "description": "Writes a status file on the far end.", "entrypoint": {"command": ["python3", "scripts/demo.py"]},
        "requires": ["lab"], "outputs": [{"kind": "model-files", "paths": ["crd/status/*.yaml"]}]}))
    (repo / "operators/art/operator.yaml").write_text(yaml.safe_dump({
        "name": "art", "status": "available", "description": "Makes an artifact, which ssh cannot return.", "entrypoint": {"command": ["python3", "scripts/demo.py"]},
        "outputs": [{"kind": "artifact", "paths": ["dist/**"]}]}))
    git(tmp_path, "init", "-q", str(repo)); git(repo, "add", "-A"); git(repo, "commit", "-q", "-m", "seed")
    fake = tmp_path / "fake-ssh"   # runs the remote command locally, reading the bundle on stdin, like ssh would
    fake.write_text('#!/bin/sh\nif [ -n "$FAKE_SSH_FAIL" ]; then echo "ssh: connect to host bmll-proxy-bn port 1222: Connection refused" >&2; exit 255; fi\n'
                    'for last; do :; done\nexec sh -c "$last"\n')
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    env = {"host": "bmll-proxy-bn", "workdir": str(tmp_path / "remote"), "capabilities": ["lab"]}
    return repo, git(repo, "rev-parse", "HEAD"), env, str(fake)


def test_the_run_happens_on_the_far_end_and_only_declared_files_come_back(world, tmp_path):
    repo, sha, env, fake = world
    secret = os.environ.get("GITHUB_TOKEN")
    os.environ["GITHUB_TOKEN"] = "gh-secret"
    try:
        result = run_ssh(request(sha), repo, env, ssh=fake)
    finally:
        os.environ.pop("GITHUB_TOKEN", None) if secret is None else os.environ.__setitem__("GITHUB_TOKEN", secret)
    assert result.status == "succeeded" and "listing done" in result.log
    assert [f["path"] for f in result.files] == ["crd/status/lab.yaml"], "crd/untouched.yaml is not declared model output"
    assert "gh-secret" not in result.files[0]["content"] and "secret: None" in result.files[0]["content"]
    assert not list((tmp_path / "remote").glob("job-*")), "the scratch directory is removed"


def test_the_model_is_taken_at_the_pinned_revision_not_from_the_working_tree(world):
    repo, sha, env, fake = world
    (repo / "scripts/demo.py").write_text("raise SystemExit('edited after the pinned revision')\n")
    git(repo, "commit", "-qam", "later")
    assert run_ssh(request(sha), repo, env, ssh=fake).status == "succeeded"


def test_a_closed_window_is_unreachable_which_means_try_later_not_failed(world, monkeypatch):
    repo, sha, env, fake = world
    monkeypatch.setenv("FAKE_SSH_FAIL", "1")
    result = run_ssh(request(sha), repo, env, ssh=fake)
    assert result.status == "unreachable" and "try again later" in result.message and "Connection refused" in " ".join(result.log + [result.message])


def test_what_cannot_run_is_refused_before_anything_is_uploaded(world):
    repo, sha, env, fake = world
    with pytest.raises(JobError, match="needs lab"):
        run_ssh(request(sha), repo, {**env, "capabilities": []}, ssh=fake)
    with pytest.raises(JobError, match="artifacts"):
        run_ssh(request(sha, "art"), repo, env, ssh=fake)
    with pytest.raises(JobError, match="no operator"):
        run_ssh(request(sha, "ghost"), repo, env, ssh=fake)
    with pytest.raises(JobError):
        run_ssh(request("f" * 40), repo, env, ssh=fake)


def test_the_ssh_command_carries_the_connection_settings_and_asks_for_no_password():
    cmd = ssh_command({"host": "bmll-proxy-bn", "user": "bmll", "port": 1222, "proxy_jump": "serveo.net", "identity_file": "~/.ssh/k", "workdir": "/home/bmll/user/jobs"})
    assert cmd[:3] == ["ssh", "-o", "BatchMode=yes"] and "-J" in cmd and cmd[cmd.index("-J") + 1] == "serveo.net"
    assert cmd[cmd.index("-p") + 1] == "1222" and "IdentitiesOnly=yes" in cmd and cmd[-2] == "bmll@bmll-proxy-bn"
    assert "dm_operators.runner exec" in cmd[-1] and "/home/bmll/user/jobs" in cmd[-1]
    with pytest.raises(JobError):
        ssh_command({})


def test_cli_picks_the_executor_from_the_environment_registry(world, tmp_path):
    repo, sha, env, fake = world
    registry = tmp_path / "envs.yaml"
    registry.write_text(yaml.safe_dump({"environments": {"lab": {**env, "executor": "ssh"}, "elsewhere": {"executor": "spcs-job", "capabilities": ["lab"]}}}))
    path = lambda name: Path(tmp_path / name)
    path("r.json").write_text(json.dumps(request(sha)))
    path("r2.json").write_text(json.dumps({**request(sha), "environment": "elsewhere"}))
    path_env = {**os.environ, "PATH": f"{Path(fake).parent}:{os.environ['PATH']}"}
    Path(fake).rename(Path(fake).parent / "ssh")
    ran = subprocess.run([sys.executable, "-m", "dm_operators.runner", "run", str(path("r.json")), "--repo", str(repo), "--environments", str(registry)], cwd=ROOT, capture_output=True, text=True, env=path_env)
    assert json.loads(ran.stdout)["status"] == "succeeded", ran.stdout + ran.stderr
    refused = subprocess.run([sys.executable, "-m", "dm_operators.runner", "run", str(path("r2.json")), "--repo", str(repo), "--environments", str(registry)], cwd=ROOT, capture_output=True, text=True, env=path_env)
    assert refused.returncode == 2 and "spcs-job" in json.loads(refused.stdout)["message"]

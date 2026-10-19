"""The `ssh` executor: run an operator on another machine, with nothing there but Python.

The model at the pinned revision (`git archive`), this package, and the request travel as one tar stream on the ssh connection's standard input;
the far end unpacks them in a scratch directory, runs `python3 -m dm_operators.runner exec` and prints the result, and the directory is removed.
No git, no clone, no push rights, and no secret leaves this machine: the far end uses the credentials it already has (the BMLL Lab's IAM role).
Reaching the Lab is the personal SSH server's business (basalt `enable_personal_ssh_server`: a Lab instance job behind a tunnel, key-only, inside
an hours window); when it is closed the run is `unreachable`, which means *try later*, not *failed*.
"""

from __future__ import annotations

import io
import json
import shlex
import subprocess
import tarfile
from dataclasses import fields
from pathlib import Path

from .core import JobError, PACKAGE_ROOT, Result, check_capabilities, contract_dir, load_operator, render_command, validate_request
from .process import _git, _now


def build_bundle(repo: Path, sha: str, request: dict) -> bytes:
    """One tar: model/ (the model at `sha`), ops/ (this package and its manifests), request.json."""
    archive = subprocess.run(["git", "archive", "--format=tar", sha], cwd=repo, capture_output=True)
    if archive.returncode:
        raise JobError(f"git archive failed: {archive.stderr.decode(errors='replace').strip()}")
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w") as bundle, tarfile.open(fileobj=io.BytesIO(archive.stdout)) as model:
        for member in model:
            member.name = f"model/{member.name}"
            bundle.addfile(member, model.extractfile(member) if member.isreg() else None)
        for folder in ("dm_operators", "manifests"):
            bundle.add(PACKAGE_ROOT / folder, arcname=f"ops/{folder}", filter=lambda i: None if "__pycache__" in i.name else i)
        body = json.dumps(request).encode()
        info = tarfile.TarInfo("request.json")
        info.size = len(body)
        bundle.addfile(info, io.BytesIO(body))
    return out.getvalue()


def remote_command(env: dict) -> str:
    work = env.get("workdir") or "/tmp/dm-jobs"
    python = env.get("python") or "python3"
    return (f"set -e; mkdir -p {shlex.quote(work)}; d=$(mktemp -d -p {shlex.quote(work)} job-XXXXXX); trap 'rm -rf \"$d\"' EXIT; cd \"$d\"; tar x; "
            f"PYTHONPATH=\"$d/ops\" {shlex.quote(python)} -m dm_operators.runner exec --checkout \"$d/model\" --request \"$d/request.json\"")


def ssh_command(env: dict, ssh: str = "ssh") -> list[str]:
    if not env.get("host"):
        raise JobError("An ssh environment needs a `host`.")
    cmd = [ssh, "-o", "BatchMode=yes", "-o", f"ConnectTimeout={int(env.get('connect_timeout', 30))}"]
    if env.get("proxy_jump"):
        cmd += ["-J", env["proxy_jump"]]
    if env.get("port"):
        cmd += ["-p", str(env["port"])]
    if env.get("identity_file"):
        cmd += ["-i", str(env["identity_file"]), "-o", "IdentitiesOnly=yes"]
    for option in env.get("ssh_options", []):
        cmd += ["-o", option]
    target = f"{env['user']}@{env['host']}" if env.get("user") else env["host"]
    return cmd + [target, remote_command(env)]


def run_ssh(request: dict, repo: str | Path, env: dict, *, operators_dir: Path | None = None, capabilities: list[str] | None = None,
            ssh: str = "ssh") -> Result:
    """Raises JobError for a request that cannot be run at all; an unreachable or failing far end comes back as a Result."""
    repo = Path(repo)
    contract = contract_dir(repo, operators_dir)
    validate_request(request, contract)
    operator = load_operator(request["operator"], contract)
    check_capabilities(operator, capabilities if capabilities is not None else env.get("capabilities", []))
    if any(o["kind"] == "artifact" for o in operator["outputs"]):
        raise JobError(f"{operator['name']} produces artifacts, which the ssh executor cannot hand back yet.")
    render_command(operator, request.get("parameters"))  # a request with bad parameters is refused here, not after the upload
    sha = _git(repo, "rev-parse", "--verify", request["model"]["sha"] + "^{commit}").strip()
    bundle = build_bundle(repo, sha, request)
    timeout = (request.get("timeout_seconds") or operator.get("timeout_seconds", 600)) + int(env.get("connect_timeout", 30)) + 30
    result = Result(request_id=request["id"], status="failed", started_at=_now())
    try:
        done = subprocess.run(ssh_command(env, ssh), input=bundle, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        result.status, result.message = "timed-out", f"{operator['name']} did not finish within {timeout}s (including the connection)."
    except FileNotFoundError:
        result.status, result.message = "unreachable", f"{ssh} is not installed here."
    else:
        stdout, stderr = done.stdout.decode(errors="replace"), done.stderr.decode(errors="replace")
        try:
            data = json.loads(stdout)
            known = {f.name for f in fields(Result)}
            result = Result(**{k: v for k, v in data.items() if k in known})
            if done.returncode == 2 and result.status == "refused":
                raise JobError(result.message)
        except json.JSONDecodeError:
            tail = " ".join(stderr.strip().splitlines()[-2:])
            if done.returncode == 255:  # ssh itself: no route, closed window, refused key
                result.status, result.message = "unreachable", f"The far end could not be reached ({tail or 'ssh exited 255'}); try again later."
            else:
                result.message = f"The far end gave no result (exit {done.returncode}): {tail}"
            result.log = [l for l in stderr.splitlines() if l.strip()][-60:]
    result.finished_at = _now()
    return result

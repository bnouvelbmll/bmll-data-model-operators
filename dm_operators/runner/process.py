"""The `process` executor: run the operator on this machine, in a throwaway checkout of the requested revision.

The caller's working tree and branch are never touched: the run happens in a detached `git worktree` at `model.sha`,
with only the credentials the manifest declares in its environment. Outputs are read back from that checkout.
"""

from __future__ import annotations

import fnmatch
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .core import JobError, Result, check_capabilities, render_command, validate_request, load_operator, OPERATORS

# What a job needs to find its interpreter and talk to the network, and nothing of the orchestrator's own secrets.
_BASE_ENV = ("PATH", "HOME", "LANG", "LC_ALL", "TMPDIR", "TZ", "PYTHONPATH", "VIRTUAL_ENV", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
             "http_proxy", "https_proxy", "no_proxy", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "PYTHON")
_TAIL = 60
_OUTPUT_CAP = 4 * 1024 * 1024


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def job_environment(operator: dict, parent: dict | None = None) -> dict:
    parent = os.environ if parent is None else parent
    names = list(_BASE_ENV) + list(operator.get("credentials", []))
    return {k: parent[k] for k in names if k in parent}


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True)
    if done.returncode:
        raise JobError(f"git {args[0]} failed: {(done.stderr or done.stdout).strip().splitlines()[-1] if (done.stderr or done.stdout).strip() else done.returncode}")
    return done.stdout


def _matches(path: str, globs: list[str]) -> bool:
    return any(fnmatch.fnmatch(path, g) for g in globs)


def _changed_model_files(checkout: Path, globs: list[str]) -> list[dict]:
    """New or modified files the operator declared as model output. `fnmatch` `*` also crosses `/`, which is what `dist/**`-style globs want."""
    listing = _git(checkout, "status", "--porcelain", "--untracked-files=all", "-z")
    files = []
    for entry in listing.split("\0"):
        if len(entry) < 4 or entry[0] == "D" or entry[1] == "D":
            continue
        path = entry[3:]
        if _matches(path, globs):
            files.append({"path": path, "content": (checkout / path).read_text()})
    return sorted(files, key=lambda f: f["path"])


def run_process(request: dict, repo: str | Path, *, capabilities: list[str] | None = None, artifact_dir: str | Path | None = None,
                operators_dir: Path = OPERATORS, parent_env: dict | None = None, keep: bool = False) -> Result:
    """Run `request` against the git repository at `repo`. A job that fails comes back as a Result; `JobError` is raised only for a
    request that is invalid or cannot be run in this environment (nothing was started)."""
    repo = Path(repo)
    validate_request(request, operators_dir)
    operator = load_operator(request["operator"], operators_dir)
    if capabilities is not None:
        check_capabilities(operator, capabilities)
    command = render_command(operator, request.get("parameters"))
    result = Result(request_id=request["id"], status="failed", started_at=_now())
    checkout = Path(tempfile.mkdtemp(prefix=f"job-{request['id']}-"))
    stdout = ""
    timeout = request.get("timeout_seconds") or operator.get("timeout_seconds", 600)
    try:
        _git(repo, "worktree", "add", "--detach", str(checkout), request["model"]["sha"])
        try:
            done = subprocess.run(command, cwd=checkout, env=job_environment(operator, parent_env), capture_output=True, text=True, timeout=timeout)
            output, result.exit_code = f"{done.stdout}\n{done.stderr}", done.returncode
            stdout = done.stdout
            result.status = "succeeded" if done.returncode == 0 else "failed"
            if done.returncode:
                result.message = f"{operator['name']} exited with {done.returncode}."
        except subprocess.TimeoutExpired as e:
            text = lambda b: b.decode(errors="replace") if isinstance(b, bytes) else (b or "")
            output, result.status, result.message = f"{text(e.stdout)}\n{text(e.stderr)}", "timed-out", f"{operator['name']} did not finish within {timeout}s."
        except FileNotFoundError as e:
            output, result.message = str(e), f"{operator['name']} cannot start here: {e.filename or command[0]} is not installed."
        result.log = [line.rstrip() for line in output.splitlines() if line.strip()][-_TAIL:]
        for out in operator["outputs"]:
            if out["kind"] == "model-files":
                result.files += _changed_model_files(checkout, out.get("paths", []))
            elif out["kind"] == "report":
                result.output = stdout[:_OUTPUT_CAP]
            elif out["kind"] == "artifact" and artifact_dir:
                for path in sorted(p for p in checkout.glob("**/*") if p.is_file() and not ".git" in p.relative_to(checkout).parts):
                    rel = path.relative_to(checkout).as_posix()
                    if _matches(rel, out.get("paths", [])):
                        target = Path(artifact_dir) / rel
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(path, target)
                        result.artifacts.append(rel)
    finally:
        if not keep:
            subprocess.run(["git", "worktree", "remove", "--force", str(checkout)], cwd=repo, capture_output=True)
            shutil.rmtree(checkout, ignore_errors=True)
            subprocess.run(["git", "worktree", "prune"], cwd=repo, capture_output=True)
        result.finished_at = _now()
    return result

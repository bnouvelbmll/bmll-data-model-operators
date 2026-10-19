"""Run an operator in a directory that already holds the model at the right revision (no git needed).

This is what runs on the far end of the `ssh` executor: the model and this package were unpacked there for one run. Changes are found by
comparing file hashes before and after, since the directory is not a git checkout.
"""

from __future__ import annotations

import fnmatch
import hashlib
import os
import subprocess
from pathlib import Path

from .core import JobError, Result, check_capabilities, load_operator, render_command, validate_request
from .process import _OUTPUT_CAP, _TAIL, _now, job_environment


def snapshot(root: Path) -> dict[str, str]:
    seen = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d != ".git"]
        for name in filenames:
            path = Path(dirpath) / name
            try:
                seen[path.relative_to(root).as_posix()] = hashlib.sha1(path.read_bytes()).hexdigest()
            except OSError:
                continue
    return seen


def run_here(request: dict, checkout: str | Path, *, operators_dir: Path | None = None, capabilities: list[str] | None = None,
             parent_env: dict | None = None) -> Result:
    checkout = Path(checkout)
    operators_dir = operators_dir or checkout / "operators"
    validate_request(request, operators_dir)
    operator = load_operator(request["operator"], operators_dir)
    if any(o["kind"] == "artifact" for o in operator["outputs"]):
        raise JobError(f"{operator['name']} produces artifacts, which a run in a bare directory cannot hand back yet.")
    if capabilities is not None:
        check_capabilities(operator, capabilities)
    command = render_command(operator, request.get("parameters"))
    result = Result(request_id=request["id"], status="failed", started_at=_now())
    timeout = request.get("timeout_seconds") or operator.get("timeout_seconds", 600)
    before = snapshot(checkout)
    stdout = ""
    try:
        done = subprocess.run(command, cwd=checkout, env=job_environment(operator, parent_env), capture_output=True, text=True, timeout=timeout)
        output, result.exit_code, stdout = f"{done.stdout}\n{done.stderr}", done.returncode, done.stdout
        result.status = "succeeded" if done.returncode == 0 else "failed"
        if done.returncode:
            result.message = f"{operator['name']} exited with {done.returncode}."
    except subprocess.TimeoutExpired as e:
        text = lambda b: b.decode(errors="replace") if isinstance(b, bytes) else (b or "")
        output, result.status, result.message = f"{text(e.stdout)}\n{text(e.stderr)}", "timed-out", f"{operator['name']} did not finish within {timeout}s."
    except FileNotFoundError as e:
        output, result.message = str(e), f"{operator['name']} cannot start here: {e.filename or command[0]} is not installed."
    result.log = [line.rstrip() for line in output.splitlines() if line.strip()][-_TAIL:]
    after = snapshot(checkout)
    for out in operator["outputs"]:
        if out["kind"] == "model-files":
            globs = out.get("paths", [])
            changed = sorted(p for p, h in after.items() if before.get(p) != h and any(fnmatch.fnmatch(p, g) for g in globs))
            result.files += [{"path": p, "content": (checkout / p).read_text()} for p in changed]
        elif out["kind"] == "report":
            result.output = stdout[:_OUTPUT_CAP]
    result.finished_at = _now()
    return result

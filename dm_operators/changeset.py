"""Track what changed in the model and say which operators are due.

    python3 -m dm_operators.changeset plan --repo DIR --head SHA [--base SHA] --environment NAME [--capabilities a,b] [--model-repo ORG/NAME]

Every operator manifest (the model's `operators/` generators and this package's observers) declares `triggers`: globs of model paths whose change
makes its output stale. Given two revisions this lists the operators whose triggers match what changed, each with a ready job-run request (the
changed paths that matched go into the operator's `trigger_parameter`) and, for artifacts, where the official release is published, so that
what the model points at is the release rather than a rebuilt copy. Without `--base` everything is considered changed.
The plan only reads git; it runs nothing. The controller (or CI) creates the runs.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

from .runner.core import JobError, OWN_MANIFESTS, bind_parameters, check_capabilities, contract_dir, load_operator, validate_request

_ID_SAFE = "abcdefghijklmnopqrstuvwxyz0123456789-"


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True)
    if done.returncode:
        raise JobError(f"git {args[0]} failed: {(done.stderr or done.stdout).strip()}")
    return done.stdout


def changed_paths(repo: Path, head: str, base: str | None = None) -> list[str]:
    """Paths that differ between `base` and `head`; every tracked path when there is no base. Submodules count as their path."""
    if base is None:
        return sorted(p for p in _git(repo, "ls-tree", "-r", "--name-only", "-z", head).split("\0") if p)
    return sorted(p for p in _git(repo, "diff", "--name-only", "-z", base, head).split("\0") if p)


def matches(path: str, glob: str) -> bool:
    """`*` crosses `/` (as in the manifests' output globs); a glob without wildcards names a file or a whole folder (a submodule)."""
    if not any(c in glob for c in "*?["):
        return path == glob or path.startswith(glob.rstrip("/") + "/")
    return fnmatch.fnmatch(path, glob)


def known_operators(contract: Path) -> dict[str, dict]:
    docs = {}
    for folder in (OWN_MANIFESTS, contract):  # the model's declaration wins over a same-named one here
        for path in sorted(folder.glob("*/operator.yaml")):
            doc = yaml.safe_load(path.read_text())
            if doc.get("status") == "available":
                docs[doc["name"]] = doc
    return docs


def plan(repo: str | Path, head: str, base: str | None, *, environment: str, model_repo: str, capabilities: list[str] | None = None,
         operators_dir: str | Path | None = None, now: str | None = None) -> dict:
    repo = Path(repo)
    contract = contract_dir(repo, operators_dir)
    sha = _git(repo, "rev-parse", head).strip()
    changed = changed_paths(repo, sha, base)
    stamp = now or datetime.now(timezone.utc).isoformat(timespec="seconds")
    due, skipped = [], []
    for name, op in sorted(known_operators(contract).items()):
        hit = sorted({p for p in changed for g in op.get("triggers", []) if matches(p, g)})
        if not hit:
            continue
        try:
            if capabilities is not None:
                check_capabilities(op, capabilities)
            param = op.get("trigger_parameter")
            parameters = {param: hit} if param else {}
            bind_parameters(op, parameters)
            kinds = {o["kind"] for o in op["outputs"]}
            mode = "pull-request" if "model-files" in kinds else "artifact" if "artifact" in kinds else "report"
            request = {"id": f"{name}-{sha[:8]}"[:61], "operator": name, "model": {"repo": model_repo, "sha": sha}, "environment": environment,
                       "deliver": {"mode": mode}, "requested_by": "dm-operators-tracker", "requested_at": stamp}
            if parameters:
                request["parameters"] = parameters
            validate_request(request, contract)
        except JobError as e:
            skipped.append({"operator": name, "because": str(e)})
            continue
        entry = {"operator": name, "because": hit if len(hit) <= 20 else hit[:20] + [f"... and {len(hit) - 20} more"], "request": request}
        releases = [o["release"] for o in op["outputs"] if "release" in o]
        if releases:
            entry["release"] = releases
        due.append(entry)
    return {"head": sha, "base": base, "changed": len(changed), "due": due, "skipped": skipped}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="dm_operators.changeset")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan", help="list the operators due for a change between two revisions")
    p.add_argument("--repo", default=".")
    p.add_argument("--head", default="HEAD")
    p.add_argument("--base", help="the revision to compare with; omit to treat everything as changed")
    p.add_argument("--environment", required=True)
    p.add_argument("--capabilities", help="skip operators this environment cannot run")
    p.add_argument("--model-repo", default="bmlltech/data-model")
    p.add_argument("--operators-dir")
    args = ap.parse_args(argv)
    try:
        out = plan(args.repo, args.head, args.base, environment=args.environment, model_repo=args.model_repo,
                   capabilities=args.capabilities.split(",") if args.capabilities else None, operators_dir=args.operators_dir)
    except JobError as e:
        print(json.dumps({"error": str(e)}))
        return 2
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

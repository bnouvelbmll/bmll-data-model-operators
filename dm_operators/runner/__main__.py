"""`python3 -m dm_operators.runner run REQUEST.json|-  [--repo DIR] [--capabilities a,b] [--artifacts DIR] [--environments FILE]`

Reads a job request (the schema is the model's operators/jobrun.schema.json), runs it with the process executor and prints the Result as JSON.
Exit status is 0 when a result was produced (whatever the job's own outcome), 2 when the request could not be run.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from .core import JobError, Result
from .process import run_process


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="dm_operators.runner")
    sub = ap.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run")
    run.add_argument("request", help="path to a JSON job request, or - for stdin")
    run.add_argument("--repo", default=".", help="the git repository holding the model (default: .)")
    run.add_argument("--capabilities", help="comma-separated capabilities of this environment; omit to skip the check")
    run.add_argument("--artifacts", help="directory to copy artifact outputs to")
    run.add_argument("--operators-dir", help="the model's operators/ folder: the schemas and the generators it declares (default: REPO/operators). This package's own manifests/ are always searched too")
    run.add_argument("--environments", help="the deployment's environment registry (YAML); the request's `environment` picks the executor: process (default) or ssh")
    here = sub.add_parser("exec", help="run a request in a directory that already holds the model (the far end of the ssh executor)")
    here.add_argument("--checkout", required=True)
    here.add_argument("--request", required=True)
    args = ap.parse_args(argv)
    try:
        if args.cmd == "exec":
            from .here import run_here
            result = run_here(json.load(open(args.request)), args.checkout)
            print(result.to_json())
            return 0
        request = json.load(sys.stdin if args.request == "-" else open(args.request))
        caps = args.capabilities.split(",") if args.capabilities else None
        extra = {"operators_dir": Path(args.operators_dir)} if args.operators_dir else {}
        env = ((yaml.safe_load(open(args.environments)) or {}).get("environments", {}).get(request.get("environment"))
               if args.environments else None)
        if env and env.get("executor") == "ssh":
            from .ssh import run_ssh
            result = run_ssh(request, args.repo, env, capabilities=caps, **extra)
        elif env and env.get("executor") not in (None, "process"):
            raise JobError(f"Environment {request['environment']!r} is run by the {env['executor']!r} executor, which this runner does not provide.")
        else:
            result = run_process(request, args.repo, capabilities=caps, artifact_dir=args.artifacts, **extra)
    except (JobError, json.JSONDecodeError, OSError) as e:
        rid = request.get("id", "") if "request" in locals() and isinstance(request, dict) else ""
        print(Result(request_id=rid, status="refused", message=str(e)).to_json())
        return 2
    print(result.to_json())
    return 0


if __name__ == "__main__":
    sys.exit(main())

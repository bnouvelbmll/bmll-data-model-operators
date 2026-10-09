"""`python3 -m operators.runner run REQUEST.json|-  [--repo DIR] [--capabilities a,b] [--artifacts DIR]`

Reads a job request (see operators/jobrun.schema.json), runs it with the process executor and prints the Result as JSON.
Exit status is 0 when a result was produced (whatever the job's own outcome), 2 when the request could not be run.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .core import JobError, Result
from .process import run_process


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="operators.runner")
    sub = ap.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run")
    run.add_argument("request", help="path to a JSON job request, or - for stdin")
    run.add_argument("--repo", default=".", help="the git repository holding the model (default: .)")
    run.add_argument("--capabilities", help="comma-separated capabilities of this environment; omit to skip the check")
    run.add_argument("--artifacts", help="directory to copy artifact outputs to")
    run.add_argument("--operators-dir", help="operator manifests and schemas to use (default: the ones next to this package); a Job points it at the checkout's own operators/")
    args = ap.parse_args(argv)
    try:
        request = json.load(sys.stdin if args.request == "-" else open(args.request))
        caps = args.capabilities.split(",") if args.capabilities else None
        extra = {"operators_dir": Path(args.operators_dir)} if args.operators_dir else {}
        result = run_process(request, args.repo, capabilities=caps, artifact_dir=args.artifacts, **extra)
    except (JobError, json.JSONDecodeError, OSError) as e:
        rid = request.get("id", "") if "request" in locals() and isinstance(request, dict) else ""
        print(Result(request_id=rid, status="refused", message=str(e)).to_json())
        return 2
    print(result.to_json())
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Observe what the BMLL Lab's storage holds for the parquet materialisations of data assets.

    python3 -m dm_operators.observers.lab --assets 'crd/assets/**/*.yaml' [--max-objects N] [--dry-run]

For each asset materialisation with a `parquet` backend whose location is an S3 path template
(`s3://{L2_ACCESS_POINT_ALIAS}/Folder/{mic}/{yyyy}/{mm}/{dd}/name-{mic}-{yyyy}{mm}{dd}.parquet`), list the objects under the folder (no file is opened)
and record in `crd/status/<asset>.<materialisation>.yaml` how many files there are, their size, which MICs and dates they cover and where the gaps are.

It runs where the storage is reachable: on the Lab itself (its IAM role is all it needs), which is where the `ssh` executor sends it. The access
point aliases come from the environment (L2_ACCESS_POINT_ALIAS, OPRA_ACCESS_POINT_ALIAS) or, failing that, from the Lab's `bmll2` configuration.
Like the other observers it works on the model checkout it runs in (the current directory, or DATAMODEL_ROOT).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator

import yaml

ROOT = Path(os.environ.get("DATAMODEL_ROOT") or Path.cwd()).resolve()
STATUS_DIR = ROOT / "crd" / "status"
CHECKED_BY = "observe-lab-datasets"
MAX_BY_MIC = 200
DEFAULT_MAX_OBJECTS = 2_000_000
_PLACEHOLDER = re.compile(r"\{([A-Za-z0-9_]+)\}")
_GROUP = {"mic": "mic", "yyyy": "y", "mm": "m", "dd": "d"}
_PART = {"mic": r"(?P<mic>[^/]+)", "yyyy": r"(?P<y>\d{4})", "mm": r"(?P<m>\d{2})", "dd": r"(?P<d>\d{2})"}


def parse_location(location: str) -> tuple[str, str, re.Pattern]:
    """`s3://{ALIAS}/Folder/{mic}/...` -> (bucket template, folder prefix to list, regex a key must match, with `mic` and `y`/`m`/`d` groups)."""
    if not location.startswith("s3://"):
        raise ValueError(f"not an S3 location: {location}")
    bucket, _, path = location[5:].partition("/")
    prefix = path.split("{", 1)[0]
    prefix = prefix[: prefix.rfind("/") + 1] if "{" in path else path
    pattern, last, seen = "", 0, set()
    for m in _PLACEHOLDER.finditer(path):
        name = m.group(1)
        group = _PART.get(name)
        if group and name in seen:  # the same part again (the MIC is in the folder and in the file name): it must be equal
            group = "(?P=%s)" % _GROUP[name]
        seen.add(name)
        pattern += re.escape(path[last:m.start()]) + (group or r"[^/]*")
        last = m.end()
    return bucket, prefix, re.compile("^" + pattern + re.escape(path[last:]) + "$")


def resolve_bucket(template: str, env=os.environ) -> str:
    """`{L2_ACCESS_POINT_ALIAS}` is the name of a setting: the environment first, then the Lab's own configuration."""
    def one(m: re.Match) -> str:
        name = m.group(1)
        if env.get(name):
            return env[name]
        try:
            from bmll2._internals import configure  # type: ignore
            value = getattr(configure, name, None)
        except Exception:  # not on the Lab
            value = None
        if not value:
            raise RuntimeError(f"{name} is not set and the Lab configuration does not provide it")
        return value
    return _PLACEHOLDER.sub(one, template)


def list_objects(bucket: str, prefix: str, limit: int) -> Iterator[tuple[str, int]]:
    """(key, size) of every object under `prefix`, at most `limit`; the caller sees the limit was hit when it gets `limit` items."""
    import boto3  # type: ignore

    seen = 0
    for page in boto3.client("s3").get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"]
            seen += 1
            if seen >= limit:
                return


def summarise(objects: Iterable[tuple[str, int]], regex: re.Pattern, prefix: str, *, limit: int = DEFAULT_MAX_OBJECTS, max_by_mic: int = MAX_BY_MIC) -> dict:
    """The `lab` block of a status record. Objects that do not match the template (a manifest, a stray folder) are not counted."""
    files = total = seen = 0
    days: set[date] = set()
    per_mic: dict[str, dict] = defaultdict(lambda: {"files": 0, "days": set()})
    for key, size in objects:
        seen += 1
        m = regex.match(key)
        if not m:
            continue
        try:
            day = date(int(m["y"]), int(m["m"]), int(m["d"]))
        except (IndexError, ValueError, TypeError):
            continue
        files, total = files + 1, total + size
        days.add(day)
        entry = per_mic[m["mic"]]
        entry["files"] += 1
        entry["days"].add(day)
    out: dict = {"prefix": prefix, "files": files, "total_bytes": total, "mics": len(per_mic), "days": len(days)}
    if days:
        first, last = min(days), max(days)
        out.update(first_date=first.isoformat(), last_date=last.isoformat(), gap_days=(last - first).days + 1 - len(days))
        ranked = sorted(per_mic.items(), key=lambda kv: (-kv[1]["files"], kv[0]))
        out["by_mic"] = [{"mic": mic, "files": e["files"], "first_date": min(e["days"]).isoformat(), "last_date": max(e["days"]).isoformat(), "days": len(e["days"])}
                         for mic, e in ranked[:max_by_mic]]
        out["by_mic_truncated"] = len(ranked) > max_by_mic
    out["listing_truncated"] = seen >= limit
    return out


def parquet_materialisations(paths: list[Path]) -> Iterator[dict]:
    for path in paths:
        doc = yaml.safe_load(path.read_text()) or {}
        for spec in doc.get("materialisations") or []:
            destination = (spec or {}).get("destination") or {}
            location = destination.get("location") or ""
            if destination.get("backend") == "parquet" and location.startswith("s3://") and doc.get("id"):
                yield {"subject": doc["id"], "name": spec.get("name"), "location": location}


def status_path(subject: str, name: str | None) -> Path:
    return STATUS_DIR / f"{subject}.{name or 'default'}.yaml"


def write_status(m: dict, lab: dict | None, error: str | None, now: datetime | None = None) -> Path:
    path = status_path(m["subject"], m["name"])
    existing = (yaml.safe_load(path.read_text()) or {}) if path.exists() else {}
    doc = {**existing, "id": f"status.{m['subject']}.{m['name'] or 'default'}", "subject": m["subject"], "materialisation_name": m["name"],
           "destination_location": m["location"], "lab": lab, "checked_at": (now or datetime.now(timezone.utc)).isoformat(),
           "checked_by": CHECKED_BY, "error": error}
    doc = {k: v for k, v in doc.items() if v is not None}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"# GENERATED by the {CHECKED_BY} operator — do not hand-edit.\n# What the Lab's storage holds for {m['subject']}'s {m['name']!r} materialisation.\n"
                    + yaml.safe_dump(doc, sort_keys=False))
    return path


def observe(m: dict, limit: int, lister: Callable = list_objects, env=os.environ) -> dict:
    bucket_template, prefix, regex = parse_location(m["location"])
    bucket = resolve_bucket(bucket_template, env)
    return summarise(lister(bucket, prefix, limit), regex, f"s3://{bucket_template}/{prefix}", limit=limit)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--assets", nargs="+", required=True, help="DataAsset YAML paths or globs")
    ap.add_argument("--max-objects", type=int, default=DEFAULT_MAX_OBJECTS, help="stop listing a folder after this many objects (figures are then lower bounds)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    import glob
    paths = sorted({Path(p) for pattern in args.assets for p in glob.glob(pattern, recursive=True)})
    found = list(parquet_materialisations(paths))
    print(f"Found {len(found)} parquet materialisation(s) in {len(paths)} asset file(s).")
    for m in found:
        print(f"  {m['subject']} ({m['name']}) -> {m['location']}")
    if args.dry_run or not found:
        return 0
    failed = 0
    for m in found:
        try:
            lab = observe(m, args.max_objects)
            error = None
            print(f"  {m['subject']}: {lab['files']} files, {lab['mics']} MICs, {lab.get('first_date')}..{lab.get('last_date')}")
        except Exception as e:  # one unreachable dataset must not stop the others
            lab, error, failed = None, str(e), failed + 1
            print(f"  ERROR {m['subject']}: {e}")
        print(f"  Wrote {write_status(m, lab, error)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

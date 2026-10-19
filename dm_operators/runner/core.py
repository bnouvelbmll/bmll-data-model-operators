"""The contract in code: load a manifest, validate a request, bind parameters, check capabilities."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

import yaml
from jsonschema import Draft7Validator

PACKAGE_ROOT = Path(__file__).resolve().parent.parent.parent
# Manifests of the operators that live here: observers of other systems (Snowflake, Databricks, ...).
OWN_MANIFESTS = PACKAGE_ROOT / "manifests"
_PLACEHOLDER = "{%s}"


class JobError(Exception):
    """A request that cannot be run, with a message meant for the person who asked."""


@dataclass
class Result:
    """What came back. `status` is succeeded | failed | timed-out | unreachable | refused; `refused` means nothing was started, `unreachable` that the
    environment could not be reached (a closed window, no route): try again later."""

    request_id: str
    status: str
    exit_code: int | None = None
    message: str = ""
    log: list[str] = field(default_factory=list)
    # report outputs: the job's standard output in full (capped), for operators whose answer is what they print
    output: str = ""
    # model-files outputs: [{"path": "crd/status/x.yaml", "content": "..."}], for the caller to deliver as a change
    files: list[dict] = field(default_factory=list)
    # artifact outputs: paths (relative to the artifact directory) the run left there
    artifacts: list[str] = field(default_factory=list)
    started_at: str = ""
    finished_at: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


def contract_dir(repo: str | Path, operators_dir: str | Path | None = None) -> Path:
    """The model's `operators/` folder: the contract (schemas) and the generators the model itself declares."""
    return Path(operators_dir) if operators_dir else Path(repo) / "operators"


def load_operator(name: str, operators_dir: Path) -> dict:
    """The manifest comes from the model's `operators/` (generators) or, failing that, from this package's own `manifests/`
    (observers); both are checked against the schema the model publishes."""
    path = next((d / name / "operator.yaml" for d in (operators_dir, OWN_MANIFESTS) if (d / name / "operator.yaml").is_file()), None)
    if path is None:
        raise JobError(f"There is no operator called {name!r}.")
    doc = yaml.safe_load(path.read_text())
    schema = json.loads((operators_dir / "operator.schema.json").read_text())
    errors = [e.message for e in Draft7Validator(schema).iter_errors(doc)]
    if errors:
        raise JobError(f"Operator {name} has an invalid manifest: {errors[0]}")
    if doc["status"] != "available":
        raise JobError(f"Operator {name} is only planned; it cannot run yet.")
    return doc


def validate_request(request: dict, operators_dir: Path) -> None:
    schema = json.loads((operators_dir / "jobrun.schema.json").read_text())
    errors = sorted(Draft7Validator(schema).iter_errors(request), key=lambda e: list(e.absolute_path))
    if errors:
        where = "/".join(map(str, errors[0].absolute_path)) or "request"
        raise JobError(f"Invalid job request ({where}): {errors[0].message}")


def _coerce(spec: dict, value):
    kind, name = spec["type"], spec["name"]
    ok = {
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "boolean": isinstance(value, bool),
        "list": isinstance(value, list) and all(isinstance(v, (str, int)) and not isinstance(v, bool) for v in value),
    }[kind]
    if not ok:
        raise JobError(f"Parameter {name} must be a {kind}.")
    return value


def bind_parameters(operator: dict, given: dict | None) -> dict:
    given = dict(given or {})
    specs = {p["name"]: p for p in operator.get("parameters", [])}
    unknown = sorted(set(given) - set(specs))
    if unknown:
        raise JobError(f"{operator['name']} has no parameter {unknown[0]!r}; it takes: {', '.join(specs) or 'none'}.")
    bound = {}
    for name, spec in specs.items():
        if name in given:
            bound[name] = _coerce(spec, given[name])
        elif "default" in spec:
            bound[name] = spec["default"]
        elif spec.get("required"):
            raise JobError(f"Parameter {name} is required.")
    return bound


def _text(value) -> str:
    return "true" if value is True else "false" if value is False else str(value)


def render_command(operator: dict, given: dict | None = None) -> list[str]:
    """The entrypoint with parameters bound. A bare `{list}` expands to one argument per item (none when empty);
    a placeholder inside a longer word is replaced by the value, a list there is refused, and a word that mentions a
    parameter that has no value (optional, no default) is left out, so `--source={source}` vanishes when there is no source."""
    bound = bind_parameters(operator, given)
    out: list[str] = []
    for word in operator["entrypoint"]["command"]:
        if word.startswith("{") and word.endswith("}") and word[1:-1] in {p["name"] for p in operator.get("parameters", [])}:
            value = bound.get(word[1:-1])
            if value is None:
                continue
            out.extend(_text(v) for v in value) if isinstance(value, list) else out.append(_text(value))
            continue
        unbound = [p["name"] for p in operator.get("parameters", []) if p["name"] not in bound and _PLACEHOLDER % p["name"] in word]
        if unbound:
            continue
        for name, value in bound.items():
            if _PLACEHOLDER % name in word:
                if isinstance(value, list):
                    raise JobError(f"List parameter {name} must stand alone as an argument.")
                word = word.replace(_PLACEHOLDER % name, _text(value))
        out.append(word)
    return out


def check_capabilities(operator: dict, capabilities: list[str] | set[str]) -> None:
    """Refuse before starting when the environment cannot do what the operator needs."""
    have = set(capabilities)
    missing = sorted(set(operator.get("requires", [])) - have)
    if missing:
        raise JobError(f"{operator['name']} needs {', '.join(missing)}, which this environment does not have.")
    any_of = set(operator.get("requires_any", []))
    if any_of and not (any_of & have):
        raise JobError(f"{operator['name']} needs one of {', '.join(sorted(any_of))}; this environment has none.")

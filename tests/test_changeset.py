"""The tracker: which operators a change between two revisions makes due."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from dm_operators.changeset import changed_paths, matches, plan  # noqa: E402
from dm_operators.runner import JobError  # noqa: E402


def git(cwd, *a):
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t", "PATH": __import__("os").environ["PATH"], "HOME": str(cwd)}
    return subprocess.run(["git", *a], cwd=cwd, env=env, check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def model(tmp_path):
    """A tiny model: a contract folder is read from contract/ (a snapshot), the repo only needs files to change."""
    repo = tmp_path / "m"
    for f in ("crd/assets/equity/a.yaml", "crd/products/p.yaml", "docs/readme.md", "product-tests/t.txt"):
        (repo / f).parent.mkdir(parents=True, exist_ok=True)
        (repo / f).write_text("v1\n")
    git(tmp_path, "init", "-q", str(repo)); git(repo, "add", "-A"); git(repo, "commit", "-q", "-m", "one")
    return repo


def commit(repo, **files):
    for f, body in files.items():
        (repo / f.replace("__", "/")).write_text(body)
    git(repo, "add", "-A"); git(repo, "commit", "-q", "-m", "change")
    return git(repo, "rev-parse", "HEAD")


def run_plan(repo, base, head="HEAD", **kw):
    return plan(repo, head, base, environment="cluster", model_repo="x/y", operators_dir=ROOT / "contract", now="2026-10-19T10:00:00Z", **kw)


def test_globs_cross_folders_and_a_bare_name_covers_a_whole_folder():
    assert matches("crd/assets/equity/a.yaml", "crd/assets/**") and not matches("docs/x.md", "crd/**")
    assert matches("product-tests/suite/t.py", "product-tests") and not matches("product-tests-old/t.py", "product-tests")


def test_changed_paths_between_two_revisions_or_everything_without_a_base(model):
    first = git(model, "rev-parse", "HEAD")
    (model / "crd/products/p.yaml").write_text("v2\n")
    second = commit(model)
    assert changed_paths(model, second, first) == ["crd/products/p.yaml"]
    assert "docs/readme.md" in changed_paths(model, second)


def test_an_asset_edit_asks_for_the_observer_with_just_that_asset_and_the_generators_for_the_model(model):
    base = git(model, "rev-parse", "HEAD")
    (model / "crd/assets/equity/a.yaml").write_text("v2\n")
    head = commit(model)
    out = run_plan(model, base)
    names = {d["operator"]: d for d in out["due"]}
    assert names["observe-materialisations"]["request"]["parameters"] == {"assets": ["crd/assets/equity/a.yaml"]}
    assert names["observe-materialisations"]["request"]["deliver"] == {"mode": "pull-request"}
    assert {"build-sql", "build-pdf", "build-docs-markdown"} <= set(names)
    assert names["build-pdf"]["release"] == [{"store": "artifactory", "path": "generic-local/data-model/docs-pdf"}]
    assert names["build-pdf"]["request"]["model"]["sha"] == head and names["build-pdf"]["request"]["deliver"]["mode"] == "artifact"
    assert "observe-test-suites" not in names, "no product-tests change"


def test_a_change_that_matters_to_nothing_asks_for_nothing(model):
    base = git(model, "rev-parse", "HEAD")
    (model / "docs/readme.md").write_text("v2\n")
    commit(model)
    assert run_plan(model, base)["due"] == []


def test_what_the_environment_cannot_run_is_reported_not_planned(model):
    out = run_plan(model, None, capabilities=["snowflake"])
    assert {d["operator"] for d in out["due"]} >= {"observe-materialisations", "build-sql"}
    skipped = {s["operator"]: s["because"] for s in out["skipped"]}
    assert "chrome" in skipped["build-pdf"] and "product-tests" in skipped["observe-test-suites"]


def test_cli_prints_the_plan(model):
    done = subprocess.run([sys.executable, "-m", "dm_operators.changeset", "plan", "--repo", str(model), "--operators-dir", str(ROOT / "contract"), "--environment", "cluster"],
                          cwd=ROOT, capture_output=True, text=True)
    assert done.returncode == 0 and json.loads(done.stdout)["due"]

# bmll-data-model-operators

Runs jobs for the BMLL data model, in two parts. It knows nothing about who asks (the UI, a schedule, CI) or where it runs.

| Repository | Is |
|---|---|
| [`data-model`](https://github.com/bmlltech/data-model) | The data: declarations, schemas, validation. It also owns the **contract** (`operators/*.schema.json`) and the **generators**: scripts under `scripts/` with a manifest under `operators/`, so its own CI can run them too. |
| [`data-model-ui`](https://github.com/bnouvelbmll/data-model-ui) | Shows and edits the model; asks for runs. |
| **this one** | The runner, the Kubernetes controller, the observers, the change tracker. |

## 1. Observers: bring status in from other systems

Code here, manifest in `manifests/`. An observer reads a system and writes `crd/status/` records in the model (delivered as a change, never pushed
straight to the model).

| Operator | Reads | Writes |
|---|---|---|
| `observe-materialisations` | Snowflake, Databricks (`dm_operators/observers/materialisations.py`) | `crd/status/*.yaml` |
| `coverage-map` *(planned)* | Snowflake, Databricks, the Lab | `dist/coverage/**` |
| Jira, Revelate, the Lab *(to do)* | | |

An observer runs in a checkout of the model (it is its working directory, or `DATAMODEL_ROOT`) with only the credentials its manifest lists.

## 2. Tracking changes and regenerating outputs

The model's generators (`build-sql`, `build-pdf`, `build-docs-markdown`, `observe-test-suites`, `report-sql-drift`) live in the model; this repository
runs them at a pinned revision, the same scripts its CI runs. Each manifest declares `triggers` (what makes the output stale) and, for artifacts, the
`release` (where the official copy is published). The tracker turns a change into the runs that are due:

```bash
python3 -m dm_operators.changeset plan --repo ../data-model --base origin/master --head HEAD --environment cluster --capabilities snowflake,databricks,chrome
```

It prints the operators due with ready job-run requests (an edited asset becomes the `assets` parameter of `observe-materialisations`), the release
location of each artifact, and what the environment cannot run. It only reads git.

## Run one

```bash
echo '{"id":"run-local-0001","operator":"build-sql","model":{"repo":"bmlltech/data-model","sha":"'$(git -C ../data-model rev-parse HEAD)'"},
       "environment":"local","deliver":{"mode":"artifact"},"requested_by":"me","requested_at":"2026-10-19T10:00:00Z"}' \
  | python3 -m dm_operators.runner run - --repo ../data-model --artifacts /tmp/out
```

The runner checks the request against the model's `operators/jobrun.schema.json`, runs the operator in a throwaway `git worktree` of `model.sha`
with only the credentials the manifest declares, and prints a result (`status`, `log`, `files`, `artifacts`, `output`). Manifests are found in the
model's `operators/` (`--operators-dir`, default `REPO/operators`) and here in `manifests/`. Exit status 2 means the request could not be run.

## On Kubernetes: `JobRun`

`deploy/helm/datamodel-operators` installs a [kopf](https://kopf.readthedocs.io) controller and the `JobRun` resource (`apiVersion:
datamodel.bmll.io/v1alpha1`): `spec` is a job-run request. The controller refuses what cannot run (unknown environment, missing capability, invalid
request) with the reason in `status.message`, otherwise creates a Job that clones the model, checks out the sha and runs the runner. `status` carries the
phase, a log tail and the files and artifacts reported; the full result is the ConfigMap `jobrun-<name>-result`. To try it on kind use the
harness of `data-model-ui` (`deploy/local/run.sh operators`, then `operators-test`), pointing `OPERATORS_PATH` at this checkout.

The image carries a copy of the model's contract (`contract/`) to refuse early. Refresh it with `scripts/sync_contract.sh ../data-model` whenever the
contract changes; the Job itself uses the model's own `operators/` at the pinned sha.

## Develop

```bash
pip install -e '.[controller,test]'
DATAMODEL_PATH=../data-model python3 -m pytest      # the observer's tests need a checkout of the model; without one they are skipped
```

`environments.example.yaml` shows the shape of a deployment's environment registry (the real one lives in the deployment's overrides repository).
Design notes (executors, GitOps with Flux, `ModelChangeSet`): `data-model/docs/architecture.md`.

"""kopf handlers: a `JobRun` becomes a Job, and the Job's outcome becomes the JobRun's status.

    kopf run --standalone -n NAMESPACE -m operators.controller.main

Settings come from the environment: ENVIRONMENTS_FILE (the registry, default /etc/datamodel/environments.yaml), DEFAULT_IMAGE,
MODEL_REPO_URL (where Jobs clone the model from), JOB_TTL_SECONDS.
"""

from __future__ import annotations

import os

import kopf
import yaml
from kubernetes import client, config

from operators.runner import JobError

from .logic import GROUP, LABEL, PLURAL, VERSION, build_job, job_name, parse_result, phase_of, plan, result_configmap, summarise

REGISTRY_FILE = os.environ.get("ENVIRONMENTS_FILE", "/etc/datamodel/environments.yaml")


def _registry() -> dict:
    try:
        with open(REGISTRY_FILE) as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    # Without this kopf lists CRDs and namespaces cluster-wide to discover resources; the Role is namespaced on purpose.
    settings.scanning.disabled = True
    settings.posting.level = 30  # warnings and errors only as Events
    settings.persistence.finalizer = f"{GROUP}/cleanup"


@kopf.on.create(GROUP, VERSION, PLURAL)
def create(spec, name, namespace, body, patch, logger, **_):
    created = (body.get("metadata") or {}).get("creationTimestamp")
    try:
        request, operator, env = plan(name, dict(spec), created, _registry())
    except JobError as e:
        logger.warning("refused %s: %s", name, e)
        patch.status.update({"phase": "Refused", "message": str(e)})
        return
    job = build_job(request, operator, env, namespace=namespace, default_image=os.environ["DEFAULT_IMAGE"],
                    default_repo_url=os.environ.get("MODEL_REPO_URL", ""), ttl_seconds=int(os.environ.get("JOB_TTL_SECONDS", "3600")))
    kopf.adopt(job)  # owned by the JobRun: deleting one deletes the other
    try:
        client.BatchV1Api().create_namespaced_job(namespace, job)
    except client.ApiException as e:
        if e.status != 409:  # already there: a retry after a restart
            raise kopf.TemporaryError(f"could not create the Job: {e.reason}", delay=10)
    patch.status.update({"phase": "Running", "job": job_name(name), "message": ""})


@kopf.timer(GROUP, VERSION, PLURAL, interval=3.0, idle=3.0, when=lambda status, **_: (status or {}).get("phase") == "Running")
def follow(name, namespace, body, patch, logger, **_):
    try:
        job = client.BatchV1Api().read_namespaced_job_status(job_name(name), namespace)
    except client.ApiException as e:
        if e.status == 404:  # removed behind our back (TTL, manual delete)
            patch.status.update({"phase": "Failed", "message": "The Job disappeared before it finished."})
        return
    phase = phase_of(job.status.to_dict() if job.status else None)
    if phase == "Running":
        return
    core = client.CoreV1Api()
    pods = core.list_namespaced_pod(namespace, label_selector=f"{LABEL}={name}").items
    log = ""
    for pod in sorted(pods, key=lambda p: p.metadata.creation_timestamp or 0, reverse=True)[:1]:
        for container in ("run", "checkout"):
            try:
                log = core.read_namespaced_pod_log(pod.metadata.name, namespace, container=container)
            except client.ApiException:
                continue
            if log:
                break
    result = parse_result(log)
    summary = summarise(result, phase, log)
    if result is not None:
        cm = result_configmap(name, namespace, result)
        cm["metadata"]["ownerReferences"] = [{"apiVersion": f"{GROUP}/{VERSION}", "kind": "JobRun", "name": name, "uid": body["metadata"]["uid"], "controller": True}]
        try:
            core.create_namespaced_config_map(namespace, cm)
        except client.ApiException as e:
            if e.status != 409:
                logger.warning("could not store the result: %s", e.reason)
        summary["resultConfigMap"] = cm["metadata"]["name"]
    patch.status.update(summary)
    logger.info("%s: %s", name, summary["phase"])


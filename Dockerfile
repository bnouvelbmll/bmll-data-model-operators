# The image Jobs and the controller run from.
#   docker build -t bmll-datamodel-operators .
#   docker build --build-arg EXTRA_PIP="snowflake-connector-python databricks-sql-connector" ...   (observers)
# It holds the runner and the controller, not the model: a Job checks the model out at the sha it was asked to run, and
# takes the scripts and manifests from there. The packages below are what the operators' scripts import.
FROM python:3.12-slim
ARG EXTRA_PIP=""
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir kopf kubernetes pyyaml jsonschema jinja2 packaging $EXTRA_PIP
WORKDIR /app
COPY dm_operators/ /app/dm_operators/
COPY manifests/ /app/manifests/
COPY contract/ /app/contract/
RUN useradd -u 1000 -m runner && chown -R runner /app
USER runner
ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1

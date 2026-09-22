# dbt runner for the gold layer. Kept separate from the Airflow image:
# dbt is a one-shot job, and a slim image means `make dbt-build` starts
# in seconds instead of building the whole Airflow stack.
FROM python:3.11-slim

# ~=1.8.0 pins the 1.8.x minor line. dbt-core is pinned explicitly:
# dbt-postgres's own dbt-core dependency is loose, and pip will happily
# resolve it to 2.0.x where the postgres adapter is "experimental".
RUN pip install --no-cache-dir "dbt-core~=1.8.0" "dbt-postgres~=1.8.0"

# Opt out of dbt's anonymous usage tracking.
ENV DO_NOT_TRACK=1

WORKDIR /dbt

ENTRYPOINT ["dbt"]

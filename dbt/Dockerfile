# dbt for GH Archive (Step 6.4): the dbt project + the exact dbt versions used on the laptop.
#
# Build (from repo root):   docker build -t gharchive-dbt:local dbt
# Run locally (dev target): docker run --rm -v ~/.aws:/root/.aws:ro gharchive-dbt:local build --target dev
# On Fargate the task definition passes:  build --target prod   (credentials = ECS task role)
FROM python:3.11-slim

# Python prints straight to the log (no buffering); no .pyc cache files;
# dbt finds profiles.yml in /app (the project folder).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app

# git: dbt debug checks for it (and dbt deps would need it). The slim image doesn't include it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Same versions as the laptop's .venv, so "works on my laptop" = "works in the container".
RUN pip install --no-cache-dir \
    dbt-core==1.12.6 \
    dbt-athena==1.11.1 \
    dbt-adapters==1.24.5 \
    dbt-common==1.39.0

WORKDIR /app
COPY . /app

# Install dbt packages only if the project uses any.
RUN if [ -f packages.yml ]; then dbt deps; fi

# Every command is a dbt command: `docker run <image> build --target dev` runs `dbt build --target dev`.
ENTRYPOINT ["dbt"]
# Default when no command is given: a harmless self-check. Prod is never the default.
CMD ["debug"]
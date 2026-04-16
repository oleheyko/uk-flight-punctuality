FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/root/.dbt

RUN mkdir -p /root/.dbt

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        curl \
        ca-certificates \
    && python -m pip install --upgrade pip \
    && pip install --no-cache-dir "dbt-bigquery==1.11.1" \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/app
COPY profiles.yml /root/.dbt/profiles.yml
COPY . /usr/app

ENTRYPOINT ["dbt"]

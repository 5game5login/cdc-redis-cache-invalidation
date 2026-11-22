FROM public.ecr.aws/docker/library/python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "confluent-kafka>=2.6,<3" "redis>=5.0" "psycopg[binary]>=3.2,<4"
WORKDIR /app
COPY worker ./worker
COPY bench_lib.py bench_node.py soak_node.py stage_probe.py connector_watchdog.py init_db.py ./
RUN useradd --uid 10001 --create-home cdc
USER cdc
ENV PYTHONUNBUFFERED=1
HEALTHCHECK NONE

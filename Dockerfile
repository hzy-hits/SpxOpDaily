FROM ghcr.io/astral-sh/uv:0.11.25 AS uv
FROM ubuntu:24.04 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 ca-certificates tzdata libgomp1 libssl3t64 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src/ src/
COPY config/ config/
COPY alembic.ini ./
COPY alembic/ alembic/
COPY scripts/ scripts/
RUN uv venv --python /usr/bin/python3.12 --relocatable .venv \
    && uv sync --frozen --no-dev --no-editable --python /usr/bin/python3.12 \
    && uv cache clean \
    && cp --remove-destination /usr/bin/python3.12 .venv/bin/python
COPY out/bin/ bin/
ARG REVISION
LABEL org.opencontainers.image.source="https://github.com/hzy-hits/SpxOpDaily" \
      org.opencontainers.image.revision="${REVISION}"
ENV PATH="/app/.venv/bin:/usr/local/bin:/usr/bin:/bin" \
    PYTHONPATH="/app/src" PYTHONDONTWRITEBYTECODE=1 UV_NO_SYNC=1 UV_OFFLINE=1 \
    SPX_SPARK_GIT_SHA="${REVISION}" SPX_SPARK_DISABLE_RUNTIME_OVERRIDES=1
RUN printf '%s\n' "$REVISION" > REVISION \
    && python -c 'import spx_spark, scipy, duckdb; assert spx_spark.__file__.startswith("/app/src/")' \
    && spx --help >/dev/null \
    && python -c 'from pathlib import Path; from spx_spark.settings import load_app_settings; load_app_settings(); assert Path("config/growth_dislocation_universe.csv").is_file()' \
    && /usr/sbin/useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin spx
USER 10001:10001
ENTRYPOINT ["/app/.venv/bin/spx"]
CMD ["--help"]

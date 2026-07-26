# Two images from one file. Build context is the repo root.
#   runtime — the daemon + CLI only; published to ghcr.io on release tags.
#   demo    — runtime plus the demo scripts (see demo/docker-compose.yml).
FROM python:3.12-slim AS runtime

# Install the package first so demo-script edits do not bust the pip layer.
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY walflux/ walflux/
RUN pip install --no-cache-dir .
WORKDIR /

LABEL org.opencontainers.image.source=https://github.com/sedai77/walflux-postgres-incremental-views
LABEL org.opencontainers.image.description="Millisecond-fresh materialized views for Postgres via logical replication"
LABEL org.opencontainers.image.licenses=MIT

# No default config baked in: mount yours and run e.g.
#   walflux run -c /etc/walflux.yaml
CMD ["walflux", "--help"]

FROM runtime AS demo
COPY demo/ /demo/
WORKDIR /demo
CMD ["walflux", "run", "-c", "/demo/config.yaml"]

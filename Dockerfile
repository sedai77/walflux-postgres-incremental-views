# WalFlux demo image: the daemon, the CLI, and the demo scripts in one image.
# Build context is the repo root (see demo/docker-compose.yml).
FROM python:3.12-slim

# Install the package first so demo-script edits do not bust the pip layer.
WORKDIR /src
COPY pyproject.toml README.md ./
COPY walflux/ walflux/
RUN pip install --no-cache-dir .

COPY demo/ /demo/
WORKDIR /demo

CMD ["walflux", "run", "-c", "/demo/config.yaml"]

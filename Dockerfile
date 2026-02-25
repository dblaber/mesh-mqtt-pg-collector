FROM python:3.13-slim

# Install build deps for psycopg2 (the binary wheel usually covers this,
# but having libpq-dev makes it more robust across architectures).
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml README.md LICENSE ./
COPY src/ src/

# Install the project and dependencies
RUN uv pip install --system .

# Default config location inside the container
ENV COLLECTOR_CONFIG_FILE=/app/config.yaml

CMD ["python", "-m", "collector"]

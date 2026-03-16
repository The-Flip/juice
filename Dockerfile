FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

WORKDIR /app

# Install dependencies (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy source
COPY juice/ juice/

# Install the project itself
RUN uv sync --frozen --no-dev

CMD ["uv", "run", "juice", "record", "--db", "/data/juice.duckdb"]

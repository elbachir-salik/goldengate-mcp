# GoldenGate MCP Server
# Multi-stage build — keeps the final image lean.
#
# Build:
#   docker build -t goldengate-mcp .
#
# Run (stdio transport — for Claude Desktop / MCP clients):
#   docker run --env-file .env goldengate-mcp
#
# Run (HTTP transport — for testing / multi-client):
#   docker run --env-file .env -p 8000:8000 goldengate-mcp fastmcp run src/server.py --transport streamable-http --host 0.0.0.0 --port 8000

# ------------------------------------------------------------------
# Stage 1 — builder
# ------------------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build deps
RUN pip install --upgrade pip hatchling

# Copy dependency spec first (layer cache)
COPY pyproject.toml ./

# Install all runtime dependencies into a prefix we can copy.
# python-oracledb is included here for standard PyPI setups.
# If your corporate mirror does not carry it, override the index:
#   docker build --build-arg ORACLEDB_INDEX=https://your-mirror/simple ...
# and adjust the RUN line below accordingly.
ARG ORACLEDB_INDEX=https://pypi.org/simple
RUN pip install --prefix=/install \
    fastmcp \
    pydantic \
    pydantic-settings \
    structlog \
    PyYAML \
    anthropic \
    httpx \
    tenacity \
    confluent-kafka && \
    pip install --prefix=/install --index-url "${ORACLEDB_INDEX}" python-oracledb

# ------------------------------------------------------------------
# Stage 2 — runtime
# ------------------------------------------------------------------
FROM python:3.12-slim AS runtime

# Non-root user for security
RUN useradd --create-home --shell /bin/bash mcpuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY src/ src/
COPY pyproject.toml ./

# Default env — override with --env-file .env at runtime
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SCHEMA_MAP_PATH=src/schema/schema_map.yaml \
    AUDIT_LOG_MODE=file \
    AUDIT_LOG_FILE_PATH=/app/audit.log

# Expose port for HTTP transport (not used in stdio mode)
EXPOSE 8000

USER mcpuser

# Default: stdio transport (Claude Desktop compatible)
CMD ["python", "-m", "fastmcp", "run", "src/server.py"]

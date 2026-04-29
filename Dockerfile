FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY asomobile_mcp ./asomobile_mcp

RUN pip install --no-cache-dir .

# stdio MCP server — no port, no healthcheck
ENTRYPOINT ["python", "-m", "asomobile_mcp"]

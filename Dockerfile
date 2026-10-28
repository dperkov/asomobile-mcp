FROM python:3.14-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY asomobile_mcp ./asomobile_mcp

RUN pip install --no-cache-dir .

# Drop root. The server only reads stdin, writes stdout/stderr and makes outbound HTTPS calls --
# it never writes to the filesystem -- so it has no reason to run privileged.
RUN useradd --create-home --uid 10001 asomobile
USER asomobile

# stdio MCP server — no port, no healthcheck
ENTRYPOINT ["python", "-m", "asomobile_mcp"]

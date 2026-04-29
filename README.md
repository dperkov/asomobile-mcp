# ASOMobile MCP Server

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![MCP](https://img.shields.io/badge/MCP-Compatible-purple.svg)](https://modelcontextprotocol.io)
[![ASOMobile](https://img.shields.io/badge/API-ASOMobile-green.svg)](https://asomobile.net)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


A [Model Context Protocol](https://modelcontextprotocol.io) server that lets Claude (or any MCP client) query the [ASOMobile](https://asomobile.net) public API for real, paid ASO data: keyword traffic, competition, KEI, app rankings, organic downloads, and more.

API reference: https://asomobile.apidog.io

## What it gives you

Tools exposed to the LLM:

| Tool | What it does |
|---|---|
| `app_profile` | Full app metadata (title, subtitle, description, screenshots, ratings) |
| `app_competitors` | ASOMobile-curated competitor list for an app |
| `app_keywords` | Every keyword the app is indexed for, with traffic/CI/KEI/rank/ASA |
| `app_keywords_multiple_countries` | Same but across many countries at once |
| `keyword_check` | Deep dive on a single search query (volume, competition, top apps) |
| `keyword_suggest` | Related/expanded keyword suggestions |
| `keyword_rank` | Historical rank of an app for a keyword |
| `keyword_monitor` | Pull tracked keywords from your ASOMobile dashboard |
| `world_wide_check` | Compare a keyword across countries |
| `app_ranking` | Top-charts (free / paid / grossing) by country & category |
| `organic_downloads` | Estimated organic installs broken down by keyword |
| `keyword_gap` | Composite tool: gap analysis between two apps |

## Install

Requires Python 3.10+.

```bash
git clone <this-folder>
cd asomobile-mcp
pip install -e .
```

Or with `uv`:

```bash
uv pip install -e .
```

## Configure

Set your API token (get it at https://app.asomobile.net/api-dashboard):

```bash
export ASOMOBILE_API_TOKEN="your-token-here"
```

⚠️ **Never commit the token to git or share it in chat.** Use a secret manager or `.env` file (and `.gitignore` it).

## Run

Standalone (stdio mode, the way MCP clients launch it):

```bash
python -m asomobile_mcp
```

Or via the installed entrypoint:

```bash
asomobile-mcp
```

The process speaks MCP over stdio and waits for a client.

## Hook it up to Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "asomobile": {
      "command": "python",
      "args": ["-m", "asomobile_mcp"],
      "env": {
        "ASOMOBILE_API_TOKEN": "your-token-here"
      }
    }
  }
}
```

If `python` isn't on your PATH or you want to use a venv, use the absolute path: `/Users/you/.venvs/aso/bin/python` etc.

Restart Claude Desktop. The tools should appear in the tool picker.

## Hook it up to Claude Code (CLI)

```bash
claude mcp add asomobile -- python -m asomobile_mcp
```

Then set `ASOMOBILE_API_TOKEN` in your shell profile so it's inherited.

## Docker (optional)

```bash
docker build -t asomobile-mcp .
docker run --rm -i -e ASOMOBILE_API_TOKEN=$ASOMOBILE_API_TOKEN asomobile-mcp
```

For Claude Desktop with Docker:

```json
{
  "mcpServers": {
    "asomobile": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-e", "ASOMOBILE_API_TOKEN",
        "asomobile-mcp"
      ],
      "env": {
        "ASOMOBILE_API_TOKEN": "your-token-here"
      }
    }
  }
}
```

## How it works

ASOMobile uses an async two-step pattern: you `GET /endpoint/` to enqueue a job (returns `ticket_id`), then `GET /endpoint/result?ticket_id=…` repeatedly until the job is done. This server hides that polling — every tool returns the final data synchronously. Default timeout: 2 minutes per call (40 polls × 3 seconds), tunable in `server.py`.

## Test it manually

Quick sanity check without Claude:

```python
import os, asyncio
os.environ["ASOMOBILE_API_TOKEN"] = "your-token"

from asomobile_mcp.server import keyword_check
print(keyword_check("samsung tv remote", country="US"))
```

## Costs

Each tool call hits the ASOMobile API and consumes credits from your plan. The composite `keyword_gap` tool makes **two** `app_keywords` calls. Watch your dashboard.

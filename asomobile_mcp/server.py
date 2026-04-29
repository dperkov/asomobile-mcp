"""
ASOMobile MCP Server (v0.2 — verified against the official OpenAPI spec)
========================================================================

A Model Context Protocol server exposing the ASOMobile public API.

Verified against ASOMobile_openapi.json (the official spec).
All endpoint paths, methods, and parameters match the spec exactly.

Auth: Bearer token via ASOMOBILE_API_TOKEN env var.
Base URL: https://app.asomobile.net/asomobile-public-api
"""

from __future__ import annotations

import os
import time
from datetime import date, timedelta
from typing import Any, Literal

import httpx
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE = "https://app.asomobile.net/asomobile-public-api"
TOKEN = os.environ.get("ASOMOBILE_API_TOKEN", "").strip()

# Polling configuration. /app-keywords/ on a real app with 100+ keywords
# typically takes 60-120s, so we need a generous timeout.
POLL_INTERVAL_SEC = 3
POLL_MAX_ATTEMPTS = 60  # ~3 minutes total
HTTP_TIMEOUT_SEC = 30

mcp = FastMCP("asomobile")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _headers() -> dict[str, str]:
    if not TOKEN:
        raise RuntimeError(
            "ASOMOBILE_API_TOKEN is not configured. "
            "Set it in your environment or MCP client config."
        )
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _get(path: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{API_BASE}{path}"
    clean = {k: v for k, v in params.items() if v is not None}
    with httpx.Client(timeout=HTTP_TIMEOUT_SEC) as client:
        resp = client.get(url, params=clean, headers=_headers())
    if resp.status_code >= 400:
        raise RuntimeError(
            f"ASOMobile API error {resp.status_code} on GET {path}: {resp.text[:500]}"
        )
    return resp.json()


def _post(path: str, json_body: dict[str, Any]) -> dict[str, Any]:
    url = f"{API_BASE}{path}"
    clean = {k: v for k, v in json_body.items() if v is not None}
    with httpx.Client(timeout=HTTP_TIMEOUT_SEC) as client:
        resp = client.post(url, json=clean, headers=_headers())
    if resp.status_code >= 400:
        raise RuntimeError(
            f"ASOMobile API error {resp.status_code} on POST {path}: {resp.text[:500]}"
        )
    return resp.json()


def _extract_ticket_id(payload: dict[str, Any]) -> str:
    """
    ASOMobile envelope: {code: 200|201, data: {ticket_id: 12345}, message: ...}
    Per the OpenAPI spec, request endpoints return code 200 or 201
    with a ticket_id in data.
    """
    data = payload.get("data") or {}
    ticket_id = data.get("ticket_id") if isinstance(data, dict) else None
    if ticket_id is None:
        raise RuntimeError(f"No ticket_id in response: {payload!r}")
    return str(ticket_id)


def _poll(result_path: str, ticket_id: str) -> dict[str, Any]:
    """
    Poll /…/result. Done = code 200 AND non-empty data.
    Anything else = still processing; sleep and retry.

    Note: keyword-monitor/result returns a top-level array, not envelope.
    """
    last: Any = None
    for _ in range(POLL_MAX_ATTEMPTS):
        last = _get(result_path, {"ticket_id": ticket_id})
        if isinstance(last, list):
            return {"data": last}
        if isinstance(last, dict):
            code = last.get("code")
            data = last.get("data")
            if code in (200, "200") and data not in (None, {}, []):
                return last
        time.sleep(POLL_INTERVAL_SEC)
    raise TimeoutError(
        f"Polling {result_path} timed out after "
        f"{POLL_INTERVAL_SEC * POLL_MAX_ATTEMPTS}s. Last: {last!r}"
    )


def _two_step(
    request_path: str,
    result_path: str,
    payload: dict[str, Any],
    method: Literal["GET", "POST"] = "GET",
) -> dict[str, Any]:
    first = _get(request_path, payload) if method == "GET" else _post(request_path, payload)
    ticket_id = _extract_ticket_id(first)
    return _poll(result_path, ticket_id)


def _normalize_app_id(raw: str) -> str:
    """
    iOS IDs in ASOMobile use the form 'id12345'. Bare numerics get the prefix;
    Android bundles (com.foo.bar) pass through.
    """
    raw = (raw or "").strip()
    if raw.startswith("id"):
        return raw
    if raw.isdigit():
        return f"id{raw}"
    return raw


# ---------------------------------------------------------------------------
# Apps  (paths verified: /apps/app/profile, /apps/competitors/)
# ---------------------------------------------------------------------------


@mcp.tool()
def app_profile(
    app_id: str,
    country: str = "US",
    platform: Literal["IOS", "ANDROID"] = "IOS",
) -> dict[str, Any]:
    """
    Full app metadata: title, subtitle, description, icon, screenshots,
    rating, version, etc.

    Args:
        app_id: 'id6476985447', '6476985447', or 'com.foo.bar'.
        country: ISO code, e.g. 'US'.
        platform: 'IOS' or 'ANDROID'.
    """
    return _two_step(
        "/apps/app/profile",
        "/apps/app/profile/result",
        {
            "app_id": _normalize_app_id(app_id),
            "country": country.upper(),
            "platform": platform,
        },
    )


@mcp.tool()
def app_competitors(
    app_id: str,
    country: str = "US",
    platform: Literal["IOS", "ANDROID"] = "IOS",
) -> dict[str, Any]:
    """ASOMobile-curated competitor list for an app in a country."""
    return _two_step(
        "/apps/competitors/",
        "/apps/competitors/result",
        {
            "app_id": _normalize_app_id(app_id),
            "country": country.upper(),
            "platform": platform,
        },
    )


# ---------------------------------------------------------------------------
# App Keywords
# ---------------------------------------------------------------------------


@mcp.tool()
def app_keywords(
    app_id: str,
    country: str = "US",
    platform: Literal["IOS", "ANDROID"] = "IOS",
    ios_device: Literal["IPHONE", "IPAD"] = "IPHONE",
) -> dict[str, Any]:
    """
    All keywords the app is indexed for, with per-keyword metrics:
    - traffic.value: estimated daily search volume
    - ci.value: competition index (lower = easier)
    - kei.value: keyword effectiveness (traffic / competition)
    - rank.value: current rank for this keyword
    - asa: Apple Search Ads popularity (iOS only)
    """
    return _two_step(
        "/app-keywords/",
        "/app-keywords/result",
        {
            "app_id": _normalize_app_id(app_id),
            "country": country.upper(),
            "platform": platform,
            "ios_device": ios_device if platform == "IOS" else None,
        },
    )


@mcp.tool()
def app_keywords_multiple_countries(
    app_id: str,
    countries: list[str],
    platform: Literal["IOS", "ANDROID"] = "IOS",
    ios_device: Literal["IPHONE", "IPAD"] = "IPHONE",
) -> dict[str, Any]:
    """Same as app_keywords but across multiple countries (POST endpoint)."""
    return _two_step(
        "/app-keywords/multiple-countries",
        "/app-keywords/multiple-countries/result",
        {
            "app_id": _normalize_app_id(app_id),
            "countries": [c.upper() for c in countries],
            "platform": platform,
            "ios_device": ios_device if platform == "IOS" else None,
        },
        method="POST",
    )


# ---------------------------------------------------------------------------
# Keyword research
# ---------------------------------------------------------------------------


@mcp.tool()
def keyword_check(
    keyword: str,
    country: str = "US",
    platform: Literal["IOS", "ANDROID"] = "IOS",
    ios_device: Literal["IPHONE", "IPAD"] = "IPHONE",
) -> dict[str, Any]:
    """
    Deep dive on a single search query. Returns: traffic, ci, kei, asa,
    apps_count, suggestions[], top_apps[].
    """
    return _two_step(
        "/keyword-check/",
        "/keyword-check/result",
        {
            "keyword": keyword,
            "country": country.upper(),
            "platform": platform,
            "ios_device": ios_device if platform == "IOS" else None,
        },
    )


@mcp.tool()
def keyword_suggest(
    keywords: list[str],
    country: str = "US",
    platform: Literal["IOS", "ANDROID"] = "IOS",
    ios_device: Literal["IPHONE", "IPAD"] = "IPHONE",
) -> dict[str, Any]:
    """
    For each seed keyword, returns ASOMobile's expanded list of related
    queries with traffic estimates. POST endpoint.
    """
    return _two_step(
        "/keyword-suggest",
        "/keyword-suggest/result",
        {
            "keywords": keywords,
            "country": country.upper(),
            "platform": platform,
            "ios_device": ios_device if platform == "IOS" else None,
        },
        method="POST",
    )


@mcp.tool()
def keyword_rank(
    app_id: str,
    keyword: str,
    country: str = "US",
    platform: Literal["IOS", "ANDROID"] = "IOS",
    ios_device: Literal["IPHONE", "IPAD"] = "IPHONE",
    from_date: str | None = None,
    to_date: str | None = None,
    days_back: int = 30,
) -> dict[str, Any]:
    """
    Historical rank time series of an app for a keyword.

    NOTE: from_date and to_date are REQUIRED by the API. If you don't pass
    them, this tool defaults to the last `days_back` days (default 30).
    Date format: 'YYYY-MM-DD'.
    """
    if not from_date or not to_date:
        today = date.today()
        from_date = from_date or (today - timedelta(days=days_back)).isoformat()
        to_date = to_date or today.isoformat()
    return _two_step(
        "/keyword-rank/",
        "/keyword-rank/result",
        {
            "app_id": _normalize_app_id(app_id),
            "keyword": keyword,
            "country": country.upper(),
            "platform": platform,
            "ios_device": ios_device if platform == "IOS" else None,
            "from_date": from_date,
            "to_date": to_date,
        },
    )


@mcp.tool()
def keyword_monitor(
    app_id: str,
    country: str = "US",
    platform: Literal["IOS", "ANDROID"] = "IOS",
    ios_device: Literal["IPHONE", "IPAD"] = "IPHONE",
) -> dict[str, Any]:
    """
    Current ranks + metrics for keywords you've added to Keyword Monitor in
    your ASOMobile dashboard. Returns empty if nothing is tracked yet.
    """
    return _two_step(
        "/keyword-monitor/",
        "/keyword-monitor/result",
        {
            "app_id": _normalize_app_id(app_id),
            "country": country.upper(),
            "platform": platform,
            "ios_device": ios_device if platform == "IOS" else None,
        },
    )


@mcp.tool()
def world_wide_check(
    keyword: str,
    countries: list[str],
    platform: Literal["IOS", "ANDROID"] = "IOS",
    ios_device: Literal["IPHONE", "IPAD"] = "IPHONE",
) -> dict[str, Any]:
    """
    A single keyword's metrics across multiple countries (POST endpoint).
    Useful for prioritizing localization markets.
    """
    return _two_step(
        "/world-wide-check",
        "/world-wide-check/result",
        {
            "keyword": keyword,
            "countries": [c.upper() for c in countries],
            "platform": platform,
            "ios_device": ios_device if platform == "IOS" else None,
        },
        method="POST",
    )


# ---------------------------------------------------------------------------
# Rankings & organic
# ---------------------------------------------------------------------------


@mcp.tool()
def app_ranking(
    app_id: str,
    platform: Literal["IOS", "ANDROID"] = "IOS",
    ios_device: Literal["IPHONE", "IPAD"] = "IPHONE",
) -> dict[str, Any]:
    """
    Top-charts presence of a specific app across countries: free / paid /
    grossing categories.
    """
    return _two_step(
        "/app-ranking/",
        "/app-ranking/result",
        {
            "app_id": _normalize_app_id(app_id),
            "platform": platform,
            "ios_device": ios_device if platform == "IOS" else None,
        },
    )


@mcp.tool()
def organic_downloads(
    app_id: str,
    country: str = "US",
    platform: Literal["IOS", "ANDROID"] = "IOS",
    ios_device: Literal["IPHONE", "IPAD"] = "IPHONE",
) -> dict[str, Any]:
    """
    Estimated organic installs broken down by the keyword that drove them.
    Returns: {keywords: [{keyword, estimatedInstalls, installsShare,
    rank, asa, traffic}], totalInstalls}
    """
    return _two_step(
        "/organic-downloads",
        "/organic-downloads/result",
        {
            "app_id": _normalize_app_id(app_id),
            "country": country.upper(),
            "platform": platform,
            "ios_device": ios_device if platform == "IOS" else None,
        },
    )


# ---------------------------------------------------------------------------
# Composite — keyword gap analysis
# ---------------------------------------------------------------------------


@mcp.tool()
def keyword_gap(
    app_id_a: str,
    app_id_b: str,
    country: str = "US",
    platform: Literal["IOS", "ANDROID"] = "IOS",
    ios_device: Literal["IPHONE", "IPAD"] = "IPHONE",
    min_traffic: float = 0.0,
    top_n: int = 50,
) -> dict[str, Any]:
    """
    Real keyword gap analysis between two apps using ASOMobile data.

    Pulls full keyword sets for both apps, then returns:
    - opportunities: keywords B ranks for that A doesn't (sorted by traffic)
    - shared_top: keywords both rank for, with side-by-side ranks
    - only_a / only_b: unique keyword sets
    - totals + overlap %

    Args:
        app_id_a: "your" app
        app_id_b: competitor
        min_traffic: filter out keywords below this daily search volume
        top_n: max items per result section
    """
    a = app_keywords(app_id_a, country, platform, ios_device)
    b = app_keywords(app_id_b, country, platform, ios_device)

    def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    return v
        return []

    def _kw(r: dict[str, Any]) -> str:
        return (r.get("keyword") or "").strip().lower()

    def _num(r: dict[str, Any], field: str) -> float:
        v = r.get(field)
        if isinstance(v, dict):
            v = v.get("value")
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _rank(r: dict[str, Any]) -> int | None:
        rk = r.get("rank")
        if isinstance(rk, dict):
            rk = rk.get("value")
        try:
            return int(rk) if rk is not None else None
        except (TypeError, ValueError):
            return None

    rows_a = {_kw(r): r for r in _rows(a) if _kw(r)}
    rows_b = {_kw(r): r for r in _rows(b) if _kw(r)}

    only_a = sorted(set(rows_a) - set(rows_b))
    only_b = sorted(set(rows_b) - set(rows_a))
    shared = set(rows_a) & set(rows_b)

    opportunities: list[dict[str, Any]] = []
    for kw in only_b:
        row = rows_b[kw]
        traffic = _num(row, "traffic")
        if traffic < min_traffic:
            continue
        opportunities.append(
            {
                "keyword": kw,
                "competitor_rank": _rank(row),
                "traffic": traffic,
                "ci": _num(row, "ci"),
                "kei": _num(row, "kei"),
                "asa": row.get("asa"),
            }
        )
    opportunities.sort(key=lambda x: x["traffic"], reverse=True)

    shared_compare = []
    for kw in shared:
        row_a, row_b = rows_a[kw], rows_b[kw]
        traffic = max(_num(row_a, "traffic"), _num(row_b, "traffic"))
        if traffic < min_traffic:
            continue
        shared_compare.append(
            {
                "keyword": kw,
                "rank_a": _rank(row_a),
                "rank_b": _rank(row_b),
                "traffic": traffic,
            }
        )
    shared_compare.sort(key=lambda x: x["traffic"], reverse=True)

    total_unique = len(set(rows_a) | set(rows_b)) or 1
    return {
        "country": country.upper(),
        "app_a": app_id_a,
        "app_b": app_id_b,
        "totals": {
            "app_a_keywords": len(rows_a),
            "app_b_keywords": len(rows_b),
            "shared": len(shared),
            "only_a": len(only_a),
            "only_b": len(only_b),
            "overlap_pct": round(100 * len(shared) / total_unique, 1),
        },
        "opportunities": opportunities[:top_n],
        "shared_top": shared_compare[:top_n],
        "only_a_sample": only_a[:top_n],
        "only_b_sample": only_b[:top_n],
    }


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    if not TOKEN:
        # Warn on stderr but DON'T crash — let the MCP client list tools first.
        print(
            "[asomobile-mcp] WARNING: ASOMOBILE_API_TOKEN env var is not set. "
            "Tool calls will fail until you set it.",
            flush=True,
        )
    mcp.run()


if __name__ == "__main__":
    main()

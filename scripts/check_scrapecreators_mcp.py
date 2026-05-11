#!/usr/bin/env python3
"""Validate parser assumptions against the live ScrapeCreators MCP manifest."""

from __future__ import annotations

import ast
import json
import os
import ssl
import sys
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MCP_URL = os.getenv("SCRAPECREATORS_MCP_URL", "https://api.scrapecreators.com/mcp")


def fail(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    raise SystemExit(1)


def fetch_manifest() -> dict:
    request = urllib.request.Request(MCP_URL, headers={"Accept": "application/json"})
    context = None
    try:
        import certifi

        context = ssl.create_default_context(cafile=certifi.where())
    except Exception:
        context = None

    if os.getenv("SCRAPECREATORS_MCP_INSECURE") == "1":
        context = ssl._create_unverified_context()

    try:
        with urllib.request.urlopen(request, timeout=30, context=context) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        fail(f"could not fetch ScrapeCreators MCP manifest from {MCP_URL}: {exc}")


def get_tools(manifest: dict) -> dict:
    tools = manifest.get("capabilities", {}).get("tools")
    if not isinstance(tools, dict):
        fail("MCP manifest does not contain capabilities.tools")
    return tools


def require_tool(tools: dict, name: str) -> dict:
    tool = tools.get(name)
    if not isinstance(tool, dict):
        fail(f"MCP tool {name!r} is missing")
    return tool


def require_input(tool: dict, name: str, required: bool = True) -> None:
    schema = tool.get("inputSchema") or {}
    properties = schema.get("properties") or {}
    required_fields = schema.get("required") or []
    if name not in properties:
        fail(f"MCP tool {tool.get('name')!r} does not define input {name!r}")
    if required and name not in required_fields:
        fail(f"MCP tool {tool.get('name')!r} no longer requires {name!r}")


def require_string_in_file(path: Path, needle: str) -> None:
    text = path.read_text(encoding="utf-8")
    if needle not in text:
        fail(f"{path.relative_to(ROOT)} does not contain expected string: {needle}")


def require_call_keyword(path: Path, function_name: str, keyword: str) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id == function_name:
            if any(arg.arg == keyword for arg in node.keywords):
                return
    fail(f"{path.relative_to(ROOT)} calls {function_name} without keyword {keyword!r}")


def main() -> None:
    manifest = fetch_manifest()
    tools = get_tools(manifest)

    tiktok_videos = require_tool(tools, "v3_tiktok_profile_videos")
    require_input(tiktok_videos, "handle", required=True)
    require_input(tiktok_videos, "user_id", required=False)
    require_string_in_file(
        ROOT / "app" / "parser_tiktok.py",
        "https://api.scrapecreators.com/v3/tiktok/profile/videos",
    )
    require_string_in_file(ROOT / "app" / "parser_tiktok.py", 'params["handle"] = handle')
    require_call_keyword(ROOT / "app" / "parser_tiktok.py", "fetch_tiktok_videos", "handle")

    tiktok_profile = require_tool(tools, "v1_tiktok_profile")
    require_input(tiktok_profile, "handle", required=True)
    require_string_in_file(
        ROOT / "app" / "services" / "enricher.py",
        "https://api.scrapecreators.com/v1/tiktok/profile",
    )

    print("OK: parser assumptions match ScrapeCreators MCP manifest")


if __name__ == "__main__":
    main()

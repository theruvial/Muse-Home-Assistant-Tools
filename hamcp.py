#!/usr/bin/env python3
"""hamcp: a fast, stdlib-only client for the HA-MCP server (Home Assistant).

Made for agents that reach HA-MCP over its webhook with a bearer token
(e.g. through Nabu Casa). It cuts the round trips that make each action slow:

* Session reuse: the MCP handshake runs once and its result is saved in a state
  file. Later runs skip ``initialize`` entirely (and re-handshake on their own
  if the server forgets the session).
* Batching: run many tool calls in one process: ``hamcp batch calls.json``.
* Caching: ``tools/list`` and ``ha_get_skill_guide`` pages are cached on disk,
  so the ~87-tool catalog and the best-practice guide are fetched once.

Configuration (no secrets in this file):

    HA_MCP_URL         full webhook URL, .../api/webhook/mcp_...
    HA_MCP_TOKEN       Home Assistant long-lived access token (sent as Bearer)
    HA_MCP_TOKEN_FILE  alternative: path to a file holding the token
    HA_MCP_CACHE_DIR   where state/caches live (default ~/.cache/hamcp)

If your credential vault injects the token as an opaque placeholder that its
egress layer swaps in, put the placeholder in HA_MCP_TOKEN: it is sent only in
the ``Authorization`` header, which is where such swaps happen.

Python 3.10+, no third-party packages.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

PROTOCOL_VERSIONS = ("2025-11-25", "2025-06-18", "2025-03-26")
CLIENT_INFO = {"name": "hamcp", "version": "1.0"}
TOOLS_TTL = 24 * 3600          # tool catalog cache lifetime (seconds)
GUIDE_TTL = 7 * 24 * 3600      # skill guide cache lifetime (seconds)
DEFAULT_TIMEOUT = 120


class MCPError(Exception):
    """A JSON-RPC error, a tool error, or an HTTP failure."""

    def __init__(self, message: str, *, status: int | None = None, data: Any = None):
        super().__init__(message)
        self.status = status
        self.data = data


class SessionExpired(MCPError):
    """The server no longer knows our session; re-handshake and retry once."""


# --------------------------------------------------------------------------- #
# Transport
# --------------------------------------------------------------------------- #

def parse_sse(body: str) -> list[dict]:
    """Return the JSON payloads of every ``data:`` event in an SSE body."""
    messages, data_lines = [], []
    for line in body.splitlines() + [""]:
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
        elif line == "" and data_lines:
            try:
                messages.append(json.loads("\n".join(data_lines)))
            except json.JSONDecodeError:
                pass
            data_lines = []
    return messages


class HAMCP:
    """Minimal Streamable-HTTP MCP client with on-disk session/caches."""

    def __init__(self, url: str, token: str | None, cache_dir: Path,
                 timeout: float = DEFAULT_TIMEOUT):
        if not url:
            raise MCPError("HA_MCP_URL is not set")
        self.url = url
        self.token = token
        self.timeout = timeout
        # Key the cache by URL hash so a rotated webhook ID never reuses state,
        # and the URL itself never lands on disk.
        key = hashlib.sha256(url.encode()).hexdigest()[:12]
        self.dir = Path(cache_dir) / key
        self.dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.dir / "session.json"
        self.state = self._load(self.state_path) or {}
        self._next_id = 1

    # ---- small file helpers ------------------------------------------------
    @staticmethod
    def _load(path: Path) -> Any:
        try:
            return json.loads(path.read_text("utf-8"))
        except (OSError, ValueError):
            return None

    @staticmethod
    def _save(path: Path, obj: Any) -> None:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(obj), "utf-8")
        tmp.replace(path)

    # ---- HTTP ----------------------------------------------------------------
    def _post(self, payload: dict, *, use_session: bool = True) -> tuple[list[dict], dict]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": self.state.get("protocol", PROTOCOL_VERSIONS[0]),
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if use_session and self.state.get("session_id"):
            headers["Mcp-Session-Id"] = self.state["session_id"]
        req = urllib.request.Request(
            self.url, data=json.dumps(payload).encode(), headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8", "replace")
                ctype = resp.headers.get("Content-Type", "")
                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
        except urllib.error.HTTPError as e:
            text = e.read().decode("utf-8", "replace")[:500]
            if e.code == 404 and use_session and self.state.get("session_id"):
                raise SessionExpired("session expired", status=404) from None
            hint = {
                401: "token missing/invalid, or not an active admin user",
                403: "forbidden",
            }.get(e.code, "")
            raise MCPError(f"HTTP {e.code} {hint}: {text}".strip(), status=e.code) from None
        except urllib.error.URLError as e:
            raise MCPError(f"network error: {e.reason}") from None

        if not body.strip():
            if "id" not in payload:          # notifications get 202/empty
                return [], resp_headers
            # Home Assistant answers an unknown webhook ID with an empty 200.
            raise MCPError("empty response: the webhook ID in HA_MCP_URL is wrong "
                           "or the server is off", status=200)
        if "text/event-stream" in ctype:
            return parse_sse(body), resp_headers
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            raise MCPError(f"non-JSON response: {body[:200]}") from None
        return (data if isinstance(data, list) else [data]), resp_headers

    def _rpc(self, method: str, params: dict | None = None, *, use_session: bool = True) -> Any:
        rid = self._next_id
        self._next_id += 1
        msgs, headers = self._post(
            {"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}},
            use_session=use_session)
        if method == "initialize" and headers.get("mcp-session-id"):
            self.state["session_id"] = headers["mcp-session-id"]
        for m in msgs:
            if m.get("id") == rid:
                if "error" in m:
                    err = m["error"]
                    msg = str(err.get("message", err))
                    if use_session and _looks_uninitialized(msg):
                        raise SessionExpired(msg)
                    raise MCPError(msg, data=err.get("data"))
                return m.get("result")
        raise MCPError(f"no response to {method}")

    # ---- session -------------------------------------------------------------
    def handshake(self) -> dict:
        """Run initialize + notifications/initialized and persist the result."""
        self.state = {}
        result = self._rpc("initialize", {
            "protocolVersion": PROTOCOL_VERSIONS[0],
            "capabilities": {},
            "clientInfo": CLIENT_INFO,
        }, use_session=False)
        self.state["protocol"] = result.get("protocolVersion", PROTOCOL_VERSIONS[0])
        self.state["server"] = result.get("serverInfo", {})
        self.state["at"] = time.time()
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        self._save(self.state_path, self.state)
        return result

    def ensure_session(self) -> None:
        if not self.state.get("protocol"):
            self.handshake()

    def request(self, method: str, params: dict | None = None) -> Any:
        """Send a request, handshaking first if needed and once more on expiry."""
        self.ensure_session()
        try:
            return self._rpc(method, params)
        except SessionExpired:
            self.handshake()
            return self._rpc(method, params)

    # ---- tools -----------------------------------------------------------------
    def list_tools(self, refresh: bool = False) -> list[dict]:
        path = self.dir / "tools.json"
        cached = self._load(path)
        if cached and not refresh and time.time() - cached["at"] < TOOLS_TTL:
            return cached["tools"]
        tools, cursor = [], None
        while True:
            res = self.request("tools/list", {"cursor": cursor} if cursor else {})
            tools.extend(res.get("tools", []))
            cursor = res.get("nextCursor")
            if not cursor:
                break
        self._save(path, {"at": time.time(), "tools": tools})
        return tools

    def call(self, tool: str, args: dict | None = None) -> Any:
        """Call a tool and return its decoded result (raises MCPError on isError)."""
        res = self.request("tools/call", {"name": tool, "arguments": args or {}})
        value = decode_result(res)
        if res.get("isError"):
            raise MCPError(f"{tool} failed: {_short(value)}", data=value)
        return value

    def guide(self, file: str | None = None, refresh: bool = False) -> Any:
        """ha_get_skill_guide with an on-disk cache (the guide rarely changes)."""
        name = (file or "SKILL.md").replace("/", "__")
        path = self.dir / f"guide__{name}.json"
        cached = self._load(path)
        if cached and not refresh and time.time() - cached["at"] < GUIDE_TTL:
            return cached["value"]
        value = self.call("ha_get_skill_guide", {"file": file} if file else {})
        self._save(path, {"at": time.time(), "value": value})
        return value

    def batch(self, calls: list[dict], stop_on_error: bool = True) -> list[dict]:
        """Run ``[{"tool": ..., "args": {...}}, ...]`` in order over one session."""
        out = []
        for i, c in enumerate(calls):
            tool = c.get("tool") or c.get("name")
            try:
                out.append({"i": i, "tool": tool, "ok": True,
                            "result": self.call(tool, c.get("args") or c.get("arguments"))})
            except MCPError as e:
                out.append({"i": i, "tool": tool, "ok": False, "error": str(e)})
                if stop_on_error:
                    break
        return out


def _looks_uninitialized(msg: str) -> bool:
    m = msg.lower()
    return any(s in m for s in ("not initialized", "session not found",
                                "invalid session", "unknown session", "no valid session"))


def decode_result(res: dict) -> Any:
    """Turn an MCP tool result into plain data.

    Prefers ``structuredContent``; otherwise joins text parts and JSON-decodes
    them when possible. Images/resources become short placeholders.
    """
    if res.get("structuredContent") is not None:
        sc = res["structuredContent"]
        # FastMCP wraps non-object returns as {"result": ...}
        return sc["result"] if isinstance(sc, dict) and set(sc) == {"result"} else sc
    parts = []
    for c in res.get("content", []):
        if c.get("type") == "text":
            parts.append(c.get("text", ""))
        else:
            parts.append(f"[{c.get('type')} content omitted]")
    text = "\n".join(parts)
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return text


def _short(v: Any, n: int = 300) -> str:
    s = v if isinstance(v, str) else json.dumps(v, default=str)
    return s if len(s) <= n else s[:n] + "…"


def _drop_keys(v: Any, keys: set[str]) -> Any:
    if isinstance(v, dict):
        return {k: _drop_keys(x, keys) for k, x in v.items() if k not in keys}
    if isinstance(v, list):
        return [_drop_keys(x, keys) for x in v]
    return v


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def _read_token() -> str | None:
    tok = os.environ.get("HA_MCP_TOKEN")
    if tok:
        return tok.strip()
    path = os.environ.get("HA_MCP_TOKEN_FILE")
    if path:
        return Path(path).expanduser().read_text("utf-8").strip()
    return None


def _json_arg(s: str | None) -> Any:
    if s is None:
        return {}
    if s == "-":
        return json.load(sys.stdin)
    if s.startswith("@"):
        return json.loads(Path(s[1:]).read_text("utf-8"))
    return json.loads(s)


def _emit(obj: Any, args: argparse.Namespace) -> None:
    if args.drop:
        obj = _drop_keys(obj, set(args.drop.split(",")))
    text = obj if isinstance(obj, str) else json.dumps(obj, indent=None if args.compact else 2,
                                                         ensure_ascii=False, default=str)
    if args.max_chars and len(text) > args.max_chars:
        text = text[:args.max_chars] + f"\n…[truncated, {len(text) - args.max_chars} more chars]"
    print(text)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="hamcp", description=__doc__.split("\n\n")[0])
    p.add_argument("--max-chars", type=int, default=0, help="truncate printed output")
    p.add_argument("--compact", action="store_true", help="single-line JSON output")
    p.add_argument("--drop", help="comma-separated keys to strip from output "
                                  "(e.g. skill_content)")
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ping", help="handshake and show server info")
    t = sub.add_parser("tools", help="list tool names (cached)")
    t.add_argument("query", nargs="?", help="filter by substring in name/description")
    t.add_argument("--refresh", action="store_true")
    t.add_argument("--full", action="store_true", help="show schemas too")
    s = sub.add_parser("schema", help="show one tool's description and input schema")
    s.add_argument("tool")
    c = sub.add_parser("call", help="call one tool")
    c.add_argument("tool")
    c.add_argument("args", nargs="?", help="JSON object, @file.json, or - for stdin")
    b = sub.add_parser("batch", help="run a JSON list of calls in one session")
    b.add_argument("file", help='file with [{"tool": ..., "args": {...}}, ...], or -')
    b.add_argument("--keep-going", action="store_true", help="don't stop at first error")
    g = sub.add_parser("guide", help="ha_get_skill_guide (cached)")
    g.add_argument("file", nargs="?", help="reference file path, omit for SKILL.md")
    g.add_argument("--refresh", action="store_true")
    a = sub.add_parser("auto-get", help="get an automation's config")
    a.add_argument("entity_id")
    sub.add_parser("reset", help="forget the saved session and caches")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cache_dir = Path(os.environ.get("HA_MCP_CACHE_DIR", "~/.cache/hamcp")).expanduser()
    try:
        client = HAMCP(os.environ.get("HA_MCP_URL", ""), _read_token(), cache_dir, args.timeout)
        if args.cmd == "reset":
            for f in client.dir.glob("*.json"):
                f.unlink()
            _emit("reset", args)
        elif args.cmd == "ping":
            info = client.handshake()
            _emit({"server": info.get("serverInfo"), "protocol": info.get("protocolVersion"),
                   "session": bool(client.state.get("session_id"))}, args)
        elif args.cmd == "tools":
            tools = client.list_tools(refresh=args.refresh)
            if args.query:
                q = args.query.lower()
                tools = [t for t in tools
                         if q in t["name"].lower() or q in (t.get("description") or "").lower()]
            _emit(tools if args.full else [t["name"] for t in tools], args)
        elif args.cmd == "schema":
            match = [t for t in client.list_tools() if t["name"] == args.tool]
            if not match:
                raise MCPError(f"no tool named {args.tool}")
            _emit(match[0], args)
        elif args.cmd == "call":
            _emit(client.call(args.tool, _json_arg(args.args)), args)
        elif args.cmd == "batch":
            calls = _json_arg(args.file if args.file == "-" else "@" + args.file)
            results = client.batch(calls, stop_on_error=not args.keep_going)
            _emit(results, args)
            return 0 if all(r["ok"] for r in results) else 1
        elif args.cmd == "guide":
            _emit(client.guide(args.file, refresh=args.refresh), args)
        elif args.cmd == "auto-get":
            _emit(client.call("ha_config_get_automation", {"entity_id": args.entity_id}), args)
        return 0
    except MCPError as e:
        print(f"hamcp: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

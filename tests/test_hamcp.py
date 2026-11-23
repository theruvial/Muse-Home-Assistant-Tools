"""Tests for ha_mcp_client/hamcp.py against an in-process mock HA-MCP server."""

from __future__ import annotations

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import hamcp  # noqa: E402

TOKEN = "good-token"
PATH = "/api/webhook/mcp_test"
TOOLS = [
    {"name": "ha_get_skill_guide", "description": "best practices", "inputSchema": {}},
    {"name": "ha_config_get_automation", "description": "get automation", "inputSchema": {}},
    {"name": "ha_config_set_automation", "description": "set automation", "inputSchema": {}},
]


class MockServer:
    """Streamable-HTTP MCP server imitating HA-MCP behind Home Assistant."""

    def __init__(self, *, stateful: bool, sse: bool):
        self.stateful, self.sse = stateful, sse
        self.sessions: set[str] = set()
        self.log: list[str] = []          # methods received, in order
        self.calls = {"ha_get_skill_guide": 0}
        server = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path != PATH:                 # HA: unknown webhook -> empty 200
                    return self._raw(200, b"", None)
                if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                    return self._raw(401, b"Unauthorized", "text/plain")
                method = body.get("method")
                server.log.append(method)
                sid = self.headers.get("Mcp-Session-Id")
                if method == "initialize":
                    extra = {}
                    if server.stateful:
                        new = f"s{len(server.sessions) + 1}"
                        server.sessions.add(new)
                        extra["Mcp-Session-Id"] = new
                    return self._reply(body["id"], {
                        "protocolVersion": body["params"]["protocolVersion"],
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "ha-mcp", "version": "9.2.0"}}, extra)
                if server.stateful and sid not in server.sessions:
                    return self._raw(404, b"session not found", "text/plain")
                if "id" not in body:                  # notification
                    return self._raw(202, b"", None)
                if method == "tools/list":
                    return self._reply(body["id"], {"tools": TOOLS})
                if method == "tools/call":
                    name, a = body["params"]["name"], body["params"]["arguments"]
                    if name == "ha_get_skill_guide":
                        server.calls[name] += 1
                        return self._reply(body["id"], {"content": [
                            {"type": "text", "text": f"guide:{a.get('file', 'SKILL.md')}"}]})
                    if name == "ha_config_get_automation":
                        return self._reply(body["id"], {"content": [{"type": "text", "text":
                            json.dumps({"config": {"id": a["entity_id"]}, "config_hash": "h1"})}]})
                    if name == "fails":
                        return self._reply(body["id"], {"isError": True, "content": [
                            {"type": "text", "text": "boom"}]})
                    return self._reply(body["id"], None,
                                       error={"code": -32602, "message": f"unknown tool {name}"})
                return self._reply(body["id"], None, error={"code": -32601, "message": "nope"})

            def _reply(self, rid, result, extra=None, error=None):
                msg = {"jsonrpc": "2.0", "id": rid}
                msg.update({"error": error} if error else {"result": result})
                if server.sse:
                    data = f"event: message\ndata: {json.dumps(msg)}\n\n".encode()
                    self._raw(200, data, "text/event-stream", extra)
                else:
                    self._raw(200, json.dumps(msg).encode(), "application/json", extra)

            def _raw(self, code, data, ctype, extra=None):
                self.send_response(code)
                if ctype:
                    self.send_header("Content-Type", ctype)
                for k, v in (extra or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.base = f"http://127.0.0.1:{self.httpd.server_port}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()


@pytest.fixture(params=[(True, True), (False, False)], ids=["stateful-sse", "stateless-json"])
def server(request):
    s = MockServer(stateful=request.param[0], sse=request.param[1])
    yield s
    s.close()


def client(server, tmp_path, token=TOKEN, path=PATH):
    return hamcp.HAMCP(server.base + path, token, tmp_path)


def test_handshake_once_then_reused_across_processes(server, tmp_path):
    client(server, tmp_path).call("ha_config_get_automation", {"entity_id": "automation.a"})
    # A "new process": fresh client object, same cache dir -> no second initialize.
    r = client(server, tmp_path).call("ha_config_get_automation", {"entity_id": "automation.b"})
    assert r == {"config": {"id": "automation.b"}, "config_hash": "h1"}
    assert server.log.count("initialize") == 1
    assert "notifications/initialized" in server.log


def test_expired_session_rehandshakes_and_retries(server, tmp_path):
    client(server, tmp_path).list_tools()
    server.sessions.clear()                       # server restarted / forgot us
    c = client(server, tmp_path)
    assert c.call("ha_get_skill_guide") == "guide:SKILL.md"
    expected = 2 if server.stateful else 1
    assert server.log.count("initialize") == expected


def test_tools_and_guide_are_cached(server, tmp_path):
    c = client(server, tmp_path)
    assert [t["name"] for t in c.list_tools()] == [t["name"] for t in TOOLS]
    c.list_tools()
    assert server.log.count("tools/list") == 1
    assert c.guide() == "guide:SKILL.md"
    assert client(server, tmp_path).guide() == "guide:SKILL.md"
    assert c.guide("references/x.md") == "guide:references/x.md"
    assert server.calls["ha_get_skill_guide"] == 2      # SKILL.md once + x.md once
    c.guide(refresh=True)
    assert server.calls["ha_get_skill_guide"] == 3


def test_batch_runs_in_one_session_and_stops_on_error(server, tmp_path):
    c = client(server, tmp_path)
    res = c.batch([
        {"tool": "ha_get_skill_guide"},
        {"tool": "fails"},
        {"tool": "ha_config_get_automation", "args": {"entity_id": "automation.a"}},
    ])
    assert [r["ok"] for r in res] == [True, False]
    assert "boom" in res[1]["error"]
    res = c.batch([{"tool": "fails"}, {"tool": "ha_get_skill_guide"}], stop_on_error=False)
    assert [r["ok"] for r in res] == [False, True]
    assert server.log.count("initialize") == 1


def test_bad_token_is_a_clear_401(server, tmp_path):
    with pytest.raises(hamcp.MCPError) as e:
        client(server, tmp_path, token="wrong").call("ha_get_skill_guide")
    assert e.value.status == 401 and "admin" in str(e.value)


def test_wrong_webhook_id_explains_empty_200(server, tmp_path):
    with pytest.raises(hamcp.MCPError) as e:
        client(server, tmp_path, path="/api/webhook/mcp_wrong").call("ha_get_skill_guide")
    assert "webhook ID" in str(e.value)


def test_rpc_error_surfaces(server, tmp_path):
    with pytest.raises(hamcp.MCPError, match="unknown tool"):
        client(server, tmp_path).call("does_not_exist")


def test_cli_end_to_end(server, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HA_MCP_URL", server.base + PATH)
    monkeypatch.setenv("HA_MCP_TOKEN", TOKEN)
    monkeypatch.setenv("HA_MCP_CACHE_DIR", str(tmp_path))
    assert hamcp.main(["--compact", "tools", "automation"]) == 0
    assert json.loads(capsys.readouterr().out) == [
        "ha_config_get_automation", "ha_config_set_automation"]
    calls = tmp_path / "calls.json"
    calls.write_text(json.dumps([{"tool": "ha_get_skill_guide", "args": {}}]))
    assert hamcp.main(["--compact", "batch", str(calls)]) == 0
    assert json.loads(capsys.readouterr().out)[0]["result"] == "guide:SKILL.md"
    assert hamcp.main(["--compact", "--drop", "config_hash",
                       "auto-get", "automation.wake_me_up"]) == 0
    assert json.loads(capsys.readouterr().out) == {"config": {"id": "automation.wake_me_up"}}
    monkeypatch.setenv("HA_MCP_TOKEN", "wrong")
    assert hamcp.main(["reset"]) == 0
    capsys.readouterr()
    assert hamcp.main(["ping"]) == 2
    assert "401" in capsys.readouterr().err


def test_parse_sse_multiple_events():
    body = 'event: message\ndata: {"a": 1}\n\n: comment\ndata: {"b":\ndata: 2}\n\n'
    assert hamcp.parse_sse(body) == [{"a": 1}, {"b": 2}]


def test_url_never_written_to_disk(server, tmp_path):
    client(server, tmp_path).list_tools()
    for f in tmp_path.rglob("*"):
        if f.is_file():
            assert "mcp_test" not in f.read_text("utf-8")

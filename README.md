# Muse Home Assistant Tools

`hamcp.py` is a fast client for the [HA-MCP](https://github.com/homeassistant-ai/ha-mcp) server, built for an AI agent (Seline, on Meta Muse) that reaches Home Assistant from a cloud sandbox over HTTPS.

It's one file, uses only the Python standard library (3.10+), and contains no secrets.

## Why it's faster

Each Home Assistant action used to pay the full setup cost: start a script, run the MCP handshake, fetch the tool list, read the best-practice guide, and only then do the actual work, with every step a round trip through two proxies. `hamcp` removes most of that:

| Before | With `hamcp` |
|---|---|
| `initialize` on every run | Handshake **once**. The session is saved to disk and reused by later runs; if the server forgets it, `hamcp` re-handshakes and retries automatically. |
| One script run per tool call | **`batch`** runs a whole list of calls in one process and one session. |
| `tools/list` (~87 tools) every time | Cached for 24 h (`tools --refresh` to force). |
| `ha_get_skill_guide` before every edit | Cached for 7 days per page (`guide --refresh` to force). |
| Huge responses | `--drop skill_content` and `--max-chars N` trim what's printed. |

## Setup

Set these in the environment (or however your connector passes config):

| Variable | Value |
|---|---|
| `HA_MCP_URL` | The full webhook URL: `https://<your-instance>.ui.nabu.casa/api/webhook/mcp_<id>` |
| `HA_MCP_TOKEN` | A Home Assistant **long-lived access token** from an **active admin** user. Sent only as `Authorization: Bearer …`. |
| `HA_MCP_TOKEN_FILE` | (alternative) path to a file containing the token |
| `HA_MCP_CACHE_DIR` | (optional) cache location, default `~/.cache/hamcp` |

**Credential vaults that use placeholders:** if your vault gives your code an opaque placeholder and swaps in the real secret on the way out, put the placeholder in `HA_MCP_TOKEN`. `hamcp` only ever puts the token in the `Authorization` header, which is where those swaps happen. They do **not** happen in the URL path, which is why the webhook URL is plain config and not a vault secret.

The cache is keyed by a hash of the URL. The URL itself is never written to disk, and a new webhook ID starts a fresh session automatically.

## Commands

```bash
python hamcp.py ping                          # handshake; shows server name/version
python hamcp.py tools                         # tool names (cached)
python hamcp.py tools automation              # filter by name/description
python hamcp.py schema ha_config_set_automation   # one tool's description + input schema
python hamcp.py guide                         # SKILL.md (cached)
python hamcp.py guide references/automation-actions.md
python hamcp.py auto-get automation.wake_me_up
python hamcp.py call ha_search '{"query": "bedroom"}'
python hamcp.py call ha_config_set_automation @edit.json   # args from a file
python hamcp.py batch examples/read-then-edit.json
python hamcp.py reset                         # forget session + caches
```

Global options go **before** the command: `--compact`, `--max-chars N`, `--drop key1,key2`, `--timeout SECONDS`.

`batch` takes a JSON list:

```json
[
  {"tool": "ha_get_skill_guide"},
  {"tool": "ha_config_get_automation", "args": {"entity_id": "automation.wake_me_up"}}
]
```

It stops at the first failure unless you pass `--keep-going`, and it exits non-zero if any call failed.

## Use it as a library

```python
from hamcp import HAMCP
from pathlib import Path
import os

ha = HAMCP(os.environ["HA_MCP_URL"], os.environ["HA_MCP_TOKEN"], Path("~/.cache/hamcp").expanduser())
current = ha.call("ha_config_get_automation", {"entity_id": "automation.wake_me_up"})
ha.call("ha_config_set_automation", {
    "entity_id": "automation.wake_me_up",
    "config_hash": current["config_hash"],
    "python_transform": "config['triggers'][0]['at'] = '23:45:00'",
})
```

## Editing automations safely

- **Read first, then write with `config_hash`.** `ha_config_set_automation` uses the hash from `ha_config_get_automation` for optimistic locking, so your edit is rejected if someone changed the automation in between.
- **Prefer `python_transform` for small changes** (change one time, add one action). It edits the stored config in place, so you don't resend the whole automation.
- **Best-practices key:** if the server runs in strict best-practices mode, `ha_config_set_automation` needs `BestPracticeKey`. It's an openly published phrase at the top of `hamcp.py guide`, not a secret. With the guide cached, that's free after the first read.
- To **turn an automation on or off**, pass `{"entity_id": "automation.x", "enabled": false}`. To **run it now**, pass `{"entity_id": "automation.x", "run_actions": true}`.

## Errors

| Message | Meaning |
|---|---|
| `HTTP 401 … token missing/invalid, or not an active admin user` | The token didn't arrive, or it belongs to a non-admin or inactive user. |
| `empty response: the webhook ID in HA_MCP_URL is wrong…` | Home Assistant answers an **unknown** webhook ID with an empty `200`. Check the URL, or whether the ID was regenerated. |
| `network error: …` | Couldn't reach the Nabu Casa URL. |
| `<tool> failed: …` | The tool ran and reported an error. The message is from Home Assistant. |

## Tests

```bash
pip install pytest
python -m pytest -q
```

The tests run against an in-process mock of HA-MCP in two modes, stateful with SSE responses and stateless with JSON. They cover session reuse across runs, re-handshaking when a session expires, caching, batching, a bad token, a wrong webhook ID, and the CLI.

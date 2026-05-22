# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

`pycertifspec` is a Python library for communicating with [SPEC](https://www.certif.com/content/spec/) — a Unix-based instrument control and data acquisition program used in X-ray synchrotron labs. It implements SPEC's binary TCP socket protocol and exposes motors, variables, counters, and scans as Python objects, plus an asyncio client and a CLI REPL.

## Development Setup

```bash
export UV_INDEX_URL="$(pip config get global.index-url)"  # required for CodeArtifact auth
uv sync --extra dev        # installs numpy + pytest
source .venv/bin/activate
```

## Common Commands

```bash
# Run all tests
uv run pytest tests/

# Run a single test file
uv run pytest tests/test_client.py

# Run a single test by name
uv run pytest tests/test_scan.py::TestScanRun::test_scan_returns_correct_points

# Run the interactive REPL (requires a live SPEC server)
uv run python -m pycertifspec --host localhost
uv run python -m pycertifspec --host 10.0.0.5 --port 6510
```

Development requires a running SPEC server started with `-S 6510` (or any port in 6510–6530). All tests run against `MockSpecServer` — no live SPEC needed.

## Architecture

### Protocol Layer (`SpecSocket.py`)

`SpecSocket` extends `socket.socket`. `SpecMessage` is a namedtuple mirroring the wire format. Header is 132 bytes packed with `struct.pack("IiIIIIiiIIIii80s", ...)`. `connect_spec()` scans a port range by sending `SV_HELLO` and checking for `SV_HELLO_REPLY`.

Key encoding detail: `SV_STRING` body is decoded to `str`; all other types stay as `bytes`.

### Threading Client (`Client.py`)

One background daemon thread (`_listener_thread`) reads messages in a loop and dispatches to:
- **`_subscribers`** dict (`prop → [callbacks]`) for `SV_EVENT` messages  
- **`_reply_events`** dict (`sn → threading.Event`) for serial-number-matched replies

`_send()` is guarded by `_send_lock` and raises `SpecError("not connected")` when `_connected` is False. On socket error the listener sets `_connected = False`, unblocks all pending reply events, then either calls `_do_reconnect()` (re-creates socket + re-sends `SV_REGISTER` for all active subscriptions) or exits.

Important: the listener thread must be started *after* all dicts are initialized but *before* `_get_counter_names()` / `subscribe()` calls in `__init__`.

### Non-blocking vs blocking `run()`

`run(blocking=True)` sends `SV_FUNC_WITH_RETURN` with the command in the **body bytes**. `run(blocking=False)` sends `SV_FUNC` with the command in the **`property_name` field** (not body). This asymmetry matters in `MockSpecServer._dispatch`.

### Motor (`Motor.py` vs `MotorUncached.py`)

`Motor` subscribes to `position`, `dial_position`, and `move_done` on construction — cached via events. `MotorUncached` polls every access. `MotorProperty` descriptor handles get/set; `move_done` value is **inverted** from SPEC's convention. `moveto()` uses a `threading.Condition` on `move_done` because the SPEC command returns before the move finishes.

### Variables (`Var.py`, `ArrayVar.py`)

`Var.value` handles all SPEC data types: `SV_STRING` → decoded str, `SV_ASSOC` → null-delimited pairs → dict, array types → `np.frombuffer`. `ArrayVar` extends `collections.abc.MutableSequence` (not the removed `collections.MutableSequence`). Index writes call `client.run("{name}[{i}]={x}")`. 2D arrays return `SubArrayVar` rows on first-index access.

### Scan (`Scan.py`)

Subscribes to `output/tty` before firing the command with `blocking=False`. Accumulates the tty stream into a line buffer, detects `#S` (scan start), `#L` (column headers), numeric rows, and the `> \n` prompt (scan complete). `run()` blocks via `threading.Event`; `run_iter()` streams via `Queue`. `as_numpy()` converts `list[dict]` → `dict[str, np.ndarray]`.

### Async Client (`AsyncClient.py`)

`AsyncClient.connect()` is the async factory. Uses `asyncio.StreamReader/Writer` internally. The `_listener()` coroutine runs as an `asyncio.Task`. Callbacks may be plain functions or async coroutines — both dispatched correctly. `AsyncScan` mirrors `Scan` with `async for` via `run_iter()` as an async generator.

### Bluesky Subpackage (`pycertifspec/bluesky/`)

Ophyd-compatible wrappers (`Motor`, `Counter`, `CommandDetector`). Optional dependency — imported with `try/except` fallback. Install with `uv sync --extra bluesky`.

## Test Infrastructure

`tests/conftest.py` provides `MockSpecServer` — a loopback TCP server speaking the full SPEC binary protocol. Handles `SV_HELLO`, `SV_REGISTER/UNREGISTER`, `SV_CHAN_READ/SEND`, `SV_FUNC/SV_FUNC_WITH_RETURN`, `SV_ABORT`. The `mock_server` pytest fixture starts one per test.

Subclass `MockSpecServer` and override `_handle_func(cmd: str) -> str` to simulate custom SPEC command responses (e.g. scan output streaming). See `tests/test_scan.py::ScanMockServer` for an example.

Key `MockSpecServer` invariant: `SV_REGISTER` for `"error"` must **never** send an initial event (real SPEC doesn't send one when there's no current error), or `Client.subscribe()` will misidentify it as a subscription failure.

## Protocol Constants

| Module | Purpose |
|--------|---------|
| `EventTypes` | SPEC command codes (`SV_REGISTER=6`, `SV_EVENT=8`, `SV_FUNC_WITH_RETURN=10`, etc.) |
| `DataTypes` | Body type codes + `NP_TYPES` dict (type → numpy dtype) + `ARRAYS` list |
| `Flags` | `SV_DELETED = 0x1000` |

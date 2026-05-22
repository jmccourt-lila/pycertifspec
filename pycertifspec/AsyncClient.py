"""
asyncio-based client for SPEC.

AsyncClient mirrors the threading-based Client interface but every blocking
operation is a coroutine. It uses asyncio streams internally so it integrates
naturally with Jupyter notebooks, FastAPI, and other async frameworks.

Usage::

    import asyncio
    from pycertifspec import AsyncClient

    async def main():
        client = await AsyncClient.connect("localhost")
        pos = await client.get("motor/m0/position")
        print(pos.body)
        await client.run("umv m0 5")
        scan = client.scan("ascan m0 0 10 20 0.5")
        async for point in scan.run_iter():
            print(point)
        await client.close()

    asyncio.run(main())
"""

import asyncio
import struct
import time as _time
import collections
import re
import numpy as np
from typing import Any, AsyncIterator, Callable, Coroutine, Dict, List, Optional, Tuple, Type, Union

from .EventTypes import EventTypes
from .DataTypes import DataTypes
from .SpecSocket import SpecMessage
from .SpecError import SpecError
from .Scan import _dispatch_line, _parse_header_cols, _try_parse_row

_SV_SPEC_MAGIC = 4277009102
_SV_VERSION = 4
_SV_NAME_LEN = 80
_HEADER_FMT = "IiIIIIiiIIIii80s"
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)  # 132 bytes


# ---------------------------------------------------------------------------
# Low-level async protocol helpers
# ---------------------------------------------------------------------------

def _pack_msg(sn: int, cmd: int, data_type: int = 0, name: str = "", body: bytes = b"", rows: int = 0, cols: int = 0) -> bytes:
    name_b = name.encode("ascii")[:_SV_NAME_LEN].ljust(_SV_NAME_LEN, b"\x00")
    header = struct.pack(
        _HEADER_FMT,
        _SV_SPEC_MAGIC,
        _SV_VERSION,
        _HEADER_SIZE,
        sn,
        int(_time.time()),
        int(_time.time() * 1e6) & 0xFFFFFFFF,
        cmd,
        data_type,
        rows,
        cols,
        len(body),
        0,
        0,
        name_b,
    )
    return header + body


async def _read_msg(reader: asyncio.StreamReader) -> SpecMessage:
    head1 = await reader.readexactly(12)
    magic, vers, size = struct.unpack("IiI", head1)

    if magic != _SV_SPEC_MAGIC:
        raise ValueError(f"Bad SPEC magic: got {magic}, expected {_SV_SPEC_MAGIC}")
    if vers < 4:
        raise ConnectionError(f"SPEC protocol version {vers} < 4 not supported")

    head2 = await reader.readexactly(size - 12)
    sn, sec, usec, cmd, dtype, rows, cols, length, err, flags = struct.unpack(
        "IIIiiIIIii", head2[: struct.calcsize("IIIiiIIIii")]
    )
    pad = size - _HEADER_SIZE
    name_raw = head2[struct.calcsize("IIIiiIIIii") + max(0, pad):][:_SV_NAME_LEN]
    name = name_raw.decode("utf-8").rstrip("\x00")

    body = await reader.readexactly(length) if length else b""

    msg = SpecMessage(magic, vers, size, sn, sec, usec, cmd, dtype, rows, cols, length, err, flags, name, body)
    if dtype == DataTypes.SV_STRING:
        msg = msg._replace(body=body.decode("utf-8").rstrip("\x00"))
    return msg


# ---------------------------------------------------------------------------
# AsyncClient
# ---------------------------------------------------------------------------

class AsyncClient:
    """
    asyncio-based connection to SPEC.

    Prefer constructing via the async factory ``await AsyncClient.connect(...)``.
    """

    def __init__(self):
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False

        self._sn_counter = 0
        self._sn_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()

        self._pending: Dict[int, asyncio.Future] = {}
        self._subscribers: Dict[str, List[Callable]] = {}
        self._sub_last_msg: Dict[str, SpecMessage] = {}

        self._last_console_output = ""
        self._console_lines: List[str] = []

        self.counter_names: collections.OrderedDict = collections.OrderedDict()

        self._listener_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    async def connect(
        cls,
        host: str = "localhost",
        port: int = None,
        port_range: Tuple[int, int] = (6510, 6530),
        ports: List[int] = [],
        timeout: float = 0.5,
        auto_reconnect: bool = True,
        reconnect_delay: float = 2.0,
    ) -> "AsyncClient":
        """
        Connect to a SPEC server and return a ready-to-use AsyncClient.

        Parameters:
            host: SPEC server hostname or IP
            port: Exact port if known; skips scanning
            port_range: Inclusive range of ports to scan
            ports: Additional specific ports to try first
            timeout: Per-port connection timeout in seconds
            auto_reconnect: Reconnect automatically on connection loss
            reconnect_delay: Seconds to wait between reconnect attempts
        """
        self = cls()
        self._host = host
        self._port = port
        self._port_range = port_range
        self._ports = ports
        self._scan_timeout = timeout
        self._auto_reconnect = auto_reconnect
        self._reconnect_delay = reconnect_delay

        await self._do_connect()
        await self._init_session()
        return self

    async def _do_connect(self):
        """Scan ports and establish the TCP + SPEC handshake."""
        to_scan = list(self._ports)
        if self._port is not None:
            to_scan.insert(0, self._port)
        to_scan += list(range(self._port_range[0], self._port_range[1] + 1))

        for p in to_scan:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self._host, p), timeout=self._scan_timeout
                )
            except (OSError, asyncio.TimeoutError):
                continue

            # Send SV_HELLO
            writer.write(_pack_msg(0, EventTypes.SV_HELLO, name="pycertifspec"))
            await writer.drain()

            try:
                msg = await asyncio.wait_for(_read_msg(reader), timeout=self._scan_timeout)
            except (asyncio.TimeoutError, Exception):
                writer.close()
                continue

            if msg.cmd == EventTypes.SV_HELLO_REPLY:
                self._reader = reader
                self._writer = writer
                self._connected = True
                return

            writer.close()

        raise ConnectionError(f"No SPEC server found on {self._host}")

    async def _init_session(self):
        """Start listener, populate counter names, subscribe to error+tty."""
        self._listener_task = asyncio.get_running_loop().create_task(self._listener())
        await self._subscribe_internal("error", None, nowait=True)
        await self._subscribe_internal("output/tty", self._tty_handler)
        await self._refresh_counter_names()

    # ------------------------------------------------------------------
    # Listener
    # ------------------------------------------------------------------

    async def _listener(self):
        while True:
            try:
                msg = await _read_msg(self._reader)
            except Exception:
                self._connected = False
                for fut in self._pending.values():
                    if not fut.done():
                        fut.set_result(None)
                self._pending.clear()

                if self._auto_reconnect:
                    await asyncio.sleep(self._reconnect_delay)
                    try:
                        await self._do_reconnect()
                    except Exception:
                        pass
                else:
                    return
                continue

            if msg.cmd == EventTypes.SV_EVENT:
                self._sub_last_msg[msg.name] = msg
                for cb in list(self._subscribers.get(msg.name, [])):
                    result = cb(msg)
                    if asyncio.iscoroutine(result):
                        asyncio.get_running_loop().create_task(result)

            sn = msg.sn
            if sn in self._pending:
                fut = self._pending.pop(sn)
                if not fut.done():
                    fut.set_result(msg)

    # ------------------------------------------------------------------
    # Reconnect
    # ------------------------------------------------------------------

    async def _do_reconnect(self):
        try:
            self._writer.close()
        except Exception:
            pass
        await self._do_connect()
        # Re-register active subscriptions
        for prop in list(self._subscribers.keys()):
            self._writer.write(_pack_msg(0, EventTypes.SV_REGISTER, name=prop))
        await self._writer.drain()

    async def reconnect(self):
        """Manually trigger reconnection."""
        await self._do_reconnect()

    # ------------------------------------------------------------------
    # Core send / receive
    # ------------------------------------------------------------------

    async def _send(
        self,
        cmd: int,
        data_type: int = 0,
        name: str = "",
        body: bytes = b"",
        rows: int = 0,
        cols: int = 0,
        wait_reply: bool = False,
        timeout: float = 2.0,
    ) -> Optional[SpecMessage]:
        if not self._connected:
            raise SpecError("not connected")

        async with self._sn_lock:
            self._sn_counter += 1
            sn = self._sn_counter

        fut: Optional[asyncio.Future] = None
        if wait_reply:
            fut = asyncio.get_event_loop().create_future()
            self._pending[sn] = fut

        data = _pack_msg(sn, cmd, data_type, name, body, rows, cols)
        async with self._write_lock:
            self._writer.write(data)
            await self._writer.drain()

        if fut is not None:
            try:
                return await asyncio.wait_for(fut, timeout=timeout)
            except asyncio.TimeoutError:
                self._pending.pop(sn, None)
                return None

        return None

    # ------------------------------------------------------------------
    # Public API — mirrors Client
    # ------------------------------------------------------------------

    async def get(self, prop: str) -> Optional[SpecMessage]:
        """Get a SPEC property value."""
        return await self._send(
            EventTypes.SV_CHAN_READ, DataTypes.SV_STRING, name=prop,
            wait_reply=True, timeout=2.0,
        )

    async def set(self, prop: str, value: str) -> None:
        """Set a SPEC property value."""
        res = await self._send(
            EventTypes.SV_CHAN_SEND, DataTypes.SV_STRING, name=prop,
            body=value.encode("ascii"), wait_reply=True, timeout=0.5,
        )
        if res and res.type == DataTypes.SV_ERROR:
            raise SpecError(res.body)

    async def run(self, console_command: str) -> Tuple[Optional[SpecMessage], str]:
        """
        Execute a SPEC console command and wait for its reply.

        Returns:
            Tuple of (reply SpecMessage, console output string).
        """
        if not console_command.endswith("\n"):
            console_command += "\n"
        reply = await self._send(
            EventTypes.SV_FUNC_WITH_RETURN,
            body=console_command.encode("ascii"),
            wait_reply=True,
            timeout=30.0,
        )
        return reply, self._last_console_output

    async def subscribe(
        self,
        prop: str,
        callback: Callable,
        timeout: float = 1.0,
    ) -> bool:
        """
        Subscribe to property change events.

        Parameters:
            prop: Property name
            callback: Called with each SpecMessage. May be sync or async.
            timeout: Seconds to wait for the initial event confirming subscription.

        Returns:
            True on success, False on timeout.
        """
        return await self._subscribe_internal(prop, callback, nowait=False, timeout=timeout)

    async def _subscribe_internal(
        self,
        prop: str,
        callback: Optional[Callable],
        nowait: bool = False,
        timeout: float = 1.0,
    ) -> bool:
        if prop in self._subscribers:
            if callback is not None:
                self._subscribers[prop].append(callback)
                last = self._sub_last_msg.get(prop)
                if last is not None:
                    result = callback(last)
                    if asyncio.iscoroutine(result):
                        asyncio.get_running_loop().create_task(result)
            return True

        self._subscribers[prop] = [callback] if callback is not None else []
        await self._send(EventTypes.SV_REGISTER, name=prop)

        if nowait:
            return True

        deadline = _time.monotonic() + timeout
        while _time.monotonic() < deadline:
            await asyncio.sleep(0.02)
            if prop in self._sub_last_msg:
                last = self._sub_last_msg[prop]
                if last.name == "error":
                    del self._subscribers[prop]
                    raise SpecError(last.body)
                if callback is not None:
                    result = callback(last)
                    if asyncio.iscoroutine(result):
                        asyncio.get_running_loop().create_task(result)
                return True

        del self._subscribers[prop]
        return False

    async def unsubscribe(self, prop: str, callback: Callable) -> bool:
        """Unsubscribe a callback. Sends SV_UNREGISTER when no listeners remain."""
        if prop not in self._subscribers:
            return False
        try:
            self._subscribers[prop].remove(callback)
        except ValueError:
            return False
        if not self._subscribers[prop]:
            del self._subscribers[prop]
            await self._send(EventTypes.SV_UNREGISTER, name=prop)
        return True

    async def abort(self):
        """Abort all running SPEC commands."""
        await self._send(EventTypes.SV_ABORT)

    async def close(self):
        """Gracefully close the connection."""
        self._connected = False
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # TTY / console helpers
    # ------------------------------------------------------------------

    def _tty_handler(self, msg: SpecMessage):
        if msg.body.endswith("> \n"):
            self._last_console_output = "".join(self._console_lines)
            self._console_lines = []
        else:
            self._console_lines.append(msg.body)

    # ------------------------------------------------------------------
    # Counter / Motor / Var helpers
    # ------------------------------------------------------------------

    async def _refresh_counter_names(self):
        self.counter_names = collections.OrderedDict()
        msg = await self.get("var/COUNTERS")
        if msg is None or msg.type == DataTypes.SV_ERROR:
            return
        try:
            n = int(msg.body)
        except (ValueError, TypeError):
            return
        for i in range(n):
            mne_msg, _ = await self.run(f"cnt_mne({i})")
            name_msg, _ = await self.run(f"cnt_name({i})")
            if mne_msg and name_msg:
                self.counter_names[mne_msg.body] = name_msg.body

    async def count(self, duration: float) -> Dict[str, float]:
        """
        Count scalers for *duration* seconds.

        Returns:
            dict mapping counter mnemonic → float value.
        """
        countvals: Dict[str, float] = {k: 0.0 for k in self.counter_names}
        done = asyncio.Event()
        expected = len(self.counter_names)
        received: Dict[str, float] = {}

        def counter_cb(msg):
            mne = msg.name.split("/")[1]
            try:
                received[mne] = float(msg.body)
            except (ValueError, TypeError):
                pass
            if len(received) >= expected:
                done.set()

        for mne in self.counter_names:
            await self._subscribe_internal(f"scaler/{mne}/value", counter_cb, nowait=True)

        await self.run(f"count {duration}")

        # Read final values directly in case events were missed
        for mne in self.counter_names:
            msg = await self.get(f"scaler/{mne}/value")
            if msg and msg.body:
                try:
                    countvals[mne] = float(msg.body)
                except (ValueError, TypeError):
                    pass
            await self.unsubscribe(f"scaler/{mne}/value", counter_cb)

        countvals.update(received)
        return countvals

    def scan(self, command: str) -> "AsyncScan":
        """
        Create an AsyncScan for a SPEC scan macro.

        Parameters:
            command: Full SPEC scan command, e.g. ``"ascan m0 0 10 20 0.5"``
        """
        return AsyncScan(self, command)


# ---------------------------------------------------------------------------
# AsyncScan
# ---------------------------------------------------------------------------

class AsyncScan:
    """
    Async version of Scan.  Parses SPEC output/tty stream into data points.

    Usage::

        async for point in client.scan("ascan m0 0 10 20 0.5").run_iter():
            print(point)

        data = await client.scan("ascan m0 0 10 20 0.5").run()
        arrays = AsyncScan.as_numpy(data)
    """

    def __init__(self, client: AsyncClient, command: str):
        self._client = client
        self._command = command.strip()

    async def run(self, callback: Optional[Callable] = None) -> List[Dict[str, float]]:
        """Execute the scan and return all data points as a list of dicts."""
        data: List[Dict[str, float]] = []

        async def on_point(point):
            data.append(point)
            if callback:
                result = callback(point)
                if asyncio.iscoroutine(result):
                    await result

        await self._execute(on_point)
        return data

    async def run_iter(self) -> AsyncIterator[Dict[str, float]]:
        """Execute the scan and yield each data point as it arrives."""
        q: asyncio.Queue = asyncio.Queue()
        _sentinel = object()

        async def on_point(point):
            await q.put(point)

        async def execute_and_signal():
            await self._execute(on_point)
            await q.put(_sentinel)

        asyncio.get_running_loop().create_task(execute_and_signal())

        while True:
            item = await q.get()
            if item is _sentinel:
                break
            yield item

    async def abort(self):
        """Abort the currently running scan."""
        await self._client.abort()

    @staticmethod
    def as_numpy(data: List[Dict[str, float]]) -> Dict[str, np.ndarray]:
        """Convert run() output to a dict of numpy float64 arrays."""
        if not data:
            return {}
        return {k: np.array([row[k] for row in data], dtype=np.float64) for k in data[0]}

    async def _execute(self, on_point: Callable):
        state: Dict[str, Any] = {"phase": "waiting", "headers": [], "buf": ""}
        done = asyncio.Event()

        def tty_cb(msg):
            state["buf"] += msg.body
            lines = state["buf"].split("\n")
            state["buf"] = lines[-1]

            for line in lines[:-1]:
                _dispatch_async(line.rstrip("\r"), state, on_point)

            if msg.body.endswith("> \n") and state["phase"] == "in_scan":
                if state["buf"].strip():
                    _dispatch_async(state["buf"].strip(), state, on_point)
                    state["buf"] = ""
                state["phase"] = "done"
                done.set()

        await self._client._subscribe_internal("output/tty", tty_cb, nowait=True)
        await self._client._send(
            EventTypes.SV_FUNC,
            name=(self._command + "\n"),
        )

        await done.wait()
        await self._client.unsubscribe("output/tty", tty_cb)


def _dispatch_async(line: str, state: Dict, on_point: Callable):
    """Like _dispatch_line but on_point may be a coroutine — schedule it."""
    if not line:
        return

    if re.match(r'^#S\s', line):
        state["phase"] = "in_scan"
        state["headers"] = []
        return

    if state["phase"] != "in_scan":
        return

    cols = _parse_header_cols(line)
    if cols:
        state["headers"] = cols
        return

    if line.startswith("#") or not state["headers"]:
        return

    row = _try_parse_row(line, state["headers"])
    if row is not None:
        result = on_point(row)
        if asyncio.iscoroutine(result):
            asyncio.get_running_loop().create_task(result)


# Re-export Dict for type hints used internally
from typing import Dict, Any

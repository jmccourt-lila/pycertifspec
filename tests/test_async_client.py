"""
Tests for AsyncClient — asyncio-based SPEC connection.
"""
import asyncio
import threading
import time
import pytest

from pycertifspec import AsyncClient, AsyncScan
from pycertifspec.SpecError import SpecError
from pycertifspec.DataTypes import DataTypes

from conftest import MockSpecServer, _send_reply
from test_scan import _ASCAN_OUTPUT, _EXPECTED_POINTS


# ---------------------------------------------------------------------------
# Async mock server — wraps MockSpecServer with a scan-streaming subclass
# ---------------------------------------------------------------------------

class AsyncScanMockServer(MockSpecServer):
    """MockSpecServer that streams scan output for async tests."""

    def _handle_func(self, cmd):
        if any(cmd.startswith(s) for s in ("ascan", "dscan", "mesh")):
            threading.Thread(target=self._stream_scan, daemon=True).start()
        return ""

    def _stream_scan(self):
        time.sleep(0.05)
        with self._lock:
            subs = list(self._subscriptions.get("output/tty", []))
        for line in _ASCAN_OUTPUT.split("\n"):
            chunk = line + "\n"
            for conn in subs:
                try:
                    _send_reply(conn, 0, self.SV_EVENT, name="output/tty", body=chunk.encode())
                except Exception:
                    pass
            time.sleep(0.005)
        prompt = "> \n"
        for conn in subs:
            try:
                _send_reply(conn, 0, self.SV_EVENT, name="output/tty", body=prompt.encode())
            except Exception:
                pass


@pytest.fixture
def async_server():
    server = AsyncScanMockServer()
    server.set_prop("var/COUNTERS", "0")
    server.set_prop("output/tty", "> \n")
    yield server
    server.stop()


async def make_async_client(server):
    return await AsyncClient.connect(
        host=server.host,
        port=server.port,
        port_range=(server.port, server.port),
        timeout=1.0,
    )


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

class TestAsyncConnection:
    def test_connect_succeeds(self, async_server):
        async def run():
            client = await make_async_client(async_server)
            assert client._connected is True
            await client.close()
        asyncio.run(run())

    def test_connect_no_server_raises(self):
        async def run():
            with pytest.raises(ConnectionError):
                await AsyncClient.connect(
                    host="127.0.0.1",
                    port_range=(19997, 19997),
                    timeout=0.1,
                )
        asyncio.run(run())


# ---------------------------------------------------------------------------
# get / set
# ---------------------------------------------------------------------------

class TestAsyncGetSet:
    def test_get_existing_prop(self, async_server):
        async_server.set_prop("motor/m0/position", "7.5")

        async def run():
            client = await make_async_client(async_server)
            msg = await client.get("motor/m0/position")
            assert msg is not None
            assert msg.body == "7.5"
            await client.close()
        asyncio.run(run())

    def test_get_missing_prop_returns_none_or_error_msg(self, async_server):
        async def run():
            client = await make_async_client(async_server)
            msg = await client.get("no/such/prop")
            assert msg is None or msg.type == DataTypes.SV_ERROR
            await client.close()
        asyncio.run(run())

    def test_set_updates_prop(self, async_server):
        async_server.set_prop("motor/m0/position", "0.0")

        async def run():
            client = await make_async_client(async_server)
            await client.set("motor/m0/position", "42.0")
            msg = await client.get("motor/m0/position")
            assert msg.body == "42.0"
            await client.close()
        asyncio.run(run())


# ---------------------------------------------------------------------------
# subscribe / unsubscribe
# ---------------------------------------------------------------------------

class TestAsyncSubscribe:
    def test_subscribe_receives_initial_event(self, async_server):
        async_server.set_prop("var/Q", "99")

        async def run():
            client = await make_async_client(async_server)
            received = asyncio.Event()
            values = []

            def cb(msg):
                values.append(msg.body)
                received.set()

            await client.subscribe("var/Q", cb, timeout=2.0)
            await asyncio.wait_for(received.wait(), timeout=2.0)
            assert "99" in values
            await client.close()
        asyncio.run(run())

    def test_unsubscribe_removes_callback(self, async_server):
        async_server.set_prop("var/R", "1")

        async def run():
            client = await make_async_client(async_server)
            calls = []

            def cb(msg):
                calls.append(msg.body)

            await client.subscribe("var/R", cb)
            await asyncio.sleep(0.1)
            await client.unsubscribe("var/R", cb)
            before = len(calls)
            # Use client.set() so the mock server fans out SV_EVENT — set_prop() does not
            await client.set("var/R", "2")
            await asyncio.sleep(0.2)
            assert len(calls) == before
            await client.close()
        asyncio.run(run())


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestAsyncRun:
    def test_run_returns_tuple(self, async_server):
        async def run():
            client = await make_async_client(async_server)
            result = await client.run("p 1+1")
            assert isinstance(result, tuple)
            assert len(result) == 2
            await client.close()
        asyncio.run(run())


# ---------------------------------------------------------------------------
# abort
# ---------------------------------------------------------------------------

class TestAsyncAbort:
    def test_abort_does_not_raise(self, async_server):
        async def run():
            client = await make_async_client(async_server)
            await client.abort()
            await client.close()
        asyncio.run(run())


# ---------------------------------------------------------------------------
# Disconnected guard
# ---------------------------------------------------------------------------

class TestAsyncConnectedGuard:
    def test_send_raises_when_not_connected(self, async_server):
        async def run():
            client = await make_async_client(async_server)
            client._connected = False
            with pytest.raises(SpecError, match="not connected"):
                await client.get("motor/m0/position")
            await client.close()
        asyncio.run(run())


# ---------------------------------------------------------------------------
# AsyncScan
# ---------------------------------------------------------------------------

class TestAsyncScan:
    def test_scan_run_returns_correct_points(self, async_server):
        async def run():
            client = await make_async_client(async_server)
            scan = client.scan("ascan m0 0 10 20 0.5")
            data = await scan.run()
            assert data == _EXPECTED_POINTS
            await client.close()
        asyncio.run(run())

    def test_scan_run_iter_yields_all_points(self, async_server):
        async def run():
            client = await make_async_client(async_server)
            points = []
            async for point in client.scan("ascan m0 0 10 20 0.5").run_iter():
                points.append(point)
            assert points == _EXPECTED_POINTS
            await client.close()
        asyncio.run(run())

    def test_scan_callback_called_per_point(self, async_server):
        async def run():
            client = await make_async_client(async_server)
            received = []
            data = await client.scan("ascan m0 0 10 20 0.5").run(callback=received.append)
            assert received == _EXPECTED_POINTS
            await client.close()
        asyncio.run(run())

    def test_as_numpy_shape(self, async_server):
        import numpy as np
        async def run():
            client = await make_async_client(async_server)
            data = await client.scan("ascan m0 0 10 20 0.5").run()
            arrays = AsyncScan.as_numpy(data)
            assert set(arrays.keys()) == {"m0", "sec", "det"}
            assert arrays["det"].shape == (5,)
            np.testing.assert_array_equal(arrays["m0"], [0.0, 0.5, 1.0, 1.5, 2.0])
            await client.close()
        asyncio.run(run())

    def test_as_numpy_empty(self, async_server):
        async def run():
            client = await make_async_client(async_server)
            assert AsyncScan.as_numpy([]) == {}
            await client.close()
        asyncio.run(run())

    def test_scan_factory_returns_async_scan(self, async_server):
        async def run():
            client = await make_async_client(async_server)
            scan = client.scan("ascan m0 0 10 20 0.5")
            assert isinstance(scan, AsyncScan)
            await client.close()
        asyncio.run(run())

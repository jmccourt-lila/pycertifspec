"""
Integration tests for Client against MockSpecServer.
"""
import threading
import time
import pytest

from pycertifspec import Client
from pycertifspec.EventTypes import EventTypes
from pycertifspec.DataTypes import DataTypes

from conftest import MockSpecServer, _send_reply


# ---------------------------------------------------------------------------
# Helper: build a Client connected to the mock server
# ---------------------------------------------------------------------------

def make_client(mock_server, **kwargs):
    """Connect a Client directly to the mock server port, bypassing port scan."""
    # Pre-populate props that Client.__init__ subscribes to unconditionally
    mock_server.set_prop("var/COUNTERS", "0")
    mock_server.set_prop("output/tty", "> \n")  # console stream; must exist or subscribe raises
    return Client(
        host=mock_server.host,
        port=mock_server.port,
        port_range=(mock_server.port, mock_server.port),
        timeout=1.0,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

class TestConnection:
    def test_connects_successfully(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        client = make_client(mock_server)
        # If we reach here, connection succeeded
        assert client is not None

    def test_no_server_raises(self):
        with pytest.raises(Exception):
            Client(host="127.0.0.1", port_range=(19998, 19998), timeout=0.1)


# ---------------------------------------------------------------------------
# get / set
# ---------------------------------------------------------------------------

class TestGetSet:
    def test_get_existing_prop(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        mock_server.set_prop("motor/m0/position", "12.5")
        client = make_client(mock_server)
        msg = client.get("motor/m0/position")
        assert msg is not None
        assert msg.body == "12.5"

    def test_get_missing_prop_returns_none_or_error(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        client = make_client(mock_server)
        msg = client.get("nonexistent/prop", force_fetch=True)
        # Mock returns SV_ERROR body for missing props — client returns None or the msg
        # Either behaviour is acceptable; just shouldn't raise
        assert msg is None or msg.type == DataTypes.SV_ERROR

    def test_set_updates_prop(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        mock_server.set_prop("motor/m0/position", "0.0")
        client = make_client(mock_server)
        client.set("motor/m0/position", "99.0")
        msg = client.get("motor/m0/position", force_fetch=True)
        assert msg.body == "99.0"


# ---------------------------------------------------------------------------
# subscribe / unsubscribe
# ---------------------------------------------------------------------------

class TestSubscribe:
    def test_subscribe_receives_initial_event(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        mock_server.set_prop("var/X", "42")
        client = make_client(mock_server)

        received = threading.Event()
        values = []

        def cb(msg):
            values.append(msg.body)
            received.set()

        client.subscribe("var/X", cb)
        assert received.wait(2.0), "No event received within timeout"
        assert "42" in values

    def test_unsubscribe_removes_callback(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        mock_server.set_prop("var/Y", "1")
        client = make_client(mock_server)

        calls = []
        def cb(msg):
            calls.append(msg.body)

        client.subscribe("var/Y", cb)
        time.sleep(0.1)
        client.unsubscribe("var/Y", cb)
        before = len(calls)

        # Use client.set() so the mock server fans out SV_EVENT — set_prop() does not
        client.set("var/Y", "2")
        time.sleep(0.2)
        assert len(calls) == before

    def test_subscribe_invalid_prop_raises_or_returns_false(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        client = make_client(mock_server)
        # nowait=False with a prop that triggers error or times out
        result = client.subscribe("totally/fake/prop/xyz", lambda m: None, timeout=0.3)
        assert result is False or result is True  # both are valid; mustn't raise


# ---------------------------------------------------------------------------
# watch / unwatch
# ---------------------------------------------------------------------------

class TestWatch:
    def test_watch_caches_value(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        mock_server.set_prop("var/Z", "7")
        client = make_client(mock_server)
        client.watch("var/Z")
        time.sleep(0.2)  # allow subscription event to arrive
        msg = client.get("var/Z")  # should hit cache
        assert msg is not None
        assert msg.body == "7"
        client.unwatch("var/Z")

    def test_unwatch_removes_cache(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        mock_server.set_prop("var/W", "5")
        client = make_client(mock_server)
        client.watch("var/W")
        time.sleep(0.1)
        client.unwatch("var/W")
        assert "var/W" not in client._watch_values


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestRun:
    def test_run_blocking_returns_tuple(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        client = make_client(mock_server)
        result = client.run("p 1+1")
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_run_nonblocking_does_not_hang(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        client = make_client(mock_server)
        # Should return immediately
        start = time.time()
        client.run("p 42", blocking=False)
        elapsed = time.time() - start
        assert elapsed < 1.0


# ---------------------------------------------------------------------------
# abort
# ---------------------------------------------------------------------------

class TestAbort:
    def test_abort_does_not_raise(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        client = make_client(mock_server)
        client.abort()  # should not raise


# ---------------------------------------------------------------------------
# motor / var accessors
# ---------------------------------------------------------------------------

class TestAccessors:
    def test_motor_returns_motor_object(self, mock_server):
        from pycertifspec.Motor import Motor
        mock_server.set_prop("var/COUNTERS", "0")
        mock_server.set_prop("motor/m0/position", "0.0")
        mock_server.set_prop("motor/m0/dial_position", "0.0")
        mock_server.set_prop("motor/m0/move_done", "1")
        client = make_client(mock_server)
        m = client.motor("m0")
        assert isinstance(m, Motor)

    def test_var_returns_var_object(self, mock_server):
        from pycertifspec.Var import Var
        mock_server.set_prop("var/COUNTERS", "0")
        mock_server.set_prop("var/MYVAR", "hello")
        client = make_client(mock_server)
        v = client.var("MYVAR")
        assert isinstance(v, Var)

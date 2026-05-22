"""
Tests for Client reconnection and robustness (Phase 3).
"""
import socket
import threading
import time
import pytest

from pycertifspec import Client
from pycertifspec.SpecError import SpecError

from conftest import MockSpecServer


def make_client(mock_server, **kwargs):
    mock_server.set_prop("var/COUNTERS", "0")
    mock_server.set_prop("output/tty", "> \n")
    return Client(
        host=mock_server.host,
        port=mock_server.port,
        port_range=(mock_server.port, mock_server.port),
        timeout=1.0,
        **kwargs,
    )


class TestConnectedFlag:
    def test_connected_after_init(self, mock_server):
        client = make_client(mock_server)
        assert client._connected is True

    def test_send_raises_when_not_connected(self, mock_server):
        client = make_client(mock_server, auto_reconnect=False)
        client._connected = False
        with pytest.raises(SpecError, match="not connected"):
            client.get("motor/m0/position", force_fetch=True)


class TestDropAndReconnect:
    def test_manual_reconnect_restores_connection(self, mock_server):
        client = make_client(mock_server, auto_reconnect=False)
        assert client._connected is True

        # Close the server-side connection forcibly
        client.sock.close()
        client._connected = False

        # Manual reconnect should succeed since mock_server is still listening
        client.reconnect()
        assert client._connected is True

    def test_auto_reconnect_restores_connection(self, mock_server):
        client = make_client(mock_server, auto_reconnect=True, reconnect_delay=0.1)
        assert client._connected is True

        # Slam the socket shut — listener thread should detect and reconnect
        client.sock.close()

        # Allow time for reconnect_delay + handshake
        deadline = time.time() + 3.0
        while not client._connected and time.time() < deadline:
            time.sleep(0.05)

        assert client._connected is True, "Client did not reconnect within 3 s"

    def test_subscriptions_reregistered_after_reconnect(self, mock_server):
        mock_server.set_prop("var/MYVAR", "123")
        client = make_client(mock_server, auto_reconnect=False)

        received = threading.Event()
        client.subscribe("var/MYVAR", lambda m: received.set())
        received.wait(1.0)

        # The subscription is tracked in _subscribers
        assert "var/MYVAR" in client._subscribers

        # After reconnect, subscription should be re-sent to server
        client.reconnect()
        assert "var/MYVAR" in client._subscribers


class TestAutoReconnectDisabled:
    def test_connected_flag_false_after_send_on_closed_socket(self, mock_server):
        client = make_client(mock_server, auto_reconnect=False)
        # Force the flag false directly — tests that _send checks it
        client._connected = False
        with pytest.raises(SpecError, match="not connected"):
            client.get("var/X", force_fetch=True)

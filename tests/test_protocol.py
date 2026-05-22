"""
Unit tests for SpecSocket — binary protocol pack/unpack and connect handshake.
"""
import struct
import socket
import threading
import time
import pytest

from pycertifspec.SpecSocket import SpecSocket, SpecMessage
from pycertifspec.EventTypes import EventTypes
from pycertifspec.DataTypes import DataTypes

from conftest import MockSpecServer, _pack_header, _send_reply, HEADER_SIZE, SV_SPEC_MAGIC


# ---------------------------------------------------------------------------
# Header round-trip
# ---------------------------------------------------------------------------

class TestHeaderPackUnpack:
    def test_magic_in_header(self):
        sock = SpecSocket()
        # Build a raw header by packing directly and checking magic bytes
        raw = _pack_header(sn=1, cmd=EventTypes.SV_HELLO, name="test", body=b"")
        magic = struct.unpack("I", raw[:4])[0]
        assert magic == SV_SPEC_MAGIC

    def test_header_size_constant(self):
        assert HEADER_SIZE == 132

    def test_name_truncated_to_80_bytes(self):
        long_name = "x" * 200
        raw = _pack_header(sn=1, cmd=EventTypes.SV_HELLO, name=long_name, body=b"")
        # name field starts at byte 52 (after IiIIIIiiIIIii = 13 fields before 80s)
        name_field = struct.unpack_from("80s", raw, HEADER_SIZE - 80)[0]
        decoded = name_field.decode("ascii").rstrip("\x00")
        assert len(decoded) <= 80


# ---------------------------------------------------------------------------
# recv_spec
# ---------------------------------------------------------------------------

class TestRecvSpec:
    """Test SpecSocket.recv_spec() by feeding it raw bytes via a loopback."""

    def _loopback_pair(self):
        """Return (client_sock, server_conn) as a connected pair."""
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        host, port = srv.getsockname()
        client = SpecSocket()
        client.connect((host, port))
        conn, _ = srv.accept()
        srv.close()
        return client, conn

    def test_recv_string_body(self):
        client, conn = self._loopback_pair()
        body = b"hello_spec"
        header = _pack_header(sn=7, cmd=EventTypes.SV_REPLY, data_type=DataTypes.SV_STRING, body=body)
        conn.sendall(header + body)
        conn.close()

        msg = client.recv_spec()
        assert msg.sn == 7
        assert msg.cmd == EventTypes.SV_REPLY
        assert msg.body == "hello_spec"
        client.close()

    def test_recv_binary_body(self):
        client, conn = self._loopback_pair()
        import numpy as np
        arr = np.array([1.0, 2.0, 3.0], dtype=np.double)
        body = arr.tobytes()
        header = _pack_header(
            sn=2, cmd=EventTypes.SV_EVENT,
            data_type=DataTypes.SV_ARR_DOUBLE,
            body=body, rows=1, cols=3,
        )
        conn.sendall(header + body)
        conn.close()

        msg = client.recv_spec()
        assert msg.type == DataTypes.SV_ARR_DOUBLE
        assert isinstance(msg.body, bytes)
        assert len(msg.body) == len(body)
        client.close()

    def test_wrong_magic_raises(self):
        client, conn = self._loopback_pair()
        bad = struct.pack("IiI", 0xDEADBEEF, 4, 132)
        bad += b"\x00" * (132 - 12)
        conn.sendall(bad)
        conn.close()

        with pytest.raises(ValueError, match="magic"):
            client.recv_spec()
        client.close()

    def test_old_protocol_raises(self):
        client, conn = self._loopback_pair()
        # version 3 should raise
        bad = struct.pack("IiI", SV_SPEC_MAGIC, 3, 132)
        bad += b"\x00" * (132 - 12)
        conn.sendall(bad)
        conn.close()

        with pytest.raises(Exception, match="protocol version"):
            client.recv_spec()
        client.close()

    def test_empty_string_body(self):
        client, conn = self._loopback_pair()
        body = b""
        header = _pack_header(sn=3, cmd=EventTypes.SV_REPLY, data_type=DataTypes.SV_STRING, body=body)
        conn.sendall(header + body)
        conn.close()

        msg = client.recv_spec()
        assert msg.body == ""
        client.close()

    def test_name_decoded_and_stripped(self):
        client, conn = self._loopback_pair()
        body = b"val"
        header = _pack_header(sn=1, cmd=EventTypes.SV_EVENT, name="motor/m0/position", body=body)
        conn.sendall(header + body)
        conn.close()

        msg = client.recv_spec()
        assert msg.name == "motor/m0/position"
        client.close()


# ---------------------------------------------------------------------------
# send_spec
# ---------------------------------------------------------------------------

class TestSendSpec:
    def _loopback_pair(self):
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        host, port = srv.getsockname()
        client = SpecSocket()
        client.connect((host, port))
        conn, _ = srv.accept()
        srv.close()
        return client, conn

    def test_send_requires_bytes_body(self):
        client, conn = self._loopback_pair()
        with pytest.raises(ValueError):
            client.send_spec(1, EventTypes.SV_HELLO, 0, "name", body="not bytes")
        client.close()
        conn.close()

    def test_sent_header_has_correct_magic(self):
        client, conn = self._loopback_pair()
        client.send_spec(5, EventTypes.SV_HELLO, 0, "pycertifspec", body=b"")
        raw = conn.recv(132)
        magic = struct.unpack("I", raw[:4])[0]
        assert magic == SV_SPEC_MAGIC
        client.close()
        conn.close()

    def test_sent_sn_round_trips(self):
        client, conn = self._loopback_pair()
        client.send_spec(42, EventTypes.SV_HELLO, 0, "", body=b"")
        raw = conn.recv(132)
        sn = struct.unpack_from("I", raw, 12)[0]
        assert sn == 42
        client.close()
        conn.close()


# ---------------------------------------------------------------------------
# connect_spec handshake
# ---------------------------------------------------------------------------

class TestConnectSpec:
    def test_hello_handshake(self, mock_server):
        sock = SpecSocket()
        port = sock.connect_spec(mock_server.host, port=mock_server.port)
        assert port == mock_server.port
        sock.close()

    def test_no_server_raises(self):
        # Port 1 is privileged and never runs SPEC
        sock = SpecSocket()
        with pytest.raises(Exception, match="No SPEC server"):
            sock.connect_spec("127.0.0.1", port_range=(19999, 19999), timeout=0.1)
        sock.close()

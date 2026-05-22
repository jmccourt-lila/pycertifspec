"""
MockSpecServer fixture — speaks the SPEC binary protocol over a loopback TCP socket.

The server runs in a daemon thread and processes one request at a time.
Tests use the `mock_server` fixture to get a (host, port) tuple they can
pass to Client or SpecSocket.
"""
import struct
import socket
import threading
import time
import collections
import pytest

SV_SPEC_MAGIC = 4277009102
SV_VERSION = 4
SV_NAME_LEN = 80
HEADER_FMT = "IiIIIIiiIIIii80s"
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 132 bytes

# Minimal namedtuple for server-side parsing
RawMsg = collections.namedtuple(
    "RawMsg", "magic vers size sn sec usec cmd type rows cols length err flags name body"
)


def _pack_header(sn, cmd, data_type=2, name="", body=b"", rows=0, cols=0, error=0, flags=0):
    name_b = name.encode("ascii")[:SV_NAME_LEN].ljust(SV_NAME_LEN, b"\x00")
    return struct.pack(
        HEADER_FMT,
        SV_SPEC_MAGIC,
        SV_VERSION,
        HEADER_SIZE,
        sn,
        int(time.time()),
        0,
        cmd,
        data_type,
        rows,
        cols,
        len(body),
        error,
        flags,
        name_b,
    )


def _recv_msg(conn):
    """Read one SPEC message from a connected socket."""
    head1 = b""
    while len(head1) < 12:
        chunk = conn.recv(12 - len(head1))
        if not chunk:
            return None
        head1 += chunk

    magic, vers, size = struct.unpack("IiI", head1)

    rest_len = size - 12
    head2 = b""
    while len(head2) < rest_len:
        chunk = conn.recv(rest_len - len(head2))
        if not chunk:
            return None
        head2 += chunk

    sn, sec, usec, cmd, dtype, rows, cols, length, err, flags = struct.unpack(
        "IIIiiIIIii", head2[: struct.calcsize("IIIiiIIIii")]
    )
    # skip padding bytes then read 80-byte name
    pad = size - HEADER_SIZE
    name_raw = head2[struct.calcsize("IIIiiIIIii") + max(0, pad):][:SV_NAME_LEN]
    name = name_raw.decode("utf-8").rstrip("\x00")

    body = b""
    bleft = length
    while bleft > 0:
        chunk = conn.recv(min(4096, bleft))
        if not chunk:
            break
        body += chunk
        bleft -= len(chunk)

    return RawMsg(magic, vers, size, sn, sec, usec, cmd, dtype, rows, cols, length, err, flags, name, body)


def _send_reply(conn, sn, cmd, name="", body=b"", data_type=2):
    if isinstance(body, str):
        body = body.encode("utf-8")
    header = _pack_header(sn, cmd, data_type=data_type, name=name, body=body)
    conn.sendall(header + body)


class MockSpecServer:
    """
    Minimal loopback SPEC server for testing.

    Handlers dict maps (cmd, name_prefix) → callable(msg) → (reply_cmd, name, body, data_type).
    Pre-registered defaults handle SV_HELLO and a configurable property store.
    """

    # SPEC command codes (duplicated here to avoid importing from package under test)
    SV_HELLO = 14
    SV_HELLO_REPLY = 15
    SV_REGISTER = 6
    SV_UNREGISTER = 7
    SV_EVENT = 8
    SV_CHAN_READ = 11
    SV_CHAN_SEND = 12
    SV_REPLY = 13
    SV_FUNC_WITH_RETURN = 10
    SV_FUNC = 9
    SV_ABORT = 2

    def __init__(self):
        self._props = {}          # name → (body_bytes, data_type)
        self._subscriptions = {}  # name → list of (conn, sn) to notify on set
        self._lock = threading.Lock()
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self._server.listen(1)
        self._server.settimeout(2.0)
        self.host, self.port = self._server.getsockname()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def set_prop(self, name, value, data_type=2):
        """Pre-populate a property (call from test setup)."""
        if isinstance(value, str):
            value = value.encode("utf-8")
        with self._lock:
            self._props[name] = (value, data_type)

    def _accept_loop(self):
        while True:
            try:
                conn, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        conn.settimeout(5.0)
        try:
            while True:
                msg = _recv_msg(conn)
                if msg is None:
                    break
                self._dispatch(conn, msg)
        except Exception:
            pass
        finally:
            conn.close()

    def _dispatch(self, conn, msg):
        if msg.cmd == self.SV_HELLO:
            _send_reply(conn, msg.sn, self.SV_HELLO_REPLY, name="mock_spec", body=b"mock_spec")
            return

        if msg.cmd == self.SV_REGISTER:
            with self._lock:
                val, dtype = self._props.get(msg.name, (None, 2))
                subs = self._subscriptions.setdefault(msg.name, [])
                subs.append(conn)
            # Real SPEC only sends the initial event if a value exists.
            # Never send an initial event for "error" (no current error).
            if val is not None and msg.name != "error":
                _send_reply(conn, msg.sn, self.SV_EVENT, name=msg.name, body=val, data_type=dtype)
            return

        if msg.cmd == self.SV_UNREGISTER:
            with self._lock:
                subs = self._subscriptions.get(msg.name, [])
                if conn in subs:
                    subs.remove(conn)
            return

        if msg.cmd == self.SV_CHAN_READ:
            with self._lock:
                val, dtype = self._props.get(msg.name, (None, 2))
            if val is None:
                _send_reply(conn, msg.sn, self.SV_REPLY, name=msg.name, body=b"", data_type=3)  # SV_ERROR
            else:
                _send_reply(conn, msg.sn, self.SV_REPLY, name=msg.name, body=val, data_type=dtype)
            return

        if msg.cmd == self.SV_CHAN_SEND:
            with self._lock:
                self._props[msg.name] = (msg.body, msg.type)
                subs = list(self._subscriptions.get(msg.name, []))
            for sub_conn in subs:
                try:
                    _send_reply(sub_conn, 0, self.SV_EVENT, name=msg.name, body=msg.body, data_type=msg.type)
                except Exception:
                    pass
            return

        if msg.cmd in (self.SV_FUNC_WITH_RETURN, self.SV_FUNC):
            # SV_FUNC_WITH_RETURN sends command in body; SV_FUNC sends in name field
            if msg.body:
                cmd_str = msg.body.decode("utf-8").strip()
            else:
                cmd_str = msg.name.strip()
            response = self._handle_func(cmd_str)
            if msg.cmd == self.SV_FUNC_WITH_RETURN:
                _send_reply(conn, msg.sn, self.SV_REPLY, body=response.encode("utf-8"))
            return

        if msg.cmd == self.SV_ABORT:
            return

    def _handle_func(self, cmd):
        """Return a string response for console commands. Override in subclasses."""
        return ""

    def stop(self):
        self._server.close()


@pytest.fixture
def mock_server():
    server = MockSpecServer()
    yield server
    server.stop()

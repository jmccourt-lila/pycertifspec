"""
Tests for Scan — parsing SPEC scan output and streaming data points.
"""
import threading
import time
import pytest
import numpy as np

from pycertifspec import Client, Scan
from pycertifspec.SpecError import SpecError

from conftest import MockSpecServer, _send_reply


# ---------------------------------------------------------------------------
# Realistic SPEC scan output fragments
# ---------------------------------------------------------------------------

_ASCAN_OUTPUT = """\

#S 1  ascan m0 0 10 20 0.5
#D Wed May 22 00:00:00 2026
#T 0.5  (Seconds)
#N 3
#L  m0  sec  det
0.0  0.5  100.0
0.5  0.5  110.0
1.0  0.5  120.0
1.5  0.5  130.0
2.0  0.5  140.0

"""

_EXPECTED_POINTS = [
    {"m0": 0.0,  "sec": 0.5, "det": 100.0},
    {"m0": 0.5,  "sec": 0.5, "det": 110.0},
    {"m0": 1.0,  "sec": 0.5, "det": 120.0},
    {"m0": 1.5,  "sec": 0.5, "det": 130.0},
    {"m0": 2.0,  "sec": 0.5, "det": 140.0},
]


class ScanMockServer(MockSpecServer):
    """MockSpecServer that streams scan output line-by-line via output/tty events."""

    def _handle_func(self, cmd):
        if cmd.startswith("ascan") or cmd.startswith("dscan") or cmd.startswith("mesh"):
            threading.Thread(target=self._stream_scan, daemon=True).start()
        return ""

    def _stream_scan(self):
        time.sleep(0.05)  # slight delay to let tty subscription register
        with self._lock:
            tty_subs = list(self._subscriptions.get("output/tty", []))

        for line in _ASCAN_OUTPUT.split("\n"):
            chunk = line + "\n"
            for conn in tty_subs:
                try:
                    _send_reply(conn, 0, self.SV_EVENT, name="output/tty", body=chunk.encode())
                except Exception:
                    pass
            time.sleep(0.005)

        # Emit the prompt that signals command completion
        prompt = "> \n"
        for conn in tty_subs:
            try:
                _send_reply(conn, 0, self.SV_EVENT, name="output/tty", body=prompt.encode())
            except Exception:
                pass


@pytest.fixture
def scan_server():
    server = ScanMockServer()
    yield server
    server.stop()


def make_client(server):
    server.set_prop("var/COUNTERS", "0")
    server.set_prop("output/tty", "> \n")
    return Client(
        host=server.host,
        port=server.port,
        port_range=(server.port, server.port),
        timeout=1.0,
    )


# ---------------------------------------------------------------------------
# Unit tests for parsing helpers
# ---------------------------------------------------------------------------

from pycertifspec.Scan import _parse_header_cols, _try_parse_row, _dispatch_line


class TestParseHelpers:
    def test_parse_header_cols_basic(self):
        assert _parse_header_cols("#L  m0  sec  det") == ["m0", "sec", "det"]

    def test_parse_header_cols_single(self):
        assert _parse_header_cols("#L  pos") == ["pos"]

    def test_parse_header_cols_non_header(self):
        assert _parse_header_cols("0.0  0.5  100.0") == []

    def test_try_parse_row_valid(self):
        result = _try_parse_row("0.0  0.5  100.0", ["m0", "sec", "det"])
        assert result == {"m0": 0.0, "sec": 0.5, "det": 100.0}

    def test_try_parse_row_wrong_col_count(self):
        assert _try_parse_row("0.0  0.5", ["m0", "sec", "det"]) is None

    def test_try_parse_row_non_numeric(self):
        assert _try_parse_row("hello world there", ["m0", "sec", "det"]) is None

    def test_dispatch_line_ignores_before_scan_start(self):
        state = {"phase": "waiting", "headers": [], "buf": ""}
        points = []
        _dispatch_line("#L  m0  sec  det", state, points.append)
        assert state["headers"] == []  # not yet in scan
        assert points == []

    def test_dispatch_line_sets_in_scan_on_S_line(self):
        state = {"phase": "waiting", "headers": [], "buf": ""}
        _dispatch_line("#S 1  ascan m0 0 10 20 0.5", state, lambda p: None)
        assert state["phase"] == "in_scan"

    def test_dispatch_line_captures_headers(self):
        state = {"phase": "in_scan", "headers": [], "buf": ""}
        _dispatch_line("#L  m0  sec  det", state, lambda p: None)
        assert state["headers"] == ["m0", "sec", "det"]

    def test_dispatch_line_yields_data_point(self):
        state = {"phase": "in_scan", "headers": ["m0", "sec", "det"], "buf": ""}
        points = []
        _dispatch_line("0.0  0.5  100.0", state, points.append)
        assert points == [{"m0": 0.0, "sec": 0.5, "det": 100.0}]

    def test_dispatch_line_skips_comment_in_scan(self):
        state = {"phase": "in_scan", "headers": ["m0", "sec"], "buf": ""}
        points = []
        _dispatch_line("#C some comment", state, points.append)
        assert points == []


# ---------------------------------------------------------------------------
# Integration tests against ScanMockServer
# ---------------------------------------------------------------------------

class TestScanRun:
    def test_scan_returns_correct_points(self, scan_server):
        client = make_client(scan_server)
        scan = client.scan("ascan m0 0 10 20 0.5")
        data = scan.run()
        assert data == _EXPECTED_POINTS

    def test_scan_callback_called_per_point(self, scan_server):
        client = make_client(scan_server)
        scan = client.scan("ascan m0 0 10 20 0.5")
        received = []
        data = scan.run(callback=received.append)
        assert len(received) == len(_EXPECTED_POINTS)
        assert received == _EXPECTED_POINTS

    def test_scan_iter_yields_all_points(self, scan_server):
        client = make_client(scan_server)
        scan = client.scan("ascan m0 0 10 20 0.5")
        points = list(scan.run_iter())
        assert points == _EXPECTED_POINTS

    def test_scan_via_client_factory(self, scan_server):
        client = make_client(scan_server)
        scan = client.scan("ascan m0 0 10 20 0.5")
        assert isinstance(scan, Scan)

    def test_as_numpy_shapes(self, scan_server):
        client = make_client(scan_server)
        data = client.scan("ascan m0 0 10 20 0.5").run()
        arrays = Scan(client, "dummy").as_numpy(data)
        assert set(arrays.keys()) == {"m0", "sec", "det"}
        assert arrays["m0"].shape == (5,)
        assert arrays["m0"].dtype == np.float64
        np.testing.assert_array_equal(arrays["m0"], [0.0, 0.5, 1.0, 1.5, 2.0])

    def test_as_numpy_empty(self, scan_server):
        client = make_client(scan_server)
        result = Scan(client, "dummy").as_numpy([])
        assert result == {}


class TestScanConstruction:
    def test_invalid_client_raises(self, scan_server):
        with pytest.raises(ValueError, match="client"):
            Scan("not_a_client", "ascan m0 0 10 20 0.5")

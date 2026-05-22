"""
Tests for the CLI REPL (Phase 6).
"""
import sys
import pytest
from unittest.mock import patch, MagicMock

from pycertifspec.cli import main


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

class TestArgParsing:
    def test_help_exits_zero(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["--help"])
        assert exc_info.value.code == 0

    def test_bad_port_range_exits(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["--port-range", "notarange"])
        assert exc_info.value.code != 0

    def test_defaults_pass_through(self):
        """Parsing with no args should not raise before attempting connection."""
        # Patch Client so we don't actually connect
        with patch("pycertifspec.cli.Client") as MockClient:
            MockClient.side_effect = Exception("no server")
            with pytest.raises(SystemExit) as exc_info:
                main([])
            assert exc_info.value.code == 1

    def test_host_arg_forwarded(self):
        with patch("pycertifspec.cli.Client") as MockClient:
            MockClient.side_effect = Exception("no server")
            with pytest.raises(SystemExit):
                main(["--host", "10.0.0.5"])
            call_kwargs = MockClient.call_args
            assert call_kwargs.kwargs["host"] == "10.0.0.5"

    def test_port_arg_forwarded(self):
        with patch("pycertifspec.cli.Client") as MockClient:
            MockClient.side_effect = Exception("no server")
            with pytest.raises(SystemExit):
                main(["--port", "6515"])
            assert MockClient.call_args.kwargs["port"] == 6515

    def test_no_reconnect_flag(self):
        with patch("pycertifspec.cli.Client") as MockClient:
            MockClient.side_effect = Exception("no server")
            with pytest.raises(SystemExit):
                main(["--no-reconnect"])
            assert MockClient.call_args.kwargs["auto_reconnect"] is False

    def test_port_range_parsed(self):
        with patch("pycertifspec.cli.Client") as MockClient:
            MockClient.side_effect = Exception("no server")
            with pytest.raises(SystemExit):
                main(["--port-range", "6520-6525"])
            assert MockClient.call_args.kwargs["port_range"] == (6520, 6525)


# ---------------------------------------------------------------------------
# Integration: successful connection launches REPL namespace
# ---------------------------------------------------------------------------

class TestCliIntegration:
    def test_repl_namespace_contains_expected_names(self, mock_server):
        """Connect to mock server and verify local namespace is correct."""
        mock_server.set_prop("var/COUNTERS", "0")
        mock_server.set_prop("output/tty", "> \n")

        captured_locals = {}

        def fake_interact(banner, local, exitmsg):
            captured_locals.update(local)

        with patch("pycertifspec.cli.code.interact", side_effect=fake_interact):
            main([
                "--host", mock_server.host,
                "--port", str(mock_server.port),
            ])

        assert "client" in captured_locals
        assert "motor" in captured_locals
        assert "var" in captured_locals
        assert "scan" in captured_locals
        assert "count" in captured_locals
        assert "abort" in captured_locals
        assert "motors" in captured_locals
        assert "counters" in captured_locals

    def test_banner_contains_host(self, mock_server):
        mock_server.set_prop("var/COUNTERS", "0")
        mock_server.set_prop("output/tty", "> \n")

        banners = []

        def fake_interact(banner, local, exitmsg):
            banners.append(banner)

        with patch("pycertifspec.cli.code.interact", side_effect=fake_interact):
            main([
                "--host", mock_server.host,
                "--port", str(mock_server.port),
            ])

        assert mock_server.host in banners[0]

    def test_connection_failure_exits_1(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["--host", "127.0.0.1", "--port-range", "19996-19996", "--timeout", "0.1"])
        assert exc_info.value.code == 1

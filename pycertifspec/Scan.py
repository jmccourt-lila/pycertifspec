import re
import threading
import numpy as np
from queue import Queue
from typing import Callable, Dict, List, Optional, Any

from .SpecError import SpecError


_SCAN_START_RE = re.compile(r'^#S\s')
_HEADER_RE = re.compile(r'^#L\s+(.+)')


def _parse_header_cols(line: str) -> List[str]:
    """Parse '#L col1  col2  col3' into ['col1', 'col2', 'col3']."""
    m = _HEADER_RE.match(line)
    if not m:
        return []
    return re.split(r'  +', m.group(1).strip())


def _try_parse_row(line: str, headers: List[str]) -> Optional[Dict[str, float]]:
    """Return a dict if line is a numeric data row matching headers, else None."""
    parts = line.split()
    if len(parts) != len(headers):
        return None
    try:
        values = [float(p) for p in parts]
    except ValueError:
        return None
    return dict(zip(headers, values))


class Scan:
    """
    Run a SPEC scan macro and parse the console output into structured data.

    Usage::

        scan = Scan(client, "ascan m0 0 10 20 0.5")
        data = scan.run()               # list of dicts, one per point
        for point in scan.run_iter():   # streaming generator
            print(point)
        scan.abort()                    # stop a running scan

    Or use the convenience factory::

        scan = client.scan("ascan m0 0 10 20 0.5")

    Each data point is a dict keyed by the column names from the ``#L`` header
    line in SPEC's output.  Use :meth:`as_numpy` to convert to numpy arrays.
    """

    def __init__(self, client, command: str):
        from .Client import Client
        if not isinstance(client, Client):
            raise ValueError("client must be a pycertifspec.Client instance")
        self._client = client
        self._command = command.strip()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, callback: Optional[Callable[[Dict[str, float]], None]] = None) -> List[Dict[str, float]]:
        """
        Execute the scan, block until complete, and return all data points.

        Parameters:
            callback: Optional callable invoked with each point dict as it arrives.

        Returns:
            list of dicts — one per scan point, keyed by column names.
        """
        data: List[Dict[str, float]] = []
        done = threading.Event()

        def on_point(point):
            data.append(point)
            if callback:
                callback(point)

        self._execute(on_point, done.set)
        done.wait()
        return data

    def run_iter(self):
        """
        Execute the scan and yield each data point as it arrives.

        Yields:
            dict: One data point per yield, keyed by column names.
        """
        q: Queue = Queue()
        _sentinel = object()

        def on_point(point):
            q.put(point)

        self._execute(on_point, lambda: q.put(_sentinel))

        while True:
            item = q.get()
            if item is _sentinel:
                break
            yield item

    def abort(self):
        """Abort the currently running scan."""
        self._client.abort()

    def as_numpy(self, data: List[Dict[str, float]]) -> Dict[str, np.ndarray]:
        """
        Convert :meth:`run` output to a dict of numpy arrays, one per column.

        Parameters:
            data: Output of :meth:`run`.

        Returns:
            dict mapping column name → 1-D ``np.ndarray`` of float64.
        """
        if not data:
            return {}
        return {k: np.array([row[k] for row in data], dtype=np.float64) for k in data[0]}

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _execute(self, on_point: Callable, on_done: Callable):
        """Subscribe to output/tty, fire the command, dispatch events."""
        state: Dict[str, Any] = {
            "phase": "waiting",   # waiting → in_scan → done
            "headers": [],
            "buf": "",
        }

        def tty_cb(msg):
            # Accumulate into line buffer and process complete lines
            state["buf"] += msg.body
            lines = state["buf"].split("\n")
            state["buf"] = lines[-1]  # incomplete final fragment

            for line in lines[:-1]:
                _dispatch_line(line.rstrip("\r"), state, on_point)

            # SPEC emits the prompt as the last fragment of a command's output
            if msg.body.endswith("> \n") and state["phase"] == "in_scan":
                # Flush any buffered partial line before declaring done
                if state["buf"].strip():
                    _dispatch_line(state["buf"].strip(), state, on_point)
                    state["buf"] = ""
                state["phase"] = "done"
                self._client.unsubscribe("output/tty", tty_cb)
                on_done()

        self._client.subscribe("output/tty", tty_cb, nowait=True)
        self._client.run(self._command, blocking=False)


def _dispatch_line(line: str, state: Dict[str, Any], on_point: Callable):
    """Process one complete output line, updating state and calling on_point."""
    if not line:
        return

    if _SCAN_START_RE.match(line):
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
        on_point(row)

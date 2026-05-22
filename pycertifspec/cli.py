"""
Interactive REPL for pycertifspec.

Launch with::

    python -m pycertifspec
    python -m pycertifspec --host 10.0.0.5
    python -m pycertifspec --host localhost --port 6510

The REPL drops you into a standard Python shell with a live Client already
connected. The following names are pre-bound in the local namespace:

    client      — the Client instance
    motor(mne)  — shorthand for client.motor(mne)
    var(name)   — shorthand for client.var(name)
    scan(cmd)   — shorthand for client.scan(cmd)
    count(t)    — shorthand for client.count(t)
    abort()     — shorthand for client.abort()
    motors      — property: list of all motor mnemonics
    counters    — property: list of all counter mnemonics

Example session::

    [pycertifspec] Connected on localhost:6512
    >>> m0 = motor("m0")
    >>> m0.position
    12.4
    >>> m0.moveto(15)
    >>> count(1.0)
    {'sec': 1.0, 'det': 423.0}
    >>> data = scan("ascan m0 0 10 20 0.5").run()
"""

import argparse
import code
import sys
import textwrap

from pycertifspec import Client


def _banner(host: str, port: int, client, motor_list: list) -> str:
    motors_str = ", ".join(motor_list[:8]) or "(none)"
    counter_list = ", ".join(list(client.counter_names.keys())) or "(none)"
    return textwrap.dedent(f"""
    ╔══════════════════════════════════════════════════════╗
    ║              pycertifspec interactive shell          ║
    ╠══════════════════════════════════════════════════════╣
    ║  Connected : {host}:{port:<38}║
    ║  Motors    : {motors_str:<39}║
    ║  Counters  : {counter_list:<39}║
    ╠══════════════════════════════════════════════════════╣
    ║  Names: client  motor()  var()  scan()               ║
    ║         count()  abort()  motors  counters           ║
    ╚══════════════════════════════════════════════════════╝
    """).strip()


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python -m pycertifspec",
        description="Interactive REPL for a live SPEC instrument connection.",
    )
    parser.add_argument("--host", default="localhost", help="SPEC server host (default: localhost)")
    parser.add_argument("--port", type=int, default=None, help="Exact port (skips scan)")
    parser.add_argument(
        "--port-range",
        default="6510-6530",
        metavar="START-END",
        help="Port range to scan (default: 6510-6530)",
    )
    parser.add_argument("--timeout", type=float, default=0.5, help="Per-port timeout in seconds")
    parser.add_argument("--no-reconnect", action="store_true", help="Disable auto-reconnect")
    args = parser.parse_args(argv)

    try:
        start, end = (int(x) for x in args.port_range.split("-"))
    except ValueError:
        parser.error(f"--port-range must be START-END, got: {args.port_range!r}")

    print(f"[pycertifspec] Connecting to {args.host} …", end=" ", flush=True)
    try:
        client = Client(
            host=args.host,
            port=args.port,
            port_range=(start, end),
            timeout=args.timeout,
            auto_reconnect=not args.no_reconnect,
        )
    except Exception as exc:
        print(f"FAILED\n[pycertifspec] {exc}", file=sys.stderr)
        sys.exit(1)

    # Figure out which port we landed on
    connected_port = client.sock.getpeername()[1]
    print("OK")

    try:
        motor_list = client.motors
    except Exception:
        motor_list = []

    local = {
        "client":   client,
        "motor":    client.motor,
        "var":      client.var,
        "scan":     client.scan,
        "count":    client.count,
        "abort":    client.abort,
        "motors":   motor_list,
        "counters": list(client.counter_names.keys()),
    }

    banner = _banner(args.host, connected_port, client, motor_list)

    try:
        code.interact(banner=banner, local=local, exitmsg="[pycertifspec] Goodbye.")
    except SystemExit:
        pass

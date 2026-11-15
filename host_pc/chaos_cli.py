"""Milestone 7 - command-line fault injector for live demonstrations on the real VMs.

Examples (default targets come from host_client.py; override with --node name=url):

    python chaos_cli.py status
    python chaos_cli.py inject node1 service                    # /health -> 503, API -> 500
    python chaos_cli.py inject node1 omission --probability 0.7 --duration 10
    python chaos_cli.py inject node1 delay --delay-ms 1500 --duration 8
    python chaos_cli.py inject node1 partition --duration 8     # cut node1 <-> node2 link
    python chaos_cli.py inject node2 byzantine --mode lying_ack
    python chaos_cli.py inject node1 crash                      # process exits (fail-stop)
    python chaos_cli.py clear node1
    python chaos_cli.py scenario node_failure                   # named scenario from fault_injector.SCENARIOS
    python chaos_cli.py list

Run ft_master.py in another terminal to watch detection, failover and recovery happen.
"""
import argparse
import json
import sys

import requests

from fault_injector import SCENARIOS, ChaosClient, FaultInjector


def default_nodes():
    try:
        import host_client
        return {"node1": host_client.NODE1_URL, "node2": host_client.NODE2_URL}
    except (ImportError, AttributeError):
        return {"node1": "http://127.0.0.1:5001", "node2": "http://127.0.0.1:5002"}


def main():
    p = argparse.ArgumentParser(description="Milestone 7 fault injector")
    p.add_argument("--node", action="append", help="name=url (repeatable)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", help="show the active faults on every node")
    sub.add_parser("list", help="list the named scenarios")
    inj = sub.add_parser("inject", help="inject one fault")
    inj.add_argument("node")
    inj.add_argument("fault", choices=["crash", "service", "omission", "delay", "partition", "byzantine"])
    inj.add_argument("--duration", type=float, help="seconds; the fault then expires by itself")
    inj.add_argument("--probability", type=float, help="omission: fraction of requests dropped")
    inj.add_argument("--delay-ms", type=float, help="delay: added latency")
    inj.add_argument("--mode", help="byzantine: corrupt | collude | lying_ack")
    clr = sub.add_parser("clear", help="remove injected faults")
    clr.add_argument("node")
    clr.add_argument("--fault")
    scn = sub.add_parser("scenario", help="run a named scenario (see 'list')")
    scn.add_argument("name", choices=list(SCENARIOS))
    args = p.parse_args()

    nodes = dict(i.split("=", 1) for i in args.node) if args.node else default_nodes()
    chaos = ChaosClient()
    try:
        if args.cmd == "list":
            for s in SCENARIOS.values():
                print(f"{s.key:<18} {s.taxonomy:<34} {s.description}")
        elif args.cmd == "status":
            for name, url in nodes.items():
                try:
                    print(name, json.dumps(chaos.status(url)["active"]) or "{}")
                except requests.exceptions.RequestException as exc:
                    print(name, f"UNREACHABLE ({type(exc).__name__})")
        elif args.cmd == "inject":
            params = {k: v for k, v in {"duration": args.duration, "probability": args.probability,
                                         "delay_ms": args.delay_ms, "mode": args.mode}.items() if v is not None}
            try:
                print(json.dumps(chaos.inject(nodes[args.node], args.fault, **params)))
            except requests.exceptions.RequestException:
                if args.fault != "crash":
                    raise
                print("crash injected (connection closed by the dying node)")
        elif args.cmd == "clear":
            print(json.dumps(chaos.clear(nodes[args.node], args.fault)))
        elif args.cmd == "scenario":
            scn_obj = SCENARIOS[args.name]
            if scn_obj.fault is None:
                sys.exit(f"'{args.name}' needs process control (SIGKILL); it is only available in the local "
                         f"experiments. On the VMs: sudo systemctl kill -s KILL worker, or power the VM off.")
            t = FaultInjector(nodes).inject(scn_obj)
            print(f"injected '{scn_obj.key}' ({scn_obj.taxonomy}) on {scn_obj.target} at {t:.3f}")
    except KeyError as exc:
        sys.exit(f"unknown node {exc}; known: {', '.join(nodes)}")
    except requests.exceptions.RequestException as exc:
        sys.exit(f"request failed: {exc}")


if __name__ == "__main__":
    main()

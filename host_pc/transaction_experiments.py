"""Milestone 5 measurements: latency, throughput, failure handling and recovery."""
import argparse
import json

from transaction_coordinator import recover, run_experiment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol", choices=("2pc", "3pc", "both"), default="both")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--failure", action="store_true", help="Make one participant unreachable after an earlier participant has prepared")
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be at least 1")
    protocols = ("2pc", "3pc") if args.protocol == "both" else (args.protocol,)
    for protocol in protocols:
        print(json.dumps(run_experiment(protocol, args.count, args.failure), indent=2))
    print("Recovery outcomes:")
    print(json.dumps(recover(), indent=2))


if __name__ == "__main__":
    main()

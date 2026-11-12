"""Run durable two-phase commit transactions across the configured participants."""
import time
from transaction_coordinator import execute, recover, run_experiment


def execute_2pc(slice_data):
    return execute("2pc", slice_data)


def main():
    print("2PC recovery:", recover())
    print("2PC experiment:", run_experiment("2pc", count=5))


if __name__ == "__main__":
    main()

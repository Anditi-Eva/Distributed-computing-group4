"""Run durable three-phase commit transactions across the configured participants."""
from transaction_coordinator import execute, recover, run_experiment


def execute_3pc(slice_data):
    return execute("3pc", slice_data)


def main():
    print("3PC recovery:", recover())
    print("3PC experiment:", run_experiment("3pc", count=5))


if __name__ == "__main__":
    main()

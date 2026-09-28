"""Run one client delivery manually. Mirrors scripts/run_ingestion.py's
argparse dispatch pattern."""

import argparse
import sys

from src.delivery import pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a MerchantMCC client data delivery")
    parser.add_argument("client_id")
    parser.add_argument("dataset_name")
    parser.add_argument("--formats", nargs="*", default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    result = pipeline.run(
        client_id=args.client_id,
        dataset_name=args.dataset_name,
        formats=args.formats,
        force=args.force,
    )
    print(result)


if __name__ == "__main__":
    sys.exit(main())

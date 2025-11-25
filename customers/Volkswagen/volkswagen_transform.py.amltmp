

import argparse
import os
from customers.common_silver_engine import run_silver_transform


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--customer_name")
    parser.add_argument("--bronze_path")
    parser.add_argument("--silver_output")
    args = parser.parse_args()

    here = os.path.dirname(__file__)
    config_path = os.path.join(here, "volkswagen_silver_config.json")

    print("===== VOLKSWAGEN SILVER TRANSFORM WRAPPER =====")
    print("Customer:", args.customer_name)
    print("Bronze Path:", args.bronze_path)
    print("Silver Output:", args.silver_output)
    print("Using Config:", config_path)

    run_silver_transform(
        customer_name=args.customer_name,
        bronze_path=args.bronze_path,
        silver_output=args.silver_output,
        config_path=config_path,
    )

    print("===== VOLKSWAGEN SILVER TRANSFORM COMPLETED =====")

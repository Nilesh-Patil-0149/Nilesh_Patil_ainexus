import argparse
import os
from customers.common_silver_engine import run_silver_transform



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--customer_name")
    parser.add_argument("--bronze_path")
    parser.add_argument("--silver_output")
    args = parser.parse_args()

    # Path of CURRENT folder (customers/hyundai/)
    here = os.path.dirname(__file__)

    # Hyundai-specific config file
    config_path = os.path.join(here, "hyundai_silver_config.json")

    print("===== HYUNDAI SILVER TRANSFORM WRAPPER =====")
    print("Customer Name:", args.customer_name)
    print("Bronze Path:", args.bronze_path)
    print("Silver Output Path:", args.silver_output)
    print("Config Loaded From:", config_path)

    # Call the shared 80% engine
    run_silver_transform(
        customer_name=args.customer_name,
        bronze_path=args.bronze_path,
        silver_output=args.silver_output,
        config_path=config_path,
    )

    print("===== HYUNDAI SILVER TRANSFORM COMPLETED =====")

import os
import pandas as pd
import logging
import pyarrow as pa
from generate_data import generate_user_profile, generate_product, generate_transaction

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Get the number of records to generate from an environment variable
num_records = int(os.getenv("NUM_RECORDS", 10000))  # Default to 10000 if not set


def generate_data():
    logging.info("Generating user profiles.")
    user_profiles = [generate_user_profile() for _ in range(num_records)]
    user_ids = [user["user_id"] for user in user_profiles]

    logging.info("Generating products.")
    products = [generate_product() for _ in range(num_records)]
    product_ids = [product["product_id"] for product in products]

    logging.info("Generating transactions.")
    transactions = [
        generate_transaction(user_ids, product_ids) for _ in range(num_records)
    ]

    logging.info("Converting generated data to DataFrame.")
    return pd.DataFrame(transactions)


def export_data(df):
    logging.info("Exporting data to CSV, Parquet, and Arrow formats.")

    # CSV
    df.to_csv("data.csv", index=False)
    logging.info("CSV export complete.")

    # Parquet
    df.to_parquet("data.parquet", index=False)
    logging.info("Parquet export complete.")

    # Arrow
    table = pa.Table.from_pandas(df)
    with pa.ipc.new_file("data.arrow", table.schema) as writer:
        writer.write(table)
    logging.info("Arrow export complete.")


if __name__ == "__main__":
    df = generate_data()
    export_data(df)
    logging.info(f"Running benchmark with {num_records} rows")

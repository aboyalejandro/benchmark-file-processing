import logging
import time
import pandas as pd
import polars as pl
import duckdb
import pyarrow as pa

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def benchmark_file_reading():
    # File paths
    csv_file = "data.csv"
    parquet_file = "data.parquet"
    arrow_file = "data.arrow"

    # Polars
    logging.info("Benchmarking Polars:")
    start_time = time.time()
    df_polars_csv = pl.read_csv(csv_file)
    csv_time = time.time() - start_time
    logging.info(f"Polars CSV read time: {csv_time:.4f} seconds")

    start_time = time.time()
    df_polars_parquet = pl.read_parquet(parquet_file)
    parquet_time = time.time() - start_time
    logging.info(f"Polars Parquet read time: {parquet_time:.4f} seconds")

    start_time = time.time()
    df_polars_arrow = pl.read_ipc(arrow_file)
    arrow_time = time.time() - start_time
    logging.info(f"Polars Arrow read time: {arrow_time:.4f} seconds")

    # DuckDB
    logging.info("Benchmarking DuckDB:")
    conn = duckdb.connect(":memory:")

    start_time = time.time()
    result = conn.execute(f"SELECT * FROM read_csv_auto('{csv_file}')")
    csv_time = time.time() - start_time
    logging.info(f"DuckDB CSV read time: {csv_time:.4f} seconds")

    start_time = time.time()
    result = conn.execute(f"SELECT * FROM parquet_scan('{parquet_file}')")
    parquet_time = time.time() - start_time
    logging.info(f"DuckDB Parquet read time: {parquet_time:.4f} seconds")

    # For Arrow, we'll use pyarrow to read the file and then pass it to DuckDB
    start_time = time.time()
    arrow_table = pa.ipc.open_file(arrow_file).read_all()
    result = conn.execute("SELECT * FROM arrow_table")
    arrow_time = time.time() - start_time
    logging.info(f"DuckDB Arrow read time: {arrow_time:.4f} seconds")

    # Pandas
    logging.info("Benchmarking Pandas:")
    start_time = time.time()
    df_pandas_csv = pd.read_csv(csv_file)
    csv_time = time.time() - start_time
    logging.info(f"Pandas CSV read time: {csv_time:.4f} seconds")

    start_time = time.time()
    df_pandas_parquet = pd.read_parquet(parquet_file)
    parquet_time = time.time() - start_time
    logging.info(f"Pandas Parquet read time: {parquet_time:.4f} seconds")

    start_time = time.time()
    df_pandas_arrow = pd.read_feather(arrow_file)
    arrow_time = time.time() - start_time
    logging.info(f"Pandas Arrow read time: {arrow_time:.4f} seconds")


if __name__ == "__main__":
    benchmark_file_reading()

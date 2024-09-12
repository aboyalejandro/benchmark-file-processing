import logging
import time
import pandas as pd
import polars as pl
import duckdb
import pyarrow as pa


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def benchmark_file_processing():
    # File paths
    csv_file = "data.csv"
    parquet_file = "data.parquet"
    arrow_file = "data.arrow"

    # Polars CSV
    logging.info("Benchmarking Polars:")
    start_time = time.time()
    df_polars_csv = pl.read_csv(csv_file)
    csv_time = time.time() - start_time
    logging.info(f"Polars CSV read time: {csv_time:.4f} seconds")

    write_start_time = time.time()
    df_polars_csv.write_csv("data_output_polars.csv")
    csv_write_time = time.time() - write_start_time
    logging.info(f"Polars CSV write time: {csv_write_time:.4f} seconds")

    # Polars Parquet
    start_time = time.time()
    df_polars_parquet = pl.read_parquet(parquet_file)
    parquet_time = time.time() - start_time
    logging.info(f"Polars Parquet read time: {parquet_time:.4f} seconds")

    write_start_time = time.time()
    df_polars_parquet.write_parquet("data_output_polars.parquet")
    parquet_write_time = time.time() - write_start_time
    logging.info(f"Polars Parquet write time: {parquet_write_time:.4f} seconds")

    # Polars Arrow
    start_time = time.time()
    df_polars_arrow = pl.read_ipc(arrow_file)
    arrow_time = time.time() - start_time
    logging.info(f"Polars Arrow read time: {arrow_time:.4f} seconds")

    write_start_time = time.time()
    df_polars_arrow.write_ipc("data_output_polars.arrow")
    arrow_write_time = time.time() - write_start_time
    logging.info(f"Polars Arrow write time: {arrow_write_time:.4f} seconds")

    # DuckDB CSV
    logging.info("Benchmarking DuckDB:")
    conn = duckdb.connect(":memory:")
    start_time = time.time()
    result = conn.execute(f"SELECT * FROM read_csv_auto('{csv_file}')")
    csv_time = time.time() - start_time
    logging.info(f"DuckDB CSV read time: {csv_time:.4f} seconds")

    write_start_time = time.time()
    conn.execute(
        f"COPY (SELECT * FROM read_csv_auto('{csv_file}')) TO 'data_output_duckdb.csv' (FORMAT CSV)"
    )
    csv_write_time = time.time() - write_start_time
    logging.info(f"DuckDB CSV write time: {csv_write_time:.4f} seconds")

    # DuckDB Parquet
    start_time = time.time()
    result = conn.execute(f"SELECT * FROM parquet_scan('{parquet_file}')")
    parquet_time = time.time() - start_time
    logging.info(f"DuckDB Parquet read time: {parquet_time:.4f} seconds")

    write_start_time = time.time()
    conn.execute(
        f"COPY (SELECT * FROM parquet_scan('{parquet_file}')) TO 'data_output_duckdb.parquet' (FORMAT PARQUET)"
    )
    parquet_write_time = time.time() - write_start_time
    logging.info(f"DuckDB Parquet write time: {parquet_write_time:.4f} seconds")

    # DuckDB Arrow
    start_time = time.time()
    arrow_table = pa.ipc.open_file(arrow_file).read_all()
    conn.register("arrow_table", arrow_table)  # Register the Arrow table
    result = conn.execute("SELECT * FROM arrow_table").fetch_arrow_table()
    arrow_time = time.time() - start_time
    logging.info(f"DuckDB Arrow read time: {arrow_time:.4f} seconds")

    write_start_time = time.time()
    # Write the result to an Arrow IPC file
    with pa.OSFile("data_output_duckdb.arrow", "wb") as sink:
        with pa.ipc.new_file(sink, result.schema) as writer:
            writer.write(result)

    arrow_write_time = time.time() - write_start_time
    logging.info(f"DuckDB Arrow write time: {arrow_write_time:.4f} seconds")

    # Pandas CSV
    logging.info("Benchmarking Pandas:")
    start_time = time.time()
    df_pandas_csv = pd.read_csv(csv_file)
    csv_time = time.time() - start_time
    logging.info(f"Pandas CSV read time: {csv_time:.4f} seconds")

    write_start_time = time.time()
    df_pandas_csv.to_csv("data_output_pandas.csv", index=False)
    csv_write_time = time.time() - write_start_time
    logging.info(f"Pandas CSV write time: {csv_write_time:.4f} seconds")

    # Pandas Parquet
    start_time = time.time()
    df_pandas_parquet = pd.read_parquet(parquet_file)
    parquet_time = time.time() - start_time
    logging.info(f"Pandas Parquet read time: {parquet_time:.4f} seconds")

    write_start_time = time.time()
    df_pandas_parquet.to_parquet("data_output_pandas.parquet", index=False)
    parquet_write_time = time.time() - write_start_time
    logging.info(f"Pandas Parquet write time: {parquet_write_time:.4f} seconds")

    # Pandas Arrow
    start_time = time.time()
    df_pandas_arrow = pd.read_feather(arrow_file)
    arrow_time = time.time() - start_time
    logging.info(f"Pandas Arrow read time: {arrow_time:.4f} seconds")

    write_start_time = time.time()
    df_pandas_arrow.to_feather("data_output_pandas.arrow")
    arrow_write_time = time.time() - write_start_time
    logging.info(f"Pandas Arrow write time: {arrow_write_time:.4f} seconds")


if __name__ == "__main__":
    benchmark_file_processing()

import logging
import time
import pandas as pd
import polars as pl
import duckdb
import pyarrow as pa


class DataBenchmark:
    def __init__(self, csv_file, parquet_file, arrow_file):
        self.csv_file = csv_file
        self.parquet_file = parquet_file
        self.arrow_file = arrow_file
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )

    def benchmark_operation(self, operation, *args, **kwargs):
        start_time = time.time()
        result = operation(*args, **kwargs)
        elapsed_time = time.time() - start_time
        return result, elapsed_time

    def log_benchmark(self, library, format, operation, elapsed_time):
        logging.info(f"{library} {format} {operation} time: {elapsed_time:.4f} seconds")

    def benchmark_polars(self):
        logging.info("Benchmarking Polars:")

        # CSV
        df, read_time = self.benchmark_operation(pl.read_csv, self.csv_file)
        self.log_benchmark("Polars", "CSV", "read", read_time)
        _, write_time = self.benchmark_operation(df.write_csv, "data_output_polars.csv")
        self.log_benchmark("Polars", "CSV", "write", write_time)

        # Parquet
        df, read_time = self.benchmark_operation(pl.read_parquet, self.parquet_file)
        self.log_benchmark("Polars", "Parquet", "read", read_time)
        _, write_time = self.benchmark_operation(
            df.write_parquet, "data_output_polars.parquet"
        )
        self.log_benchmark("Polars", "Parquet", "write", write_time)

        # Arrow
        df, read_time = self.benchmark_operation(pl.read_ipc, self.arrow_file)
        self.log_benchmark("Polars", "Arrow", "read", read_time)
        _, write_time = self.benchmark_operation(
            df.write_ipc, "data_output_polars.arrow"
        )
        self.log_benchmark("Polars", "Arrow", "write", write_time)

    def benchmark_duckdb(self):
        logging.info("Benchmarking DuckDB:")
        conn = duckdb.connect(":memory:")

        # CSV
        _, read_time = self.benchmark_operation(
            conn.execute, f"SELECT * FROM read_csv_auto('{self.csv_file}')"
        )
        self.log_benchmark("DuckDB", "CSV", "read", read_time)
        _, write_time = self.benchmark_operation(
            conn.execute,
            f"COPY (SELECT * FROM read_csv_auto('{self.csv_file}')) TO 'data_output_duckdb.csv' (FORMAT CSV)",
        )
        self.log_benchmark("DuckDB", "CSV", "write", write_time)

        # Parquet
        _, read_time = self.benchmark_operation(
            conn.execute, f"SELECT * FROM parquet_scan('{self.parquet_file}')"
        )
        self.log_benchmark("DuckDB", "Parquet", "read", read_time)
        _, write_time = self.benchmark_operation(
            conn.execute,
            f"COPY (SELECT * FROM parquet_scan('{self.parquet_file}')) TO 'data_output_duckdb.parquet' (FORMAT PARQUET)",
        )
        self.log_benchmark("DuckDB", "Parquet", "write", write_time)

        # Arrow
        def read_arrow():
            arrow_table = pa.ipc.open_file(self.arrow_file).read_all()
            conn.register("arrow_table", arrow_table)
            return conn.execute("SELECT * FROM arrow_table").fetch_arrow_table()

        result, read_time = self.benchmark_operation(read_arrow)
        self.log_benchmark("DuckDB", "Arrow", "read", read_time)

        def write_arrow(result):
            with pa.OSFile("data_output_duckdb.arrow", "wb") as sink:
                with pa.ipc.new_file(sink, result.schema) as writer:
                    writer.write(result)

        _, write_time = self.benchmark_operation(write_arrow, result)
        self.log_benchmark("DuckDB", "Arrow", "write", write_time)

    def benchmark_pandas(self):
        logging.info("Benchmarking Pandas:")

        # CSV
        df, read_time = self.benchmark_operation(pd.read_csv, self.csv_file)
        self.log_benchmark("Pandas", "CSV", "read", read_time)
        _, write_time = self.benchmark_operation(
            df.to_csv, "data_output_pandas.csv", index=False
        )
        self.log_benchmark("Pandas", "CSV", "write", write_time)

        # Parquet
        df, read_time = self.benchmark_operation(pd.read_parquet, self.parquet_file)
        self.log_benchmark("Pandas", "Parquet", "read", read_time)
        _, write_time = self.benchmark_operation(
            df.to_parquet, "data_output_pandas.parquet", index=False
        )
        self.log_benchmark("Pandas", "Parquet", "write", write_time)

        # Arrow
        df, read_time = self.benchmark_operation(pd.read_feather, self.arrow_file)
        self.log_benchmark("Pandas", "Arrow", "read", read_time)
        _, write_time = self.benchmark_operation(
            df.to_feather, "data_output_pandas.arrow"
        )
        self.log_benchmark("Pandas", "Arrow", "write", write_time)

    def run_benchmarks(self):
        self.benchmark_polars()
        self.benchmark_duckdb()
        self.benchmark_pandas()

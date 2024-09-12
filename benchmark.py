from methods import DataBenchmark

if __name__ == "__main__":
    csv_file = "data.csv"
    parquet_file = "data.parquet"
    arrow_file = "data.arrow"

    benchmark = DataBenchmark(csv_file, parquet_file, arrow_file)
    benchmark.run_benchmarks()

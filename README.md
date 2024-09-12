# 🔎 File Processing Benchmark

This project provides a comprehensive benchmark for comparing the performance of different file formats (CSV, Parquet, and Arrow) and data processing libraries (Polars, DuckDB, and Pandas) in Python.

## 📝 Introduction

The project consists of three main Python scripts:

- `generate_data.py`: Generates fake transaction data.
- `export_data.py`: Exports the generated data to CSV, Parquet, and Arrow formats.
- `methods.py`: Contains the methods used to benchmark the file processing.
- `benchmark.py`: Performs the benchmark tests on the exported files.

## 👨🏻‍💻 Prerequisites

- Docker Desktop

## 🔨 Usage

1. Build the Docker image:

    ```sh
    docker build -t file-reading-benchmark .
    ```

2. You can customize the number of records generated by setting the `NUM_RECORDS` environment variable:

    ```sh
    docker run -e NUM_RECORDS=500000 -it file-reading-benchmark
    ```

This example will generate and benchmark 500.000 records instead of the default 10.000. Be patient if going above 100.000 rows since it will take time to generate the files.

This command will:
1. Generate a fake transaction dataset with foreign key relationships to user/product datasets.
2. Export the data to CSV, Parquet, and Arrow formats.
3. Run the benchmark tests on each file format using Polars, DuckDB, and Pandas.

You can check the `.csv` files generated in the `output` folder.

### 😎 [Follow me on Linkedin](https://www.linkedin.com/in/alejandro-aboy/)
- Get tips, learnings and tricks for your Data career!

### 📩 [Subscribe to The Pipe & The Line](https://thepipeandtheline.substack.com/?utm_source=github&utm_medium=referral)
- Join the Substack newsletter to get similar content to this one and more to improve your Data career!

# csv-to-parquet-to-delta

Convert huge CSVs to Parquet parts to bypass Databricks upload limits and speed up ingestion.

Motivation
- Databricks workspace upload limit (~200MB) vs CSV sources >7GB.
- This repo contains only the code to split a large CSV into Parquet parts (no data committed).

Key points
- Streaming read with pyarrow (low memory).
- Split by target compressed size (~100 MB per file by default).
- Defaults: delimiter=';' and encoding='latin1', compression='snappy'.
- Output files named: parte_001.parquet, parte_002.parquet, …

Requirements
- Python 3.13.3
- pip install -r requirements.txt
- pandas
- pyarrow

How to run (local)
1) Edit the parameters at the top of the script:
 - csv_path: full path to your CSV
 - output_dir: folder for Parquet parts
 - max_file_size_mb: target size per file (compressed), e.g., 100
2) Execute:
 - python notebooks/01_csv_to_parquet.py

Outputs
- Files will be written to the folder defined in `output_dir`, e.g.:
- parte_001.parquet, parte_002.parquet, …

Notes
- No data is versioned in this repo (code only).
- The script estimates an in-memory buffer using a compression ratio of ~0.20; final sizes may vary slightly.

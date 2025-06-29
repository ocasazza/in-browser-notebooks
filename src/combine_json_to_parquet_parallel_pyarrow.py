#!/usr/bin/env python3

import os
import json
import sys
from multiprocessing import Pool, cpu_count
from functools import partial  # To pass fixed arguments to the worker function

import pyarrow as pa
import pyarrow.parquet as pq


# --- Worker function for a single JSON file ---
def process_single_json_file(file_path, root_dir):
    """
    Reads, parses, and formats data from a single JSON file.
    Includes metadata and the original unflattened JSON content.
    Returns a dictionary suitable for an Arrow Table row.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as infile:
            json_data = json.load(infile)

        # Create a wrapper dictionary including metadata and the actual data
        record_to_write = {
            "original_file_path": os.path.relpath(file_path, root_dir),
            "original_filename": os.path.basename(file_path),
            "data": json_data,  # The original, unflattened content
        }
        return record_to_write

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {file_path}: {e}", file=sys.stderr)
        return None  # Indicate failure for this file
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred with {file_path}: {e}", file=sys.stderr)
        return None


# --- Main parallelization function ---
def combine_json_to_parquet_parallel_pyarrow(
    root_dir, output_parquet_name="combined_data_pyarrow.parquet", num_processes=None
):
    """
    Recursively gathers all .json files in parallel, parses their content,
    and combines them into a single Parquet file using PyArrow directly.
    The original nested JSON structure is preserved within a a 'data' column (as a struct type).
    """
    all_json_file_paths = []
    print(f"Scanning directory: {root_dir} for JSON files...")

    # Validate root_dir
    if not os.path.isdir(root_dir):
        print(
            f"Error: Root directory '{root_dir}' not found or is not a directory.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Collect all JSON file paths (this part is sequential but fast)
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".json"):
                all_json_file_paths.append(os.path.join(dirpath, filename))

    if not all_json_file_paths:
        print(f"No JSON files found in '{root_dir}'.", file=sys.stderr)
        sys.exit(0)

    print(
        f"\nFound {len(all_json_file_paths)} JSON files. Starting parallel processing..."
    )

    # Determine number of processes
    if num_processes is None:
        num_processes = max(1, cpu_count() - 1)  # Use all but one core
    print(f"Using {num_processes} worker processes.")

    all_records = []
    # Use multiprocessing Pool to parallelize file processing
    func_with_root_dir = partial(process_single_json_file, root_dir=root_dir)

    try:
        with Pool(processes=num_processes) as pool:
            results = pool.map(func_with_root_dir, all_json_file_paths)

            # Filter out None results (from failed file processing)
            all_records = [record for record in results if record is not None]

    except Exception as e:
        print(f"Error during parallel processing: {e}", file=sys.stderr)
        sys.exit(1)

    if not all_records:
        print("No valid JSON documents were processed to combine.", file=sys.stderr)
        sys.exit(0)

    print(f"\nSuccessfully processed {len(all_records)} valid JSON documents.")

    # --- Key Change: Convert list of dictionaries to PyArrow Table ---
    try:
        # PyArrow's from_pylist can infer complex nested schemas from Python dicts/lists
        arrow_table = pa.Table.from_pylist(all_records)
    except pa.ArrowInvalid as e:
        print(
            f"Error converting Python list to Arrow Table. This often indicates inconsistent schemas or complex nested types that PyArrow can't infer easily. Error: {e}",
            file=sys.stderr,
        )
        # For more complex cases, you might need to define the PyArrow schema explicitly
        # and then use pa.array to create columns.
        sys.exit(1)
    except Exception as e:
        print(
            f"An unexpected error occurred during PyArrow Table creation: {e}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(
        f"Arrow Table created with {arrow_table.num_rows} rows and {arrow_table.num_columns} columns."
    )
    print("\nArrow Table Schema:")
    print(arrow_table.schema)
    # print("\nFirst few rows (limited for brevity):")
    # print(arrow_table.slice(length=5).to_pydict()) # Print first 5 rows as Python dicts

    # Save the Arrow Table to a Parquet file
    try:
        # 'compression' can be 'snappy', 'gzip', 'brotli', 'lz4', 'zstd'
        pq.write_table(arrow_table, output_parquet_name, compression="snappy")
        print(f"\nSuccessfully created Parquet file: {output_parquet_name}")
    except Exception as e:
        print(
            f"Error writing Parquet file '{output_parquet_name}': {e}", file=sys.stderr
        )
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print(
            "Usage: python combine_json_to_parquet_parallel_pyarrow.py <root_directory> [output_parquet_name]",
            file=sys.stderr,
        )
        sys.exit(1)

    root_directory = sys.argv[1]
    output_parquet_file = (
        sys.argv[2] if len(sys.argv) == 3 else "combined_data_pyarrow.parquet"
    )

    # Ensure pyarrow is installed
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print(
            "Error: 'pyarrow' library not found. Please install it: pip install pyarrow",
            file=sys.stderr,
        )
        sys.exit(1)

    combine_json_to_parquet_parallel_pyarrow(root_directory, output_parquet_file)

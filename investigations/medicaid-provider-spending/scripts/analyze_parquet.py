#!/usr/bin/env python3
"""
Analyze large parquet dataset using Polars with memory-efficient streaming.
"""

import polars as pl
from pathlib import Path

def analyze_parquet(file_path: str):
    """
    Analyze parquet file using lazy evaluation and streaming to minimize memory usage.

    Args:
        file_path: Path to the parquet file
    """
    print(f"Analyzing: {file_path}")
    print("=" * 80)

    # Use scan_parquet for lazy evaluation (doesn't load data into memory)
    lazy_df = pl.scan_parquet(file_path)

    # Get schema information (this is fast, doesn't read data)
    print("\n📋 SCHEMA INFORMATION")
    print("-" * 80)
    schema = lazy_df.collect_schema()
    print(f"\nTotal columns: {len(schema)}")
    print("\nColumn names and types:")
    for col_name, col_type in schema.items():
        print(f"  • {col_name:<40} {col_type}")

    # Get row count efficiently
    print("\n📊 DATASET SIZE")
    print("-" * 80)
    row_count = lazy_df.select(pl.len()).collect().item()
    print(f"Total rows: {row_count:,}")

    # Get basic statistics using streaming (processes in batches)
    print("\n📈 DESCRIPTIVE STATISTICS")
    print("-" * 80)
    print("\nComputing statistics (streaming mode - memory efficient)...\n")

    # Compute statistics for numeric columns only (more efficient)
    numeric_cols = [col for col, dtype in schema.items()
                    if dtype in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]]

    if numeric_cols:
        # Compute key statistics using streaming
        stats_exprs = []
        for col in numeric_cols:
            stats_exprs.extend([
                pl.col(col).min().alias(f"{col}_min"),
                pl.col(col).max().alias(f"{col}_max"),
                pl.col(col).mean().alias(f"{col}_mean"),
                pl.col(col).median().alias(f"{col}_median"),
                pl.col(col).std().alias(f"{col}_std"),
            ])

        stats_result = lazy_df.select(stats_exprs).collect(engine="streaming")

        # Format and display statistics
        for col in numeric_cols:
            print(f"\n{col}:")
            print(f"  Min:    {stats_result[f'{col}_min'][0]:,.2f}" if stats_result[f'{col}_min'][0] is not None else "  Min:    None")
            print(f"  Max:    {stats_result[f'{col}_max'][0]:,.2f}" if stats_result[f'{col}_max'][0] is not None else "  Max:    None")
            print(f"  Mean:   {stats_result[f'{col}_mean'][0]:,.2f}" if stats_result[f'{col}_mean'][0] is not None else "  Mean:   None")
            print(f"  Median: {stats_result[f'{col}_median'][0]:,.2f}" if stats_result[f'{col}_median'][0] is not None else "  Median: None")
            print(f"  StdDev: {stats_result[f'{col}_std'][0]:,.2f}" if stats_result[f'{col}_std'][0] is not None else "  StdDev: None")
    else:
        print("No numeric columns found.")

    # For string columns, show unique value counts
    string_cols = [col for col, dtype in schema.items() if dtype == pl.String]
    if string_cols:
        print("\n\n📝 STRING COLUMN STATISTICS")
        print("-" * 80)
        unique_exprs = [pl.col(col).n_unique().alias(col) for col in string_cols]
        unique_counts = lazy_df.select(unique_exprs).collect(engine="streaming")

        for col in string_cols:
            unique_count = unique_counts[col][0]
            print(f"\n{col}:")
            print(f"  Unique values: {unique_count:,}")

    # Get null counts for each column
    print("\n🔍 NULL VALUE ANALYSIS")
    print("-" * 80)
    null_counts = lazy_df.select([
        pl.col(col).null_count().alias(col) for col in schema.keys()
    ]).collect(engine="streaming")

    # Transpose for better readability
    null_data = []
    for col in schema.keys():
        null_count = null_counts[col][0]
        null_pct = (null_count / row_count * 100) if row_count > 0 else 0
        null_data.append({
            "Column": col,
            "Null Count": null_count,
            "Null %": f"{null_pct:.2f}%"
        })

    null_df = pl.DataFrame(null_data)
    print(null_df)

    # Memory usage estimate
    print("\n💾 MEMORY INFORMATION")
    print("-" * 80)
    print(f"File size on disk: {Path(file_path).stat().st_size / (1024**3):.2f} GB")
    print("Note: Using lazy evaluation and streaming - minimal memory usage!")

    print("\n✅ Analysis complete!")


if __name__ == "__main__":
    # Path to the parquet file
    parquet_path = str(Path(__file__).resolve().parent.parent / "data" / "medicaid-provider-spending.parquet")

    # Run analysis
    analyze_parquet(parquet_path)

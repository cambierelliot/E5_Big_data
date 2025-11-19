# Data

by BRAUX Owen and CAMBIER Elliot

This directory contains all the raw, processed, and intermediate data used in the BDA Bitcoin Price Predictor project.

## Directory Structure

data/
├── blocks/
│ └── blocks/
│ ├── blk00013.dat
│ └── ... (raw Bitcoin block files)
├── prices/
│ ├── btcusd_1-min_data.csv
│ └── ... (historical BTC price data)
├── processed/
│ ├── transactions.parquet/
│ └── features.parquet/
└── btc_blocks_pruned_1GiB.tar.gz

## Description of Contents

### `/blocks`
- **Source:** Generated from a pruned Bitcoin Core node.
- **Content:** Contains the raw binary block files (`blk*.dat`). These files are the primary input for the ETL stage of the pipeline (`02-ETL-Bitcoin-Blocks.ipynb` / `run_etl` function).
- **Archive:** The original archive `btc_blocks_pruned_1GiB.tar.gz` is also included at the root of the `data/` directory.

### `/prices`
- **Source:** [Bitcoin Historical Data on Kaggle](https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data)
- **Content:** Contains historical Bitcoin price data in CSV format. The primary file used is `btcusd_1-min_data.csv`, which provides minute-by-minute candle data.

### `/processed`
This directory holds the output of the data processing stages, saved in the optimized Parquet format for efficient reuse.
- **`transactions.parquet/`**: The output of the ETL stage. This is a clean table of all transactions extracted from the raw block files.
- **`features.parquet/`**: The output of the feature engineering stage. This is the final, model-ready table containing both market prices and aggregated on-chain features, joined by timestamp.
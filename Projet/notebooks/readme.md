# Project Notebooks

by BRAUX Owen and CAMBIER Elliot

This directory contains the Jupyter notebooks used for the development, exploration, and step-by-step implementation of the project pipeline.

The notebooks are numbered to reflect the logical flow of the project, from initial setup to final modeling. They serve as a detailed log of the development process.

## Notebooks

-   **`00-Environment-Check.ipynb`**: Captures the execution environment (OS, Python, Java, Spark versions) and generates the `ENV.md` file required for reproducibility.
-   **`01-Data-Ingestion.ipynb`**: Performs the initial loading and validation of the market price data (CSV files) to ensure the Spark environment is correctly set up.
-   **`02-ETL-Bitcoin-Blocks.ipynb`**: Contains the core ETL logic for parsing the raw binary Bitcoin block files (`blk*.dat`) into a structured transactions table. The final output is saved to `transactions.parquet`.
-   **`03-Feature-Engineering.ipynb`**: Loads the transaction and market price data, aggregates on-chain metrics into 1-minute windows, and joins them to create the primary feature set (`features.parquet`).
-   **`04-Modeling-and-Evaluation.ipynb`**: Implements the final stage of the project. It defines the prediction label, engineers advanced time-series features (e.g., rolling averages), trains a `GBTClassifier` model, and evaluates its performance (AUC).

**Note:** While these notebooks show the development process, the final, automated, end-to-end pipeline should be executed via the script in the `scripts/` directory.

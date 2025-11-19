# Automation Scripts

by BRAUX Owen and CAMBIER Elliot

This directory contains the final, production-ready scripts for executing the entire project pipeline in an automated, end-to-end fashion.

## Files

-   **`main.py`**: The master Python script that orchestrates the full pipeline. It is structured into sequential functions for each major stage: ETL, feature engineering, and modeling. It reads its configuration from the `bda_project_config.yml` file located in the project root.
-   **`run_all.sh`**: (Located in the project root) The main entry point for the project. This shell script is the "one-shot runner" required by the project brief. It handles activating the correct Conda environment and then executes `main.py` to run the entire pipeline.

## How to Run the Pipeline

To execute the full pipeline from start to finish, run the `run_all.sh` script from the **root directory** of the project:

```bash
# Make sure the script is executable (only needs to be done once)
chmod +x run_all.sh

# Run the entire pipeline
./run_all.sh

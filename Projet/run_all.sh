#!/bin/bash

# =============================================================
# One-shot runner for the BDA Bitcoin Price Predictor Project
# =============================================================

# Exit immediately if a command exits with a non-zero status.
set -e

# configurations
CONDA_ENV_NAME="bda-env"
MAIN_SCRIPT_PATH="scripts/main.py"

# --- Main Execution ---
echo "--- Starting BDA Project Pipeline ---"

# Activate conda environment
echo "Activating conda environment: $CONDA_ENV_NAME"
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV_NAME"

# 3Check if PySpark is available in the environment
if ! python -c "import pyspark" >/dev/null 2>&1; then
    echo "Error: PySpark not found in the '$CONDA_ENV_NAME' environment." >&2
    echo "Please ensure you have run 'pip install pyspark'." >&2
    exit 1
fi

# Run the main Spark application
echo "Executing the main PySpark script: $MAIN_SCRIPT_PATH"
python "$MAIN_SCRIPT_PATH"

# Deactivate environment
conda deactivate
echo "--- Pipeline Finished Successfully ---"
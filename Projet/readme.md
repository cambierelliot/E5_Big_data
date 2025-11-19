# Big Data

by BRAUX Owen and CAMBIER Elliot

conda create -n bda-env python=3.10 -y
conda activate bda-env
conda install -c conda-forge openjdk=21 maven -y
pip install --upgrade pip jupyterlab kaggle pyspark

Pour Kaggle : 
Connectez-vous à votre compte sur kaggle.com.
Allez dans votre profil, puis dans la section "Account".
Cliquez sur "Create New Token". Un fichier kaggle.json sera téléchargé.
Déplacez ce fichier et sécurisez-le avec les commandes suivantes dans votre terminal :

```
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```

chmod +x run_all.sh
./run_all.sh


## Data Acquisition Guide

This project requires two main datasets: Bitcoin blockchain blocks and historical market prices.

### Option 1: Using the Provided Dataset (Recommended)

The easiest way to get started is to use the pre-packaged data available on the course Google Drive.

1.  **Download the archive:** `BDA-project-final-data.zip` from the provided Gdrive link.
2.  **Unzip the file** into the `data/` directory of this project.

This will provide the necessary `blocks/` and `prices/` subdirectories.

### Option 2: Fetching from Source

#### A. Market Prices (Kaggle)

1.  Ensure the Kaggle API is configured (`~/.kaggle/kaggle.json`).
2.  Run the following command to download the price data:
    ```bash
    conda activate bda-env
    kaggle datasets download -d mczielinski/bitcoin-historical-data -p data/prices --unzip --force
    ```

#### B. Raw Bitcoin Blocks (Bitcoin Core)

The raw `blk*.dat` files were generated using a pruned Bitcoin Core node as described in the project support documents. Refer to `BDA_final_project_support_dataset.md` for the full procedure.
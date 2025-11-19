import os
from pyspark.sql import SparkSession
import sys
import platform
import subprocess
from pathlib import Path
import pyspark
import binascii
import struct
from datetime import datetime
from pyspark.sql.functions import explode, col, from_unixtime, window, sum, count, avg, lead, when, max, stddev
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, ArrayType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, GBTClassifier 
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import csv
import yaml 
from pathlib import Path
from io import StringIO


class BlockchainParser:
    def __init__(self, raw_data):
        self.data = raw_data
        self.offset = 0

    def read_bytes(self, n):
        out = self.data[self.offset : self.offset + n]
        self.offset += n
        return out

    def read_uint32(self):
        return struct.unpack('<I', self.read_bytes(4))[0]

    def read_int32(self):
        return struct.unpack('<i', self.read_bytes(4))[0]

    def read_uint64(self):
        return struct.unpack('<Q', self.read_bytes(8))[0]

    def read_varint(self):
        i = self.read_bytes(1)[0]
        if i < 0xfd:
            return i
        elif i == 0xfd:
            return struct.unpack('<H', self.read_bytes(2))[0]
        elif i == 0xfe:
            return struct.unpack('<I', self.read_bytes(4))[0]
        else:
            return struct.unpack('<Q', self.read_bytes(8))[0]

    def parse_block(self):
        # Verify Magic Bytes
        magic = self.read_bytes(4)
        if magic != b'\xf9\xbe\xb4\xd9': # Mainnet magic bytes
            return None 
        
        size = self.read_uint32()

        version = self.read_int32()
        prev_block = self.read_bytes(32)[::-1].hex()
        merkle_root = self.read_bytes(32)[::-1].hex()
        timestamp = self.read_uint32()
        bits = self.read_uint32()
        nonce = self.read_uint32()

        tx_count = self.read_varint()
        
        transactions = []
        for _ in range(tx_count):
            transactions.append(self.parse_transaction(timestamp))
            
        return {
            "prev_block_hash": prev_block,
            "timestamp": timestamp,
            "nonce": nonce,
            "n_transactions": tx_count,
            "transactions": transactions
        }

    def parse_transaction(self, block_ts):

        start_offset = self.offset
        
        version = self.read_int32()
        
        # Inputs
        n_inputs = self.read_varint()
        inputs = []
        for _ in range(n_inputs):
            tx_hash = self.read_bytes(32)[::-1].hex()
            vout_idx = self.read_uint32()
            script_len = self.read_varint()
            script_sig = self.read_bytes(script_len)
            sequence = self.read_uint32()
            inputs.append({"prev_tx_hash": tx_hash, "prev_out_idx": vout_idx})
            
        # Outputs
        n_outputs = self.read_varint()
        outputs = []
        total_amount = 0
        for _ in range(n_outputs):
            amount = self.read_uint64() # Satoshi
            pk_script_len = self.read_varint()
            pk_script = self.read_bytes(pk_script_len)
            outputs.append({"amount": amount})
            total_amount += amount
            
        lock_time = self.read_uint32()
        
        # Calculate simplified TxID because it's too long otherwise
        return {
            "block_timestamp": block_ts,
            "n_inputs": n_inputs,
            "n_outputs": n_outputs,
            "total_amount_satoshi": total_amount,
            "total_amount_btc": total_amount / 100000000.0
        }

def parse_raw_block_file(file_data):
    filename, content = file_data
    parser = BlockchainParser(content)
    blocks = []

    while parser.offset < len(content):
        try:
            block = parser.parse_block()
            if block:
                blocks.append(block)
            else:
                break # Stop if magic bytes don't match 
        except Exception:
            break
            
    return blocks


def create_spark_session():
    """Creates and returns a Spark Session."""
    spark = SparkSession.builder \
        .appName("BDA Final Project - Bitcoin Predictor") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    return spark

def load_config():
    """
    Loads the YAML configuration file by building a reliable path
    relative to the script's location.
    """
    script_dir = Path(__file__).parent.resolve()
    project_root = script_dir.parent
    config_path = project_root / "bda_project_config.yml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def generate_environment_file(spark, output_path):
    """Generates the ENV.md file documenting the execution environment."""
    print("--- Generating Environment File ---")
    
    def get_java_version():
        try:
            output = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT)
            return output.decode("utf-8").strip().splitlines()[0]
        except Exception as e:
            return f"Could not determine Java version: {e}"

    java_version_str = get_java_version()
    spark_conf_items = sorted(spark.sparkContext.getConf().getAll())

    env_lines = [
        "# BDA Final Project - Environment Summary", "",
        "This file documents the environment used to run the project pipeline.", "",
        "## Key Components",
        f"- **Operating System:** `{platform.platform()}`",
        f"- **Python Version:** `{sys.version.split()[0]}`",
        f"- **PySpark Version:** `{pyspark.__version__}`",
        f"- **Apache Spark Version:** `{spark.version}`",
        f"- **Java Version:** `{java_version_str}`", "",
        "## Spark Configuration", ""
    ]
    for key, value in spark_conf_items:
        env_lines.append(f"- `{key}`: `{value}`")

    env_file_path = Path(output_path)
    env_file_path.write_text("\n".join(env_lines) + "\n", encoding='utf-8')
    print(f"Environment details saved to {env_file_path.resolve()}")


def run_etl(spark, input_path, output_path):
    """
    Runs the ETL process from raw blocks to a transactions table.
    (Content from notebook 02-ETL-Bitcoin-Blocks.ipynb)
    """
    print("--- Starting ETL Stage ---")
    # needed : RDD operations / binaryFiles
    sc = spark.sparkContext

    # path
    blocks_path = input_path

    print(f"Targeting block files at: {blocks_path}")

    # Load the binary files into an RDD
    raw_blocks_rdd = sc.binaryFiles(blocks_path)

    # Validate the load
    file_count = raw_blocks_rdd.count()
    print(f"Number of 'blk' files loaded: {file_count}")

    # Show the file names to verify we grabbed the right ones
    file_names = raw_blocks_rdd.keys().take(5)
    print("Sample file names:")
    for name in file_names:
        print(name)

    parsed_blocks_rdd = raw_blocks_rdd.flatMap(parse_raw_block_file)

    # Cache the result. Learned this during my internship at Lite to save ressources
    parsed_blocks_rdd.cache()

    # Since this must match the dictionary structure from our parser, we create the schemas
    transaction_schema = StructType([
        StructField("block_timestamp", LongType(), True),
        StructField("n_inputs", IntegerType(), True),
        StructField("n_outputs", IntegerType(), True),
        StructField("total_amount_satoshi", LongType(), True),
        StructField("total_amount_btc", DoubleType(), True)
    ])
    block_schema = StructType([
        StructField("prev_block_hash", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("nonce", LongType(), True),
        StructField("n_transactions", IntegerType(), True),
        StructField("transactions", ArrayType(transaction_schema), True)
    ])

    blocks_df = spark.createDataFrame(parsed_blocks_rdd, schema=block_schema)
    blocks_df.printSchema()

    # Use explode to create a new row for each element for transactions : we flatten it
    transactions_df = blocks_df.select(explode("transactions").alias("tx"))

    # flattens to get out final columns :
    final_tx_df = transactions_df.select(
        col("tx.block_timestamp"),
        col("tx.n_inputs"),
        col("tx.n_outputs"),
        col("tx.total_amount_satoshi"),
        col("tx.total_amount_btc")
    )

    final_tx_df = final_tx_df.withColumn(
    "timestamp_utc",
    from_unixtime(col("block_timestamp")).cast("timestamp")
    )
    final_tx_df.coalesce(1).write \
    .mode("overwrite") \
    .parquet(output_path)

    print("--- ETL Stage Complete ---")
    return final_tx_df 

def run_feature_engineering(spark, transactions_path, prices_path, output_path):
    """
    Runs the feature engineering process.
    (Content from notebook 03-Feature-Engineering.ipynb)
    """
    print("--- Starting Feature Engineering Stage ---")
    # Load the market price data of notebook 1
    price_data_path = prices_path
    df_prices_raw = spark.read.csv(price_data_path, header=True, inferSchema=True)
    df_prices_raw.printSchema()

    # Clean up the price data columns
    df_prices = df_prices_raw.withColumnRenamed("Timestamp", "unix_timestamp") \
                            .withColumnRenamed("Open", "price_open") \
                            .withColumnRenamed("High", "price_high") \
                            .withColumnRenamed("Low", "price_low") \
                            .withColumnRenamed("Close", "price_close") \
                            .withColumnRenamed("Volume", "volume_btc")

    if "Volume_(Currency)" in df_prices.columns:
        df_prices = df_prices.withColumnRenamed("Volume_(Currency)", "volume_currency")

    if "Weighted_Price" in df_prices.columns:
        df_prices = df_prices.withColumnRenamed("Weighted_Price", "weighted_price")


    # Convert Unix timestamp to a proper timestamp type
    df_prices = df_prices.withColumn("timestamp_utc", from_unixtime(col("unix_timestamp")).cast("timestamp"))

    print("\n Cleaned Price Data Schema and Sample :")
    df_prices.printSchema()
    df_prices.select("timestamp_utc", "price_open", "price_close", "volume_btc").show(5)

    # load the parquet file
    df_transactions = spark.read.parquet(transactions_path)

    print("\n data schema :")
    df_transactions.printSchema()
    df_transactions.show(5)

    # Aggregate transaction data into 1-minute windows
    # (We group transactions by time windows to match the granularity of our price data)
    onchain_features_df = df_transactions.groupBy(
        # 'window' creates tumbling (non-overlapping) windows of a specified duration.
        window(col("timestamp_utc"), "1 minute")
    ).agg(
        count("*").alias("tx_count"),
        sum("total_amount_btc").alias("tx_volume_btc"),
        avg("n_inputs").alias("avg_inputs"),
        avg("n_outputs").alias("avg_outputs")
    )

    print("\n--- On-Chain Features Schema and Sample ---")
    onchain_features_df.printSchema()

    # Show the results, sorting by the window time
    onchain_features_df.sort("window").show(10, truncate=False)

    # Prepare the data for the join.
    #  renamed to 'timestamp_utc' to match the price df column
    onchain_features_to_join = onchain_features_df.withColumn("timestamp_utc", col("window.start")) \
                                                .drop("window")

    # join both df
    print("Joining price data with on-chain features...")
    df_combined = df_prices.join(
        onchain_features_to_join,
        on="timestamp_utc",  # The common column to join on
        how="left"
    )

    # cleaning we replace nulls with 0
    feature_columns = ["tx_count", "tx_volume_btc", "avg_inputs", "avg_outputs"]
    df_combined = df_combined.fillna(0, subset=feature_columns)

    df_combined.printSchema() # Quick check

    print("\n Sample of the Final Combined DataFrame : ")
    df_combined.select(
        "timestamp_utc",
        "price_close",
        "tx_count",
        "tx_volume_btc"
    ).sort("timestamp_utc", ascending=False).show(15)

    save_explain_plan(df_combined, "explain_plan_features.txt")
    df_combined.write.mode("overwrite").parquet(output_path)
    print("Saving in parquet format Done !")
    print("--- Feature Engineering Stage Complete ---")

def run_modeling(spark, features_path, metrics_path, params):
    """
    Runs the model training and evaluation.
    (Content from notebook 04-Modeling-and-Evaluation.ipynb)
    """
    print("--- Starting Modeling Stage ---")
    # Load the data to not recalculate the join
    df_ml = spark.read.parquet(features_path)

    print("Successfully loaded feature data.")
    df_ml.printSchema()
    df_ml.show(5)

    # Define the prediction horizon and threshold
    prediction_horizon = params['prediction_horizon_minutes']
    price_increase_threshold = params['price_increase_threshold']

    # Use a Window function to get the future price
    window_spec = Window.orderBy("timestamp_utc")
    df_with_future_price = df_ml.withColumn( # <-- Using df_ml now
        "future_price",
        lead(col("price_close"), prediction_horizon).over(window_spec)
    )

    # label
    df_labeled = df_with_future_price.withColumn(
        "label",
        when(col("future_price") > col("price_close") * (1 + price_increase_threshold), 1)
        .otherwise(0)
    )

    # Removing rows where we couldn't calculate the label
    df_final_ml = df_labeled.na.drop(subset=["future_price"])

    print(f" Final ML-ready data with label :")
    df_final_ml.select("timestamp_utc", "price_close", "future_price", "label", "tx_count").sort("timestamp_utc", ascending=False).show(15)
    print("\n Label Distribution :")
    df_final_ml.groupBy("label").count().show()

    # Additional feature engineering: rolling averages and momentum indicators
    rolling_window_rows = params['rolling_window_hours'] * 60
    rolling_window_1h = Window.orderBy("timestamp_utc").rowsBetween(-rolling_window_rows, 0)

    df_with_features = df_final_ml.withColumn(
        # Price momentum 
        "price_1h_avg", avg("price_close").over(rolling_window_1h)
    ).withColumn(
        "price_momentum", col("price_close") / col("price_1h_avg") 
    ).withColumn(
        # On-chain activity momentum
        "tx_volume_btc_1h_avg", avg("tx_volume_btc").over(rolling_window_1h)
    ).withColumn(
        "tx_count_1h_max", max("tx_count").over(rolling_window_1h) # Spike detection
    )

    df_with_features = df_with_features.na.drop(subset=[
        "price_momentum",
        "tx_volume_btc_1h_avg",
        "tx_count_1h_max"
    ])
    df_with_features.select(
        "timestamp_utc",
        "price_close",
        "price_momentum",
        "tx_volume_btc",
        "tx_volume_btc_1h_avg",
        "tx_count_1h_max"
    ).sort("timestamp_utc", ascending=False).show()

    # Select the features to be used by the model.
    feature_cols = [
        "price_close",
        "tx_count",
        "tx_volume_btc",
        "price_momentum",
        "tx_volume_btc_1h_avg",
        "tx_count_1h_max"          
    ]

    # 2. Split the data for training and test
    (training_data, test_data) = df_with_features.randomSplit(
        [1.0 - params['test_size'], params['test_size']], 
        seed=params['random_seed']
    )
    print("Data split into training and testing sets :")
    print(f" - Training set count: {training_data.count():,}")
    print(f" - Test set count: {test_data.count():,}")

    # we use VectorAssembler to combines our feature columns into a single vector column
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    # Models (need to change bcs rn it's kinda bad).
    gbt = GBTClassifier(
        featuresCol="features",
        labelCol="label"
    )

    pipeline = Pipeline(stages=[assembler, gbt])

    # training
    print("\nTraining the model...")
    model = pipeline.fit(training_data)
    print("Training complete.")

    # prediction
    print("\nMaking predictions on the test set...")
    predictions = model.transform(test_data)

    # Sample of prediction
    predictions.select("timestamp_utc", "price_close", "label", "prediction", "probability").show()
    save_explain_plan(predictions, "explain_plan_predictions.txt")

    # Evaluate the model suing ROC
    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    auc = evaluator.evaluate(predictions)
    print(f"\nModel Performance on Test Set :")
    print(f"Area Under ROC Curve (AUC) = {auc:.4f}")

    metrics_file = metrics_path
    run_id = params['run_id'] 
    timestamp = datetime.now().isoformat()

    metrics_to_log = [
        {"run_id": run_id, "stage": "evaluation", "metric": "auc", "value": auc, "timestamp": timestamp},
        {"run_id": run_id, "stage": "data_split", "metric": "training_set_size", "value": training_data.count(), "timestamp": timestamp},
        {"run_id": run_id, "stage": "data_split", "metric": "test_set_size", "value": test_data.count(), "timestamp": timestamp}
    ]

    file_exists = os.path.isfile(metrics_file)
    with open(metrics_file, 'a', newline='') as csvfile:
        fieldnames = ["run_id", "stage", "metric", "value", "timestamp"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()  
        for row in metrics_to_log:
            writer.writerow(row)

    print(f"\n Metrics successfully logged to {metrics_file}")

    print("--- Modeling Stage Complete ---")

def save_explain_plan(df, filename):
    """Saves the formatted explain plan of a DataFrame to a text file."""
    script_dir = Path(__file__).parent.resolve()
    project_root = script_dir.parent
    evidence_dir = project_root / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    output_path = evidence_dir / filename
    old_stdout = sys.stdout
    sys.stdout = explain_buffer = StringIO()
    df.explain("formatted")
    sys.stdout = old_stdout
    with open(output_path, 'w') as f:
        f.write(explain_buffer.getvalue())
    print(f"Saved explain plan to {output_path}")

if __name__ == "__main__":
    config = load_config()
    spark = create_spark_session()
    paths = config['paths']
    params = config['modeling']

    # Run the pipeline step-by-step
    generate_environment_file(spark, paths['env_file'])
    run_etl(spark, paths['raw_blocks'], paths['transactions_parquet'])
    run_feature_engineering(spark, paths['transactions_parquet'], paths['market_prices'], paths['features_parquet'])
    run_modeling(spark, paths['features_parquet'], paths['metrics_log'], params)

    spark.stop()
    print("--- Pipeline Finished ---")
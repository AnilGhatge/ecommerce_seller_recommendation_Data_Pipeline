import sys
import os
import yaml
import logging
import shutil
from datetime import datetime
from pyspark.sql import SparkSession

def load_config():
    config_idx = sys.argv.index("--config") + 1
    with open(sys.argv[config_idx], "r") as f:
        return yaml.safe_load(f)

def get_logger(name, log_file):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger

def get_spark_session(app_name: str) -> SparkSession:
    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark

def write_to_hudi(df, path, record_key, partition_path, precombine_field):
    table_name = path.rstrip('/').split('/')[-1]
    
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': record_key,
        'hoodie.datasource.write.partitionpath.field': partition_path,
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        
        # 1. INCREMENTAL UPSERTS: Enables stateful updates based on unique primary keys
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': precombine_field,
        'hoodie.index.type': 'SIMPLE', # Tracks existing keys across files efficiently
        
        # 2. SCHEMA EVOLUTION: Allows Hudi to safely evolve schemas automatically
        'hoodie.datasource.write.schema.allow.auto.evolution.column.drop': 'false',
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.datasource.hive_sync.enable': 'false'
    }
    
    # CRITICAL: Switched from "overwrite" to "append" to allow Hudi to 
    # upsert new data into existing files without wiping historical data.
    df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(path)

def route_to_quarantine(df, condition, failure_reason, dataset_name, quarantine_base_path):
    from pyspark.sql.functions import lit, to_json, struct
    bad_df = df.filter(~condition)
    if bad_df.count() > 0:
        quarantine_df = bad_df.select(
            lit(dataset_name).alias("dataset_name"),
            to_json(struct("*")).alias("original_record"),
            lit(failure_reason).alias("dq_failure_reason")
        )
        q_path = os.path.join(quarantine_base_path, dataset_name)
        quarantine_df.write.mode("append").json(q_path)
    return df.filter(condition)

def archive_and_rotate_file(src_path, archive_dir):
    if os.path.exists(src_path):
        os.makedirs(archive_dir, exist_ok=True)
        base_name = os.path.basename(src_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_name = f"{timestamp}_{base_name}"
        shutil.move(src_path, os.path.join(archive_dir, dest_name))
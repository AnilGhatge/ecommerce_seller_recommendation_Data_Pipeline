from pyspark.sql.functions import col, trim, initcap, when
from utils import get_spark_session, load_config, get_logger, write_to_hudi, route_to_quarantine, archive_and_rotate_file
from pyspark.sql.functions import current_timestamp

spark = get_spark_session("SellerCatalogBatchMedallion")
cfg = load_config()
p_cfg = cfg["seller_catalog"]
logger = get_logger("SellerCatalogPipeline", cfg["log_file"])

logger.info("Ingesting raw landing CSV to Bronze Layer Parquet format...")
raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(p_cfg["input_path"])
raw_df.write.mode("overwrite").parquet(p_cfg["bronze_path"])

# Read from Bronze to apply processing logic
df = spark.read.parquet(p_cfg["bronze_path"])

# Data Cleaning steps
for c in ["seller_id", "item_id", "item_name", "category"]:
    df = df.withColumn(c, trim(col(c)))

df = df.withColumn("item_name", initcap(col("item_name"))) \
       .withColumn("category", initcap(col("category"))) \
       .withColumn("marketplace_price", col("marketplace_price").cast("double")) \
       .withColumn("stock_qty", when(col("stock_qty").isNull(), 0).otherwise(col("stock_qty")).cast("int"))

df = df.dropDuplicates(["seller_id", "item_id"])

# DQ Rules Validation Checkpoints
q_base = cfg["quarantine_base"]
df = route_to_quarantine(df, col("seller_id").isNotNull(), "missing_seller_id", "seller_catalog", q_base)
df = route_to_quarantine(df, col("item_id").isNotNull(), "missing_item_id", "seller_catalog", q_base)
df = route_to_quarantine(df, col("marketplace_price") >= 0, "price_negative", "seller_catalog", q_base)
df = route_to_quarantine(df, col("stock_qty") >= 0, "stock_negative", "seller_catalog", q_base)
df = route_to_quarantine(df, col("item_name").isNotNull(), "missing_item_name", "seller_catalog", q_base)
df = route_to_quarantine(df, col("category").isNotNull(), "missing_category", "seller_catalog", q_base)

#logger.info(f"Writing clean records down to Silver Hudi Table: {p_cfg['silver_path']}")
#write_to_hudi(df, p_cfg["silver_path"], "seller_id,item_id", "category", "stock_qty")

# Add a processing clock stamp to handle duplicate arrivals
df = df.withColumn("ingestion_time", current_timestamp())

logger.info(f"Upserting records to Silver Hudi Table: {p_cfg['silver_path']}")
write_to_hudi(df, p_cfg["silver_path"], "seller_id,item_id", "category", "ingestion_time")

archive_and_rotate_file(p_cfg["input_path"], p_cfg["archive_path"])
logger.info("Seller Catalog processing completed.")
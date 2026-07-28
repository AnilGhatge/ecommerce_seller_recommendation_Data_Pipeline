from pyspark.sql.functions import col, trim, when, current_date, to_date
from utils import get_spark_session, load_config, get_logger, write_to_hudi, route_to_quarantine, archive_and_rotate_file

spark = get_spark_session("CompanySalesBatchMedallion")
cfg = load_config()
p_cfg = cfg["company_sales"]
logger = get_logger("CompanySalesPipeline", cfg["log_file"])

logger.info("Ingesting raw landing CSV to Bronze Layer Parquet format...")
raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(p_cfg["input_path"])
raw_df.write.mode("overwrite").parquet(p_cfg["bronze_path"])

df = spark.read.parquet(p_cfg["bronze_path"])

# Data Cleaning steps
df = df.withColumn("item_id", trim(col("item_id"))) \
       .withColumn("units_sold", when(col("units_sold").isNull(), 0).otherwise(col("units_sold")).cast("int")) \
       .withColumn("revenue", when(col("revenue").isNull(), 0.0).otherwise(col("revenue")).cast("double")) \
       .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))

df = df.dropDuplicates(["item_id"])

# DQ Rules Validation Checkpoints
q_base = cfg["quarantine_base"]
df = route_to_quarantine(df, col("item_id").isNotNull(), "missing_item_id", "company_sales", q_base)
df = route_to_quarantine(df, col("units_sold") >= 0, "units_negative", "company_sales", q_base)
df = route_to_quarantine(df, col("revenue") >= 0, "revenue_negative", "company_sales", q_base)
df = route_to_quarantine(df, col("sale_date").isNotNull() & (col("sale_date") <= current_date()), "invalid_sale_date", "company_sales", q_base)

#logger.info(f"Writing clean records down to Silver Hudi Table: {p_cfg['silver_path']}")
#write_to_hudi(df, p_cfg["silver_path"], "item_id", "sale_date", "units_sold")

logger.info(f"Upserting records to Silver Hudi Table: {p_cfg['silver_path']}")
write_to_hudi(df, p_cfg["silver_path"], "item_id", "sale_date", "sale_date")

archive_and_rotate_file(p_cfg["input_path"], p_cfg["archive_path"])
logger.info("Company Sales processing completed.")
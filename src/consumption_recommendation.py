import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, coalesce, desc, row_number, lit, trim, initcap
from pyspark.sql.window import Window

# Explicit manual bindings to preserve helper configurations safely
import utils
load_config = utils.load_config
get_logger = utils.get_logger

# Initialize Core System Component Properties
cfg = load_config()
p_cfg = cfg["recommendation"]
logger = get_logger("GoldRecommendationEngine", cfg["log_file"])

logger.info("Initializing high-availability Spark context...")
spark = (SparkSession.builder
         .appName("GoldRecommendationParquetFinalEngine")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

logger.info("Extracting transactional parquet data files while blocking out hidden metadata paths...")

# Flat structural data extraction path loops
catalog = spark.read.format("parquet").load(p_cfg["seller_catalog_silver"] + "category=*/*.parquet")
company = spark.read.format("parquet").load(p_cfg["company_sales_silver"] + "sale_date=*/*.parquet")
competitor = spark.read.format("parquet").load(p_cfg["competitor_sales_silver"] + "sale_date=*/*.parquet")

# Clean up strings and spaces across all column fields
catalog = catalog.select(
    trim(col("seller_id")).alias("seller_id"),
    trim(col("item_id")).alias("item_id"),
    trim(col("item_name")).alias("item_name"), 
    trim(initcap(coalesce(col("category"), lit("General")))).alias("category")
).filter("seller_id IS NOT NULL AND item_id IS NOT NULL").distinct()

company = company.select(
    trim(col("item_id")).alias("item_id"),
    col("units_sold").cast("int").alias("units_sold"),
    col("revenue").cast("double").alias("revenue")
).filter("item_id IS NOT NULL")

competitor = competitor.select(
    trim(col("item_id")).alias("item_id"),
    trim(col("seller_id")).alias("seller_id"),
    col("units_sold").cast("int").alias("units_sold"),
    col("revenue").cast("double").alias("revenue"),
    col("marketplace_price").cast("double").alias("marketplace_price")
).filter("item_id IS NOT NULL")

# Isolate unique item metadata mapping
item_meta = catalog.select("item_id", "item_name", "category").distinct()

# Calculate internal performance metrics (Top 10 internal items)
company_agg = company.groupBy("item_id").agg(sum("units_sold").alias("comp_units"))
comp_joined = company_agg.join(item_meta, "item_id", "inner")

window_co = Window.partitionBy("category").orderBy(desc("comp_units"))
top10_company = comp_joined.withColumn("rn", row_number().over(window_co)).filter("rn <= 10").drop("rn")

# Calculate competitor performance metrics (Top 10 marketplace products)
competitor_agg = competitor.groupBy("item_id").agg(
    sum("units_sold").alias("market_units"),
    count("seller_id").alias("seller_count"),
    sum("revenue").alias("market_revenue")
)
market_joined = competitor_agg.join(item_meta, "item_id", "inner")

window_mkt = Window.partitionBy("category").orderBy(desc("market_units"))
top10_market = market_joined.withColumn("rn", row_number().over(window_mkt)).filter("rn <= 10").drop("rn")

# Combine top products and calculate cross join recommendation matrix
all_sellers = catalog.select("seller_id").distinct()
target_items = top10_company.select("item_id", "category").union(top10_market.select("item_id", "category")).distinct()

# Build core cross join matrix
matrix = all_sellers.crossJoin(target_items)

# Run gap optimization check
missing_recs = matrix.join(catalog, ["seller_id", "item_id"], "left_anti")
if missing_recs.count() == 0:
    logger.warn("Anti-join gap analysis returned 0 rows. Falling back to full targets.")
    missing_recs = matrix

# Mathematical formulation logic before the final select to prevent duplication
processed_recs = missing_recs.join(competitor_agg, "item_id", "left") \
                             .join(company_agg, "item_id", "left")

processed_recs = processed_recs.withColumn("expected_units_sold", 
    coalesce(col("market_units") / coalesce(col("seller_count"), lit(1)), col("comp_units"), lit(1.0)))

processed_recs = processed_recs.withColumn("market_price", 
    coalesce(col("market_revenue") / col("market_units"), lit(19.99)))

processed_recs = processed_recs.withColumn("expected_revenue", col("expected_units_sold") * col("market_price"))

# SYSTEM FIX: Apply clean distinct aliases ("a" and "b") to isolate fields completely 
# and eliminate [AMBIGUOUS_REFERENCE] or ambiguous self-join exceptions
final_recs = processed_recs.alias("a").join(item_meta.alias("b"), "item_id", "inner")

final_df = final_recs.select(
    col("a.seller_id"),
    col("item_id"),
    col("b.item_name"),
    col("b.category"),  # Explicitly grabs from the metadata alias block cleanly
    col("a.market_price"),
    col("a.expected_units_sold"),
    col("a.expected_revenue")
)

# Output to disk execution block
logger.info(f"Target saving directory location: {p_cfg['output_csv']}")

if os.path.exists(p_cfg["output_csv"]):
    import shutil
    shutil.rmtree(p_cfg["output_csv"])

# Force direct file generation onto the filesystem
logger.info("Forcing direct physical write out to filesystem path...")
final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(p_cfg["output_csv"])
logger.info("Success! Recommendation CSV has been generated and saved to disk.")
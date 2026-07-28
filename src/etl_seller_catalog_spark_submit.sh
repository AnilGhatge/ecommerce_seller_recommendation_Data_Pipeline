#!/bin/bash
set -e
spark-submit \
  --name "SellerCatalog_BatchETL" \
  --master "local[*]" \
  --driver-memory 8g \
  --executor-memory 8g \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  /home/cloud/ecommerce_seller_recommendation/src/etl_seller_catalog.py \
  --config /home/cloud/ecommerce_seller_recommendation/config/ecom_prod.yml
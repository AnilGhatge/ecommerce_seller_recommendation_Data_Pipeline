#!/bin/bash
set -e

echo "=== Launching Recommendation Engine Aggregation Layer ==="

spark-submit \
  --name "Recommendation_BatchEngine" \
  --master "local[*]" \
  --driver-memory 10g \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --conf spark.kryoserializer.buffer.max=512m \
  --conf spark.driver.maxResultSize=4g \
  --conf spark.driver.memoryOverhead=2g \
  --conf spark.driver.extraJavaOptions="-XX:MaxMetaspaceSize=1g -XX:+UseG1GC" \
  /home/cloud/ecommerce_seller_recommendation/src/consumption_recommendation.py \
  --config /home/cloud/ecommerce_seller_recommendation/config/ecom_prod.yml

echo "=== Gold Recommendation Output Strategy Written Successfully ==="
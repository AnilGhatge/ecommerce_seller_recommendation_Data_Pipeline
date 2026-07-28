**End-to-End Enterprise Seller Recommendation Engine**

A production-grade, distributed batch data pipeline designed to ingest, clean, and analyze high-volume e-commerce operational logs (1,000,000+ records). Built natively using PySpark and optimized for transactional storage management via the Medallion Lakehouse Framework (Bronze → Silver → Gold).
The final engine handles dirty cross-system operational data, quarantines pipeline anomalies automatically to preserve strict data auditing, isolates catalog expansion gaps via matrix anti-joins, and outputs targeted Top-10 category product recommendations along with estimated revenue forecasts.

**🏗️ Medallion System Architecture**
To maximize single-node compute cluster velocity and eliminate memory crashes (OOM) on standard environments, this pipeline utilizes parallel block disc-backed streaming processing rather than traditional streaming event loops:text  

[ 1M+ Raw Landing Logs ] (seller_catalog_dirty.csv, company_sales_dirty.csv, competitor_sales_dirty.csv)
             │
             ▼
    1. BRONZE LAYER   ➔ Bulk Disc Ingest & Lazy Schema Inference (Parquet Blocks)
             │
             ├──► [ Valid Schema Sub-Streams ]
             │          │
             │          ▼
             │     2. SILVER LAYER   ➔ Casing, Trimming, Deduplication, & Hive Partitioning
             │          │
             │          ▼
             │     3. GOLD LAYER     ➔ Window Rankings, Catalog Cross-Joins, & Math Projections
             │          │
             │          ▼
             │     [ seller_recommend_data.csv ] ➔ Final Clean 7-Column Analytics Matrix
             │
             └──► [ Malformed Rows Deflection ]
                        │
                        ▼
                   [ QUARANTINE ZONE ] ➔ Structured JSON Audit Logs (Zero Ingestion Loss)


**📂 Repository Workspace Directory Layout**

├── config/
│   └── ecom_prod.yml               # Unified production pipeline properties
├── src/
│   ├── utils.py                    # Shared Spark Session builders & Quarantine routers
│   ├── etl_seller_catalog.py       # Ingests master catalog maps down to Silver Layer
│   ├── etl_company_sales.py        # Cleans and loads internal ledger transactions
│   ├── etl_competitor_sales.py     # normalizes global marketplace performance indexes
│   ├── consumption_recommendation.py# Computes window rankings, matrices gaps & projections
│   ├── *.sh                        # Distributed spark-submit shell optimization drivers
│   └── run_all.sh                  # Automation Master execution engine orchestrator
└── README.md                       # Repository profile guide documentation

Note: Staging storage data partitions (/bronze/, /silver/, /gold/, /quarantine/) are intentionally excluded from version control metrics via matching rules to prevent file allocations leakage.

**📊 Pipeline Quality Control & Quarantine Gateways**
Every script automatically diverts malformed runtime records to a specialized audit directory without crashing driver executors.

Example Captured Anomaly Payload (quarantine/company_sales/*.json)

json{
  "dataset_name": "company_sales",
  "original_record": "{\"item_id\":\"ITEM-9042\",\"units_sold\":\"-14\",\"revenue\":\"350.00\",\"sale_date\":\"2026-07-28\"}",
  "dq_failure_reason": "units_negative"
}

**🚀 Execution & Dynamic Validation Guide**

1. Sequential Pipeline Automation Run
   Trigger the consolidated production script framework directly from your server context terminal path to drive all layers sequentially
   :bashchmod +x src/*.sh
   ./src/run_all.sh

2. Verify Output Generation Format
   Preview the completed, single-partition report data structure generated inside your local path directory workspace
   :bash
   head -n 5 gold/recommendations_csv/seller_recommend_data.csv/part-*.csv

**🎯 Expected 7-Column Evaluation Matrix Schema**
The engine compiles analytical returns into this exact assignment-specified projection layout structure:
textseller_id,item_id,item_name,category,market_price,expected_units_sold,expected_revenue
SELL-0012,ITEM-9981,Wireless Mouse,General,19.99,150.0,2998.5
SELL-0012,ITEM-4412,Running Shoes,General,59.50,42.3,2516.85
SELL-3042,ITEM-1102,Mechanical Keyboard,General,89.00,105.0,9345.0
SELL-1105,ITEM-0042,Leather Wallet,General,15.25,12.0,183.00

**⚡ Production Cluster Performance Optimization**
To prevent local worker deadlocks and accommodate structural data size over 1M+ items, all spark-submit command wrappers incorporate these heavy-throughput system tuning options:
1. --driver-memory 10g --driver-memoryOverhead 2g: Prevents JVM out-of-memory overhead freeze drops during large cross-joins matrix processing.
2. 2. --conf spark.serializer=org.apache.spark.serializer.KryoSerializer: Boosts network/disc serialization speed up to 10x over default options.--conf spark.driver.extraJavaOptions="-XX:MaxMetaspaceSize=1g -XX:+UseG1GC": Expands metadata class-loading borders for heavy Hudi/AWS bundle integrations.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_PATH = "data/bronze/airports"
WAREHOUSE_PATH = "data/warehouse"

def build_spark_session():
    return (SparkSession.builder
        .appName("silver-airports-iceberg")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
        .getOrCreate())

def main():
    spark = build_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    
    df = spark.read.parquet(BRONZE_PATH)
    silver_df = (df
        .withColumn("airport_id", F.col("airport_id").cast("int"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .filter(F.col("airport_id").isNotNull())
        .dropDuplicates(["airport_id"]))
    
    silver_df.writeTo("local.silver.airports").using("iceberg").createOrReplace()
    spark.sql("SELECT count(*) AS total FROM local.silver.airports").show()
    spark.stop()

if __name__ == "__main__":
    main()
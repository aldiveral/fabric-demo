import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def create_spark_session(app_name: str = "Fabric_Local_Test") -> SparkSession:
    """Get Spark session (Fabric auto provides one, local must create)."""
    try:
        # In Fabric, spark is pre-defined
        spark
        print("⚡ Using Fabric Spark session")
        return spark
    except NameError:
        print("🧱 Creating local Spark session...")
        return (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "4g")
            .config("spark.hadoop.fs.file.impl.disable.cache", "true")
            .getOrCreate()
        )

def read_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read CSV (works in Fabric Lakehouse or local paths)."""
    print(f"📥 Reading CSV from {path}")
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

def write_csv(df: DataFrame, path: str, mode: str = "overwrite"):
    """Write Spark DataFrame to CSV."""
    print(f"💾 Writing CSV to {path}")
    (
        df.write
        .option("header", True)
        .mode(mode)
        .csv(path)
    )

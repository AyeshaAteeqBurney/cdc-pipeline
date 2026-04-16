import os
from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """
    SparkSession pre-wired for:
    - Iceberg REST catalog (MinIO-backed)
    - Kafka source (via PYSPARK_SUBMIT_ARGS in compose.yml)
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", os.environ.get("ICEBERG_URI", "http://iceberg-rest:8181"))
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.sql.catalog.lakehouse.s3.region", os.environ.get("AWS_REGION", "us-east-1"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


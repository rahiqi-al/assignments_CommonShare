import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from batch.batchConfig.config import config
import logging

logger = logging.getLogger(__name__)

try:
    # Spark session config
    conf = (
        pyspark.SparkConf()
        .setAppName("GoldModelization")
        .set("spark.jars.packages",
             "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
             "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,"
             "org.apache.hadoop:hadoop-aws:3.3.4")
        .set("spark.sql.extensions",
             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
             "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.uri", config.nessie_uri)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.warehouse", config.warehouse)
        .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .set("spark.hadoop.fs.s3a.access.key", config.aws_access_key)
        .set("spark.hadoop.fs.s3a.secret.key", config.aws_secret_key)
        .set("spark.hadoop.fs.s3a.endpoint", config.aws_s3_endpoint)
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .set("spark.sql.defaultCatalog", "nessie")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger.info("Spark session started for Gold modelization")

    # Read Silver
    df_silver = spark.table("nessie.silver.fact_certifications")

    # Corrected dimension tables
    dim_company = df_silver.select("company_id", "name").dropDuplicates()
    dim_certification = df_silver.select("certificate_id", "standard_name").dropDuplicates()

    # Fact table
    fact_cert = df_silver.select("record_id", "company_id", "certificate_id")

    # Write Gold
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    
    spark.sql("DROP TABLE IF EXISTS nessie.gold.dim_company")
    dim_company.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_company")

    spark.sql("DROP TABLE IF EXISTS nessie.gold.dim_certification")
    dim_certification.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_certification")

    spark.sql("DROP TABLE IF EXISTS nessie.gold.fact_certifications")
    fact_cert.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.fact_certifications")

    logger.info("Gold tables (facts + dims) written successfully")

except Exception as e:
    logger.exception(f"Error in Gold modelization: {e}")
    raise
finally:
    spark.stop()
    logger.info("Spark session stopped for Gold")

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, when, split, trim, upper, monotonically_increasing_id
from batch.batchConfig.config import config
import logging

logger = logging.getLogger(__name__)



try :
    conf = (
        pyspark.SparkConf()
        .setAppName("IcebergNessieMinio")
        .set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
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
    logger.info("Spark session started for Silver transformation")

    # read bronze csv
    bronze_path = f"s3a://{config.bucket_name}/{config.bronze_prefix}"
    df_companies = spark.read.option("header", True).csv(f"{bronze_path}companies.csv")
    df_certifications = spark.read.option("header", True).csv(f"{bronze_path}certifications.csv")
    df_company_cert = spark.read.option("header", True).csv(f"{bronze_path}company_certificates.csv")

    # join tables
    df_silver = df_company_cert.join(df_companies, "company_id", "left") \
                               .join(df_certifications, "certificate_id", "left")

    # Clean text columns
    for c in ["name", "standard_name"]:
        if c in df_silver.columns:
            df_silver = df_silver.withColumn(c, upper(trim(col(c))))

    # surrogate key
    df_silver = df_silver.withColumn("record_id", monotonically_increasing_id())

    # write to silver
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    spark.sql("DROP TABLE IF EXISTS nessie.silver.fact_certifications")
    df_silver.write.format("iceberg").mode("overwrite").saveAsTable("nessie.silver.fact_certifications")
    logger.info("Silver table written successfully")

except Exception as e:
    logger.exception(f"Error in Silver transformation: {e}")
    raise
finally:
    spark.stop()
    logger.info("Spark session stopped for Silver")

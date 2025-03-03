import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    FloatType,
    IntegerType,
    StringType,
)
from pyspark.sql.functions import from_json, col

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)
logger = logging.getLogger("spark_structured_streaming")

# Configuration constants
KAFKA_BOOTSTRAP_SERVERS = "kafka1:19092,kafka2:19093,kafka3:19094"
KAFKA_TOPIC = "random_names"

CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = "9042"
CASSANDRA_USERNAME = "cassandra"
CASSANDRA_PASSWORD = "cassandra"
CASSANDRA_KEYSPACE = "spark_streaming"
CASSANDRA_TABLE = "random_names"

SPARK_JARS_PACKAGES = (
    "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0"
)


def create_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession configured for Cassandra and Kafka.
    """
    try:
        spark = (
            SparkSession.builder.appName("SparkStructuredStreaming")
            .config("spark.jars.packages", SPARK_JARS_PACKAGES)
            .config("spark.cassandra.connection.host", CASSANDRA_HOST)
            .config("spark.cassandra.connection.port", CASSANDRA_PORT)
            .config("spark.cassandra.auth.username", CASSANDRA_USERNAME)
            .config("spark.cassandra.auth.password", CASSANDRA_PASSWORD)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error("Failed to create Spark session: %s", e)
        raise


def create_initial_dataframe(spark_session: SparkSession):
    """
    Reads streaming data from Kafka and returns the initial DataFrame.
    """
    try:
        df = (
            spark_session.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )
        logger.info("Initial DataFrame created successfully.")
        return df
    except Exception as e:
        logger.error("Error creating initial DataFrame: %s", e)
        raise


def create_final_dataframe(df, spark_session: SparkSession):
    """
    Transforms the initial Kafka DataFrame by parsing the JSON value and selecting the required fields.
    """
    schema = StructType([
        StructField("full_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("location", StringType(), False),
        StructField("city", StringType(), False),
        StructField("country", StringType(), False),
        StructField("postcode", IntegerType(), False),
        StructField("latitude", FloatType(), False),
        StructField("longitude", FloatType(), False),
        StructField("email", StringType(), False)
    ])

    try:
        df_final = (
            df.selectExpr("CAST(value AS STRING)")
              .select(from_json(col("value"), schema).alias("data"))
              .select("data.*")
        )
        logger.info("Final DataFrame created successfully with schema: %s", df_final.schema)
        return df_final
    except Exception as e:
        logger.error("Error creating final DataFrame: %s", e)
        raise


def start_streaming(df):
    """
    Starts the streaming job that writes data to the Cassandra table.
    """
    try:
        logger.info("Starting stream to Cassandra table %s.%s", CASSANDRA_KEYSPACE, CASSANDRA_TABLE)
        query = (
            df.writeStream
              .format("org.apache.spark.sql.cassandra")
              .outputMode("append")
              .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE)
              .start()
        )
        query.awaitTermination()
    except Exception as e:
        logger.error("Error during streaming: %s", e)
        raise


def write_streaming_data():
    """
    Orchestrates the streaming pipeline:
    - Creates a Spark session.
    - Reads streaming data from Kafka.
    - Transforms the data.
    - Writes the transformed data to Cassandra.
    """
    try:
        spark = create_spark_session()
        df_initial = create_initial_dataframe(spark)
        df_final = create_final_dataframe(df_initial, spark)
        start_streaming(df_final)
    except Exception as e:
        logger.error("Streaming pipeline failed: %s", e)
        raise


if __name__ == "__main__":
    write_streaming_data()

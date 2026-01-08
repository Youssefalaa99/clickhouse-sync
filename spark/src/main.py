import sys
from utils.logging import get_logger
from config.settings import *
from pyspark.sql import SparkSession

logger = get_logger(__name__)


def create_spark_conn():
    try:
        spark = SparkSession.builder \
                        .appName("Postgres to ClickHouse") \
                        .master("local[*]") \
                        .config("spark.jars", "jars/clickhouse-jdbc-0.9.4-all.jar,"
                                "jars/postgresql-42.5.5.jar") \
                        .getOrCreate()
        spark.sparkContext.setLogLevel("INFO")
    except Exception as e:
        logger.error(f"Error while creating spark session: {e}")
        sys.exit(1)
    return spark

def get_last_ts(spark):
    try: 
        last_updated_ts_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:ch://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}") \
        .option("user", CLICKHOUSE_USER) \
        .option("password", CLICKHOUSE_PASS) \
        .option("driver", CLICKHOUSE_DRIVER) \
        .option("query", f"SELECT max(updated_at) as max_val FROM {TBL_NAME}") \
        .load()

        # Extract the value, default to 0 if table is empty
        last_val = last_updated_ts_df.collect()[0]['max_val']
        
        # Can also implement safety window/watermark for late arriving records. Ex:
        # safety_window_ms = 7 * 24 * 60 * 60 * 1000 
        # lookback_val = last_val - safety_window_ms
        
        if last_val is None: last_val = 0
        return last_val
    
    except Exception as e:
        logger.error(f"Error while fetching last_updated_ts: {e}")
        spark.stop()
        sys.exit(1)

def read_postgres_table(spark):
    # Get last updated_at value from CH table
    last_val = get_last_ts(spark)

    logger.info(f"Read from table where updated_at > {last_val}")
    try:
        # Read from Postgres
        pg_query = f"(SELECT * FROM data_source.{TBL_NAME} WHERE updated_at > {last_val}) as subq"
        
        pg_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("dbtable", pg_query) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASS) \
            .option("driver", POSTGRES_DRIVER) \
            .load()
        return pg_df
    
    except Exception as e:
        logger.error(f"Error while reading postgres table: {e}")
        spark.stop()
        sys.exit(1)


def write_clickhouse_table(pg_df):
    logger.info(f"Writing records to clickhouse...")
    try:
        pg_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:ch://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}") \
            .option("user", CLICKHOUSE_USER) \
            .option("password", CLICKHOUSE_PASS) \
            .option("dbtable", TBL_NAME) \
            .option("driver", CLICKHOUSE_DRIVER) \
            .mode("append") \
            .save()
        
        count = pg_df.count()
        
        logger.info(f"Sync complete. Records synced: {count}")

    except Exception as e:
        logger.error(f"Error while writing to clickhouse table: {e}")
        spark.stop()
        sys.exit(1)





if __name__ == "__main__":
    spark = create_spark_conn()
    pg_df = read_postgres_table(spark)
    # pg_df.show(5)
    # pg_df.printSchema()
    write_clickhouse_table(pg_df)
    spark.stop()


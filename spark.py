from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, TimestampType, DateType, StringType
from pyspark.sql.functions import struct, lit, to_json, from_json, col, year, to_timestamp, to_date, month, sum as spark_sum
import os


# def write_to_kafka(df, key_columns, agg_type, kafka_bootstrap_servers, output_topic):
#         kafka_df = df.select(
#             struct(*[col(k) for k in key_columns]).alias("key"),
#             to_json(struct([col(c) for c in df.columns], lit(agg_type).alias("type"))).alias("value")
#         ).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

#         query = kafka_df.writeStream \
#             .format("kafka") \
#             .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#             .option("topic", output_topic) \
#             .option("checkpointLocation", f"/tmp/spark_checkpoints/{'_'.join(key_columns)}") \
#             .outputMode("update") \
#             .start()
#         return query

def write_to_kafka(df, key_columns, agg_type, kafka_bootstrap_servers, output_topic):
    kafka_df = df.select(
        struct(*[col(k) for k in key_columns]).alias("key"),
        to_json(
            struct(
                *[col(c) for c in df.columns],  # Include all existing columns
                lit(agg_type).alias("type")    # Add `type` as a top-level field
            )
        ).alias("value")
    ).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    query = kafka_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .option("checkpointLocation", f"/tmp/spark_checkpoints/{'_'.join(key_columns)}") \
        .outputMode("update") \
        .start()
    return query


# Define the main function
def main():
    # Create a Spark session
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'
    
    spark = SparkSession.builder \
        .appName("SalesAggregator") \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    spark = (
        SparkSession 
        .builder 
        .appName("Streaming from Kafka") 
        .config("spark.streaming.stopGracefullyOnShutdown", True) 
        # .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:2.8.1')
        .config("spark.sql.shuffle.partitions", 4)
        .master("local[*]") 
        .getOrCreate()
    )

    # Kafka configurationcls

    kafka_bootstrap_servers = "20.40.52.216:9092"
    kafka_topic = "orderTopic"
    output_topic = "aggregatedOrderSales"

    # Define the schema
    schema = StructType([
        StructField("orderId", LongType(), True),
        StructField("sellerId", LongType(), True),
        StructField("productId", LongType(), True),
        StructField("productPrice", DoubleType(), True),
        StructField("productQuantity", LongType(), True),
        StructField("categoryId", LongType(), True),
        StructField("date", StringType(), True)
    ])

    
    # Read messages from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract and parse the value field as JSON
    kafka_values = kafka_df.selectExpr("CAST(value AS STRING)")
    parsed_df = kafka_values.select(from_json(col("value"), schema).alias("data")).select( col("data.*"),
    to_date(col("data.date"), "yyyy-MM-dd").alias("parsed_date"))

    enriched_df = parsed_df.withColumn("year", year(col("parsed_date"))) \
                           .withColumn("month", month(col("parsed_date"))) \
                           .withColumn("totalSales", col("productPrice") * col("productQuantity"))

    # Group and aggregate
    aggregated_df = enriched_df.groupBy(
        "sellerId", "categoryId", "productId", "year", "month"
    ).agg(
        spark_sum("totalSales").alias("totalSales"),
        spark_sum("productQuantity").alias("totalQuantity")
    )

    agg_product_month = enriched_df.groupBy("sellerId", "productId", "year", "month") \
                                   .agg(spark_sum("totalSales").alias("monthly_sales_product"))

    agg_product_year = enriched_df.groupBy("sellerId", "productId", "year") \
                                  .agg(spark_sum("totalSales").alias("annual_sales_product"))

    agg_category_month = enriched_df.groupBy("sellerId", "categoryId", "year", "month") \
                                    .agg(spark_sum("totalSales").alias("monthly_sales_category"))

    agg_category_year = enriched_df.groupBy("sellerId", "categoryId", "year") \
                                   .agg(spark_sum("totalSales").alias("annual_sales_category"))

    total_monthly_sales = enriched_df.groupBy("sellerId", "year", "month") \
                                     .agg(spark_sum("totalSales").alias("total_monthly_sales"))

    total_annual_sales = enriched_df.groupBy("sellerId", "year") \
                                    .agg(spark_sum("totalSales").alias("total_annual_sales"))

    # Combine all aggregated tables into a list for writing to Kafka
    tables = [
        (agg_product_month, ["sellerId", "productId", "year", "month"]),
        (agg_product_year, ["sellerId", "productId", "year"]),
        (agg_category_month, ["sellerId", "categoryId", "year", "month"]),
        (agg_category_year, ["sellerId", "categoryId", "year"]),
        (total_monthly_sales, ["sellerId", "year", "month"]),
        (total_annual_sales, ["sellerId", "year"])
    ]


    # Print parsed messages to the console
    # query = parsed_df.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()
    
    # query = aggregated_df.writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()

    queries = []
    # for df, keys in tables:
    #     queries.append(write_to_kafka(df, keys, output_topic, kafka_bootstrap_servers))
    queries.append(write_to_kafka(agg_product_month, tables[0][1], "agg_product_month", kafka_bootstrap_servers, output_topic))
    queries.append(write_to_kafka(agg_product_year, tables[1][1], "agg_product_year", kafka_bootstrap_servers, output_topic))
    queries.append(write_to_kafka(agg_category_month, tables[2][1], "agg_category_month", kafka_bootstrap_servers, output_topic))
    queries.append(write_to_kafka(agg_category_year, tables[3][1], "agg_category_year", kafka_bootstrap_servers, output_topic))
    queries.append(write_to_kafka(total_monthly_sales, tables[4][1], "total_monthly_sales", kafka_bootstrap_servers, output_topic))
    queries.append(write_to_kafka(total_annual_sales, tables[5][1], "total_annual_sales", kafka_bootstrap_servers, output_topic))
    # Wait for all queries to terminate
    for query in queries:
        query.awaitTermination()
    # Wait for the query to terminate
    # query.awaitTermination()

# Run the main function
if __name__ == "__main__":
    main()

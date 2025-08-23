#!/usr/bin/env python3
"""
Temperature Data Streaming Processor
Processes IoT temperature data from Kafka and performs aggregations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TemperatureProcessor:
    def __init__(self):
        self.spark = None
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.postgres_host = os.getenv('POSTGRES_HOST', 'postgres')
        self.postgres_db = os.getenv('POSTGRES_DB', 'iot_data')
        self.postgres_user = os.getenv('POSTGRES_USER', 'iot_user')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', 'iot_password')
        self.hdfs_namenode = os.getenv('HDFS_NAMENODE', 'hdfs://namenode:9000')
        
    def create_spark_session(self):
        """Create Spark session with required configurations"""
        self.spark = SparkSession.builder \
            .appName("IoT-Temperature-Processor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.checkpointLocation", "/app/checkpoints/temperature") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        
    def define_schema(self):
        """Define schema for temperature data"""
        return StructType([
            StructField("event_timestamp", StringType(), True),   # FIX: was 'timestamp'
            StructField("timestamp_epoch", LongType(), True),
            StructField("device_id", StringType(), True),
            StructField("temperature_fahrenheit", DoubleType(), True),
            StructField("temperature_celsius", DoubleType(), True),
            StructField("message_type", StringType(), True),
            StructField("producer_timestamp", StringType(), True)
        ])

    
    def read_kafka_stream(self):
        """Read temperature data from Kafka"""
        schema = self.define_schema()
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "iot-temperature") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON data
        parsed_df = df.select(
            col("key").cast("string").alias("message_key"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), schema).alias("data")
        ).select(
            col("message_key"),
            col("kafka_timestamp"),
            col("data.*")
        )
        
        # Convert timestamp string to timestamp type
        processed_df = parsed_df.withColumn(
            "event_timestamp", to_timestamp(col("event_timestamp"))
        ).withColumn(
            "producer_timestamp", to_timestamp(col("producer_timestamp"))   # <---- FIX
        ).withColumn(
            "processing_timestamp", current_timestamp()
        ).drop("timestamp")  # to avoid Kafka metadata confusion



        
        return processed_df
    
    def detect_anomalies(self, df):
        """Detect temperature anomalies"""
        # Simple anomaly detection based on temperature thresholds
        anomaly_df = df.withColumn(
            "is_anomaly",
            when(
                (col("temperature_celsius") > 40) | 
                (col("temperature_celsius") < -20) |
                (col("temperature_fahrenheit") > 104) |
                (col("temperature_fahrenheit") < -4),
                lit(True)
            ).otherwise(lit(False))
        ).withColumn(
            "anomaly_type",
            when(col("temperature_celsius") > 40, lit("HIGH_TEMP"))
            .when(col("temperature_celsius") < -20, lit("LOW_TEMP"))
            .otherwise(lit("NORMAL"))
        )
        
        return anomaly_df
    
    def create_aggregations(self, df):
        """Create time-based aggregations"""
        # 1-minute tumbling window aggregations
        window_1min = df \
            .withWatermark("event_timestamp", "2 minutes") \
            .groupBy(
                window(col("event_timestamp"), "1 minute"),
                col("device_id")
            ) \
            .agg(
                avg("temperature_celsius").alias("avg_temp_celsius"),
                avg("temperature_fahrenheit").alias("avg_temp_fahrenheit"),
                max("temperature_celsius").alias("max_temp_celsius"),
                min("temperature_celsius").alias("min_temp_celsius"),
                count("*").alias("reading_count"),
                sum(when(col("is_anomaly") == True, 1).otherwise(0)).alias("anomaly_count")
            ) \
            .withColumn("window_duration", lit("1_minute")) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        # 5-minute tumbling window aggregations
        window_5min = df \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                col("device_id")
            ) \
            .agg(
                avg("temperature_celsius").alias("avg_temp_celsius"),
                avg("temperature_fahrenheit").alias("avg_temp_fahrenheit"),
                max("temperature_celsius").alias("max_temp_celsius"),
                min("temperature_celsius").alias("min_temp_celsius"),
                count("*").alias("reading_count"),
                sum(when(col("is_anomaly") == True, 1).otherwise(0)).alias("anomaly_count")
            ) \
            .withColumn("window_duration", lit("5_minute")) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        return window_1min, window_5min
    
    def write_to_postgres(self, df, table_name):
        """Write DataFrame to PostgreSQL"""
        postgres_url = f"jdbc:postgresql://{self.postgres_host}:5432/{self.postgres_db}"
        
        def write_batch(batch_df, batch_id):
            try:
                batch_df.write \
                    .mode("append") \
                    .format("jdbc") \
                    .option("url", postgres_url) \
                    .option("dbtable", table_name) \
                    .option("user", self.postgres_user) \
                    .option("password", self.postgres_password) \
                    .option("driver", "org.postgresql.Driver") \
                    .save()
                
                logger.info(f"Batch {batch_id} written to {table_name} - {batch_df.count()} records")
                
            except Exception as e:
                logger.error(f"Error writing batch {batch_id} to {table_name}: {e}")
        
        return write_batch
    
    def write_to_hdfs(self, df, path):
        """Write DataFrame to HDFS"""
        def write_batch(batch_df, batch_id):
            try:
                hdfs_path = f"{self.hdfs_namenode}/{path}/batch_{batch_id}"
                
                batch_df.write \
                    .mode("append") \
                    .format("parquet") \
                    .option("path", hdfs_path) \
                    .save()
                
                logger.info(f"Batch {batch_id} written to HDFS: {hdfs_path}")
                
            except Exception as e:
                logger.error(f"Error writing batch {batch_id} to HDFS: {e}")
        
        return write_batch
    
    def run_processor(self):
        """Main processing logic"""
        logger.info("Starting Temperature Data Processor")
        
        # Create Spark session
        self.create_spark_session()
        
        try:
            # Read from Kafka
            raw_df = self.read_kafka_stream()
            
            # Detect anomalies
            anomaly_df = self.detect_anomalies(raw_df)
            
            # Create aggregations
            agg_1min, agg_5min = self.create_aggregations(anomaly_df)
            
            # Write raw data to PostgreSQL
            raw_query = anomaly_df.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_postgres(anomaly_df, "temperature_readings")) \
                .trigger(processingTime='10 seconds') \
                .option("checkpointLocation", "/app/checkpoints/temperature/raw") \
                .start()
            
            # Write 1-minute aggregations to PostgreSQL
            agg_1min_query = agg_1min.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_postgres(agg_1min, "temperature_aggregations_1min")) \
                .trigger(processingTime='1 minute') \
                .option("checkpointLocation", "/app/checkpoints/temperature/agg_1min") \
                .start()
            
            # Write 5-minute aggregations to PostgreSQL
            agg_5min_query = agg_5min.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_postgres(agg_5min, "temperature_aggregations_5min")) \
                .trigger(processingTime='5 minutes') \
                .option("checkpointLocation", "/app/checkpoints/temperature/agg_5min") \
                .start()
            
            # Write raw data to HDFS for long-term storage
            hdfs_query = anomaly_df.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_hdfs(anomaly_df, "iot_data/temperature/raw")) \
                .trigger(processingTime='30 seconds') \
                .option("checkpointLocation", "/app/checkpoints/temperature/hdfs") \
                .start()
            
            # Console output for debugging
            console_query = anomaly_df.filter(col("is_anomaly") == True).writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", False) \
                .trigger(processingTime='30 seconds') \
                .start()
            
            logger.info("All streaming queries started successfully")
            
            # Wait for all queries
            raw_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in temperature processor: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    processor = TemperatureProcessor()
    processor.run_processor()

if __name__ == "__main__":
    main()
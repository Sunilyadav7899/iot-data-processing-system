#!/usr/bin/env python3
"""
Window Event Data Streaming Processor
Processes IoT window sensor data from Kafka
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WindowEventProcessor:
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
            .appName("IoT-Window-Processor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.checkpointLocation", "/app/checkpoints/window") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        
    def define_schema(self):
        """Define schema for window event data"""
        return StructType([
            StructField("event_timestamp", StringType(), True),
            StructField("object_code", IntegerType(), True),
            StructField("object_name", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("message_type", StringType(), True),
            StructField("producer_timestamp", StringType(), True)
        ])
    
    def read_kafka_stream(self):
        """Read window event data from Kafka"""
        schema = self.define_schema()
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "iot-window-events") \
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
            "processing_timestamp", current_timestamp()
        )

        
        return processed_df
    
    def calculate_window_durations(self, df):
        """Calculate window open durations by correlating open/close events"""
        # Separate open and close events
        open_events = df.filter(col("event_type") == "opened").alias("open")
        close_events = df.filter(col("event_type") == "closed").alias("close")
        
        # Join open and close events to calculate duration
        duration_df = open_events.join(
            close_events,
            (col("open.object_code") == col("close.object_code")) &
            (col("open.event_timestamp") < col("close.event_timestamp")),
            "inner"
        ).select(
            col("open.object_code").alias("object_code"),
            col("open.object_name").alias("object_name"),
            col("open.event_timestamp").alias("opened_at"),
            col("close.event_timestamp").alias("closed_at"),
            (unix_timestamp(col("close.event_timestamp")) - 
             unix_timestamp(col("open.event_timestamp"))).alias("duration_seconds")
        ).withColumn(
            "duration_minutes", 
            col("duration_seconds") / 60
        ).withColumn(
            "processing_timestamp",
            current_timestamp()
        )
        
        return duration_df
    
    def create_window_aggregations(self, df):
        """Create time-based aggregations for window events"""
        # Hourly aggregations
        hourly_agg = df \
            .withWatermark("event_timestamp", "1 hour") \
            .groupBy(
                window(col("event_timestamp"), "1 hour"),
                col("object_code"),
                col("object_name"),
                col("event_type")
            ) \
            .agg(
                count("*").alias("event_count")
            ) \
            .withColumn("aggregation_period", lit("hourly")) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        # Daily aggregations
        daily_agg = df \
            .withWatermark("event_timestamp", "1 day") \
            .groupBy(
                window(col("event_timestamp"), "1 day"),
                col("object_code"),
                col("object_name"),
                col("event_type")
            ) \
            .agg(
                count("*").alias("event_count")
            ) \
            .withColumn("aggregation_period", lit("daily")) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        return hourly_agg, daily_agg
    
    def detect_window_patterns(self, duration_df):
        """Detect patterns in window usage"""
        pattern_df = duration_df.withColumn(
            "usage_pattern",
            when(col("duration_minutes") < 5, lit("BRIEF_OPENING"))
            .when(col("duration_minutes") < 30, lit("SHORT_OPENING"))
            .when(col("duration_minutes") < 120, lit("MEDIUM_OPENING"))
            .otherwise(lit("LONG_OPENING"))
        ).withColumn(
            "is_unusual",
            when(col("duration_minutes") > 300, lit(True))  # > 5 hours
            .otherwise(lit(False))
        )
        
        return pattern_df
    
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
        logger.info("Starting Window Event Data Processor")
        
        # Create Spark session
        self.create_spark_session()
        
        try:
            # Read from Kafka
            raw_df = self.read_kafka_stream()
            
            # Create aggregations
            hourly_agg, daily_agg = self.create_window_aggregations(raw_df)
            
            # Calculate window durations (for stateful processing)
            # Note: This is simplified - in production, you'd use more sophisticated state management
            
            # Write raw events to PostgreSQL
            raw_query = raw_df.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_postgres(raw_df, "window_events")) \
                .trigger(processingTime='15 seconds') \
                .option("checkpointLocation", "/app/checkpoints/window/raw") \
                .start()
            
            # Write hourly aggregations
            hourly_query = hourly_agg.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_postgres(hourly_agg, "window_aggregations_hourly")) \
                .trigger(processingTime='1 hour') \
                .option("checkpointLocation", "/app/checkpoints/window/hourly") \
                .start()
            
            # Write daily aggregations
            daily_query = daily_agg.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_postgres(daily_agg, "window_aggregations_daily")) \
                .trigger(processingTime='1 day') \
                .option("checkpointLocation", "/app/checkpoints/window/daily") \
                .start()
            
            # Write to HDFS for long-term storage
            hdfs_query = raw_df.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_hdfs(raw_df, "iot_data/window_events/raw")) \
                .trigger(processingTime='1 minute') \
                .option("checkpointLocation", "/app/checkpoints/window/hdfs") \
                .start()
            
            # Console output for debugging
            console_query = raw_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", False) \
                .trigger(processingTime='30 seconds') \
                .start()
            
            logger.info("All window event streaming queries started successfully")
            
            # Wait for termination
            raw_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in window event processor: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    processor = WindowEventProcessor()
    processor.run_processor()

if __name__ == "__main__":
    main()
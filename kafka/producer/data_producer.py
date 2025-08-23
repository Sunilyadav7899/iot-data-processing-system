#!/usr/bin/env python3
"""
IoT Data Producer for Kafka
Streams temperature and window sensor data to Kafka topics
"""

import json
import time
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime, timedelta
import logging
import os
from typing import Dict, Any
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

    
def parse_time_safe(date_str, time_str):
    """Try parsing times with and without seconds"""
    for fmt in ["%Y-%m-%d %I:%M:%S %p", "%Y-%m-%d %I:%M %p"]:
        try:
            return datetime.strptime(f"{date_str} {time_str}", fmt)
        except ValueError:
            continue
    return pd.NaT

class IoTDataProducer:
    def __init__(self, kafka_servers: str = 'kafka:29092'):
        self.kafka_servers = kafka_servers
        self.producer = None
        self.temperature_topic = 'iot-temperature'
        self.window_topic = 'iot-window-events'
        
    def connect_kafka(self) -> bool:
        """Connect to Kafka with retry logic"""
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers.split(','),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    batch_size=16384,
                    linger_ms=10,
                    buffer_memory=33554432
                )
                
                # Test connection
                future = self.producer.send(self.temperature_topic, {'test': 'connection'})
                future.get(timeout=10)
                
                logger.info(f"Successfully connected to Kafka at {self.kafka_servers}")
                return True
                
            except Exception as e:
                logger.warning(f"Kafka connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to Kafka after all retries")
                    return False
        
        return False


    
    def load_data(self) -> tuple:
        try:
            data_path = os.getenv('DATA_PATH', '/app/data')

            # Load temperature data
            temp_file = os.path.join(data_path, 'temperature.csv')
            temp_df = pd.read_csv(temp_file)
            temp_df['timestamp_utc'] = pd.to_datetime(temp_df['timestamp_utc'], format='%Y-%m-%d %H:%M:%S')
            temp_df['device_id'] = temp_df['device_id'].astype(str)
            temp_df = temp_df.sort_values('timestamp_utc')
            logger.info(f"Loaded {len(temp_df)} temperature records")

            # Load window data
            window_file = os.path.join(data_path, 'window_opened_closed.csv')
            window_df = pd.read_csv(window_file)

            # Clean and prepare window data
            window_df['DayPST'] = pd.to_datetime(window_df['DayPST'], errors='coerce')

            window_df['StartTimePST'] = [
                parse_time_safe(d.strftime("%Y-%m-%d"), t)
                for d, t in zip(window_df['DayPST'], window_df['StartTimePST'])
            ]
            window_df['EndTimePST'] = [
                parse_time_safe(d.strftime("%Y-%m-%d"), t)
                for d, t in zip(window_df['DayPST'], window_df['EndTimePST'])
            ]

            window_df = window_df.sort_values('StartTimePST')
            logger.info(f"Loaded {len(window_df)} window event records")

            return temp_df, window_df

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None, None
    
    def create_temperature_message(self, row) -> Dict[str, Any]:
        """Create temperature message for Kafka"""
        return {
            'event_timestamp': row['timestamp_utc'].isoformat(),
            'timestamp': row['timestamp_utc'].isoformat(),
            'timestamp_epoch': int(row['timestamp_utc'].timestamp()),  # <-- FIXED
            'device_id': row['device_id'],
            'temperature_fahrenheit': float(row['temp_f']),
            'temperature_celsius': float(row['temp_c']),
            'message_type': 'temperature_reading',
            'producer_timestamp': datetime.utcnow().isoformat()
        }

    
    def create_window_message(self, row, event_type: str) -> Dict[str, Any]:
        """Create window event message for Kafka"""
        timestamp = row['StartTimePST'] if event_type == 'opened' else row['EndTimePST']
        return {
            'timestamp': timestamp.isoformat(),
            'object_code': int(row['ObjectCode']),
            'object_name': row['ObjectName'],
            'event_type': event_type,
            'message_type': 'window_event',
            'producer_timestamp': datetime.utcnow().isoformat()
        }
    
    def stream_temperature_data(self, df: pd.DataFrame, delay_ms: int = 100):
        """Stream temperature data to Kafka"""
        logger.info(f"Starting temperature data streaming with {delay_ms}ms delay")
        
        for idx, row in df.iterrows():
            try:
                message = self.create_temperature_message(row)
                future = self.producer.send(
                    self.temperature_topic,
                    value=message,
                    key=row['device_id']
                )
                
                # Log every 100 messages
                if idx % 100 == 0:
                    logger.info(f"Sent temperature message {idx+1}/{len(df)}")
                
                time.sleep(delay_ms / 1000.0)
                
            except Exception as e:
                logger.error(f"Error sending temperature message {idx}: {e}")
        
        logger.info("Completed temperature data streaming")
    
    def stream_window_data(self, df: pd.DataFrame, delay_ms: int = 500):
        """Stream window event data to Kafka"""
        logger.info(f"Starting window data streaming with {delay_ms}ms delay")
        
        # Create events for both open and close
        events = []
        for idx, row in df.iterrows():
            # Add open event
            events.append((row['StartTimePST'], row, 'opened'))
            # Add close event
            events.append((row['EndTimePST'], row, 'closed'))
        
        # Sort by timestamp
        events.sort(key=lambda x: x[0])
        
        for idx, (timestamp, row, event_type) in enumerate(events):
            try:
                message = self.create_window_message(row, event_type)
                future = self.producer.send(
                    self.window_topic,
                    value=message,
                    key=f"{row['ObjectCode']}_{event_type}"
                )
                
                if idx % 20 == 0:
                    logger.info(f"Sent window event {idx+1}/{len(events)}")
                
                time.sleep(delay_ms / 1000.0)
                
            except Exception as e:
                logger.error(f"Error sending window message {idx}: {e}")
        
        logger.info("Completed window data streaming")
    
    def run_producer(self):
        """Main producer execution"""
        logger.info("Starting IoT Data Producer")
        
        # Connect to Kafka
        if not self.connect_kafka():
            logger.error("Failed to connect to Kafka. Exiting.")
            sys.exit(1)
        
        # Load data
        temp_df, window_df = self.load_data()
        if temp_df is None or window_df is None:
            logger.error("Failed to load data. Exiting.")
            sys.exit(1)
        
        try:
            # Stream data in parallel (simplified sequential for this implementation)
            logger.info("Starting data streaming...")
            
            # Stream temperature data (faster rate)
            self.stream_temperature_data(temp_df, delay_ms=50)
            
            # Stream window data (slower rate)
            self.stream_window_data(window_df, delay_ms=200)
            
            # Keep streaming in a loop for continuous data
            logger.info("Starting continuous streaming loop...")
            loop_count = 0
            
            while True:
                loop_count += 1
                logger.info(f"Starting streaming loop {loop_count}")
                
                # Stream smaller batches continuously
                temp_batch = temp_df.sample(min(100, len(temp_df)))
                self.stream_temperature_data(temp_batch, delay_ms=100)
                
                window_batch = window_df.sample(min(20, len(window_df)))
                self.stream_window_data(window_batch, delay_ms=500)
                
                # Wait before next loop
                time.sleep(30)
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            logger.info("Producer shutdown complete")

def main():
    """Main entry point"""
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    
    producer = IoTDataProducer(kafka_servers)
    producer.run_producer()

if __name__ == "__main__":
    main()
# IoT Data Processing System

This project is an **end-to-end IoT Data Processing System** designed for real-time data ingestion, stream processing, storage, and visualization. The architecture is built using **Dockerized microservices** for scalability and reliability.

---

##  Key Features

- **IoT Device Simulation**  
  - Python Kafka Producer simulates temperature and window sensor data streams.

- **Stream Processing with Apache Spark**  
  - Detects anomalies in sensor data.  
  - Performs transformations in real-time.  
  - Outputs results to PostgreSQL.

- **Reliable Messaging with Kafka**  
  - Guarantees ordered, fault-tolerant delivery of IoT events.

- **Persistent Storage with PostgreSQL**  
  - Stores processed sensor data for downstream analysis.

- **Visualization with Grafana**  
  - Real-time dashboards for monitoring temperature trends, anomaly alerts, and window activity.

- **Containerized Architecture**  
  - All services run in isolated Docker containers.  
  - Orchestrated via Docker Compose.

---

## System Architecture

```
IoT Sensor Simulation (Python) -> Kafka -> Spark Structured Streaming -> PostgreSQL -> Grafana
```

**Components:**
- **Kafka**: Message broker for IoT data ingestion  
- **Spark**: Real-time processing engine  
- **PostgreSQL**: Relational database for structured data storage  
- **Grafana**: Visualization platform for monitoring and analytics  

---

## Setup & Installation

### Prerequisites
- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)  
- [Python 3.9+](https://www.python.org/) (for Kafka producer)  
- [Git](https://git-scm.com/)

### Steps to Run

1. **Clone the repository**
   ```bash
   git clone https://github.com/Sunilyadav7899/iot-data-processing-system.git
   cd iot-data-processing-system
   ```

2. **Start all services using Docker Compose**
   ```bash
   docker-compose up -d
   ```

3. **Run the IoT data producer**
   ```bash
   python producer/iot_producer.py
   ```

4. **Access the services**
   - Kafka: `localhost:9092`  
   - PostgreSQL: `localhost:5432`  
   - Grafana: [http://localhost:3000](http://localhost:3000) (default login: `admin/admin`)  

---

## Grafana Dashboard (Example)

The Grafana dashboard provides:
- Real-time **temperature monitoring**  
- **Anomaly detection alerts**  
- **Window activity tracking**  

You can customize queries and visualizations inside Grafana.

---


##  Scalability & Reliability

- **Scalability**: Microservices can scale independently.  
- **Reliability**: Retry logic in Kafka producer ensures no data loss.  
- **Reproducibility**: Entire pipeline runs locally via Docker.  

---

##  References

- [Apache Kafka](https://kafka.apache.org/)  
- [Apache Spark](https://spark.apache.org/)  
- [PostgreSQL](https://www.postgresql.org/)  
- [Grafana](https://grafana.com/)  

---

## Author

**Sunil Yadav**  
_Data Engineering Coursework Project_  

# Real-Time ETL Pipeline with Anomaly Detection and Power BI Integration

This project implements a **real-time ETL pipeline** to extract data from **Berkeley DB**, transform it with real-time processing and anomaly detection, and load the transformed data into **Power BI** dashboards for visualization. The pipeline also supports anomaly detection and alerting.

## Project Overview

The project leverages **Apache Kafka**, **Apache Flink**, and **Machine Learning models** to process streaming data, detect anomalies in real-time, and load the data into **Power BI** for visualization. This setup ensures that the data is available in real-time for monitoring purposes and allows for timely responses to detected anomalies.

## Technologies Used

- **Berkeley DB**: The source database for storing raw performance and traffic data.
- **Apache Kafka**: A distributed streaming platform for managing and processing real-time data streams.
- **Apache Flink**: A stream processing framework for transforming and analyzing real-time data.
- **Scikit-learn / TensorFlow / PyTorch**: For implementing and deploying anomaly detection algorithms.
- **Power BI**: For real-time data visualization and dashboarding.
- **Docker**: For containerizing the ETL pipeline and stream processing applications.
- **Kubernetes**: For orchestrating and managing the containerized pipeline at scale.
- **Apache Airflow**: For automating and orchestrating the ETL pipeline tasks.
- **Prometheus + Grafana**: For monitoring the pipeline performance and metrics.
- **AWS / Azure / Google Cloud**: For cloud-based storage and deployment.

## Project Structure

├── data_pipeline │ ├── extract │ ├── transform │ └── load ├── anomaly_detection │ ├── models │ └── scripts ├── powerbi_integration │ └── powerbi_rest_api ├── docker │ ├── Dockerfile │ └── docker-compose.yml ├── kubernetes │ ├── deployment.yaml │ └── service.yaml ├── airflow │ ├── dags │ └── tasks └── README.md


## Installation

### Prerequisites

- Docker and Docker Compose for containerization
- Python 3.x
- Kafka and Flink setup
- Power BI account with access to REST API
- Kubernetes setup (for scaling and deployment)

### Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/real-time-etl-pipeline.git
   cd real-time-etl-pipeline

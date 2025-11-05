🚴‍♂️ Real-Time Bike Sharing Analytics & Forecasting — Dubai

An end-to-end real-time data pipeline that collects Dubai’s public bike sharing data (GBFS standard) and live weather conditions. Data flows through Kafka, is processed via Spark Structured Streaming, aggregated into meaningful analytics, and then used to train a predictive model that forecasts station utilization one hour ahead.

🌐 System Overview
• GBFS API → public bike station info + status updates
• OpenWeather API → real-time weather conditions

📌 Technologies
| Layer                  | Tool                              |
| ---------------------- | --------------------------------- |
| Ingestion              | Python, Requests, dotenv          |
| Messaging              | Apache Kafka                      |
| Data Stream Processing | Apache Spark Structured Streaming |
| Storage                | Local CSV output (future: DB)     |
| ML Forecasting         | PySpark MLlib Random Forest       |
| Monitoring             | Logging + console batch prints    |

🧩 Pipeline Architecture

![Project Diagram](diagrams/project_diagramm.png)

📡 Streaming Analytics
- Joins real-time station status with static station metadata
- Computes station utilization: utilization = bikes_available / (bikes_available + docks_available)
- Aggregates every 30 minutes: avg, min, max, std deviation of utilization, weather conditions per time window
- Stores analytics to: csv/usage_summary.csv

🤖 Machine Learning Forecasting
Model: Random Forest Regressor
Goal: Predict next-hour average utilization

📌 Input Features
- Time-of-day, weekend flag
- Temperature, Wind, Clouds, Rain
- Utilization patterns with window shifts
📌 Output Target
- Bike utilization +1 hour later

🗂️ Project Structure

![Project Structure](diagrams/project_structure1.png)

▶️ How to Run
1. Start Kafka (two terminals)
cd ~/kafka_2.13-3.8.0
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

2. Start producers
python producers/producer_info.py
python producers/producer_status.py
python producers/weather_producer.py

3. Run Analytics Stream
python streaming/hourly_analytics_stream.py

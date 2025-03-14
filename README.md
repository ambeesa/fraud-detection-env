# Kafka Fraud Detection System

A real-time fraud detection system using Apache Kafka.

## Components

- **Producer**: Generates fake transaction data with occasional fraudulent transactions
- **Consumer**: Processes transactions and flags potentially fraudulent ones

## Technologies

- Python
- Apache Kafka
- Faker library for generating synthetic data

## Setup

1. Install requirements: `pip install kafka-python faker`
2. Start Zookeeper: `brew services start zookeeper`
3. Start Kafka: `brew services start kafka`
4. Run the consumer: `python backend/kafka_consumer.py`
5. Run the producer: `python backend/kafka_producer.py`
EOF

git add README.md
git commit -m "Add README"
git push

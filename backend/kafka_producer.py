from kafka import KafkaProducer #library for producing kafka
import json #using this to format messages
import random #gfor generating random transaction amts
import time # simulate real time streaming
from faker import Faker #generates fake transactions


#fake data generator
fake = Faker()

#kafka config
KAFKA_BROKER = "localhost:9092" #kafka is running locally on port 9092
TOPIC = "transactions" #kafka topic name

#create kafka producer to send messages
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER, #connets to kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf8') #converts python dict to JSON
)
#random transaction fucntion generator
def generate_transaction():
    return {
        "transaction_id": fake.uuid4(), #uniqeu transaction data
        "user_id": fake.random_int(min=1000, max=9999), #random user id
        "amount": round(random.uniform(1.0, 5000.0), 2), #random trnsaction amoount
        "merchant": fake.company(),  # Fake merchant name
        "location": fake.city(),  # Fake city name
        "timestamp": fake.iso8601(),  # Timestamp in ISO 8601 format
        "is_fraud": random.choice([True, False, False, False])  # 25% chance of fraud
    }
#stream fake transactiosn to kafka in real time
print(" Streaming transactions to Kafka.... ")
while True:
    transaction = generate_transaction() #generate a new transaction
    producer.send(TOPIC, value=transaction) #send transaction to Kafka topic
    print(f"Produced: {transaction}") #print for debugging
    time.sleep(2) #2 seconds before sending the next transaction
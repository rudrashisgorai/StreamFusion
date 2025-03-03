import json
import time
import requests
import logging
from kafka import KafkaProducer
from typing import Dict

# Constants
RANDOM_USER_API_URL = "https://randomuser.me/api/?results=1"
KAFKA_TOPIC = "random_names"
KAFKA_SERVERS = ['kafka1:19092', 'kafka2:19093', 'kafka3:19094']
STREAM_DURATION_SECONDS = 120  # total running time
STREAM_INTERVAL_SECONDS = 10   # interval between sends

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def fetch_random_user(api_url: str = RANDOM_USER_API_URL) -> Dict:
    """
    Fetches a random user from the API and returns the user data.
    
    Raises:
        HTTPError: If the API request fails.
    """
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    return data["results"][0]


def transform_user_data(user: Dict) -> Dict:
    """
    Transforms the raw user data from the API into a simplified dictionary for Kafka.
    
    Returns:
        A dictionary with selected and formatted user information.
    """
    try:
        full_name = f"{user['name']['title']}. {user['name']['first']} {user['name']['last']}"
        location = user['location']
        kafka_data = {
            "full_name": full_name,
            "gender": user["gender"],
            "location": f"{location['street']['number']}, {location['street']['name']}",
            "city": location['city'],
            "country": location['country'],
            "postcode": int(location['postcode']),
            "latitude": float(location['coordinates']['latitude']),
            "longitude": float(location['coordinates']['longitude']),
            "email": user["email"]
        }
    except (KeyError, ValueError, TypeError) as e:
        logging.error("Error transforming user data: %s", e)
        raise

    return kafka_data


def create_kafka_producer(servers: list = KAFKA_SERVERS) -> KafkaProducer:
    """
    Creates and returns a KafkaProducer instance connected to the provided servers.
    """
    return KafkaProducer(bootstrap_servers=servers)


def stream_random_user_data(duration: int = STREAM_DURATION_SECONDS, interval: int = STREAM_INTERVAL_SECONDS):
    """
    Streams random user data to a Kafka topic every `interval` seconds for a total of `duration` seconds.
    """
    producer = create_kafka_producer()
    end_time = time.time() + duration
    logging.info("Starting streaming data to Kafka topic '%s'", KAFKA_TOPIC)

    while time.time() < end_time:
        try:
            user_data = fetch_random_user()
            kafka_data = transform_user_data(user_data)
            # Send data to Kafka as a JSON-encoded byte string
            producer.send(KAFKA_TOPIC, json.dumps(kafka_data).encode('utf-8'))
            logging.info("Data sent to Kafka: %s", kafka_data)
        except Exception as e:
            logging.error("Failed to process or send data: %s", e)
        time.sleep(interval)

    logging.info("Finished streaming data.")


if __name__ == "__main__":
    stream_random_user_data()

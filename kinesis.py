import boto3
import json
import time
from loguru import logger


STREAM_NAME = 'car-stats'
REGION = 'us-east-1'
INPUT_FILE = 'datos.json'

kinesis = boto3.client('kinesis', region_name=REGION)


def load_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def run_producer():
    data = load_data(INPUT_FILE)
    events = data.get('events', [])
    records_sent = 0

    logger.info(f"Iniciando transmisión al stream: {STREAM_NAME}")

    for event in events:
        payload = {
            "vehicle_id": event["vehicle_id"],
            "speed": event["speed"],
            "fuel_level": event["fuel_level"],
            "rpm": event["rpm"],
            "timestamp": event["timestamp"]
        }

        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(payload),
            PartitionKey=event["vehicle_id"]  
        )

        records_sent += 1
        logger.info(
            f"Enviado {event['vehicle_id']} → "
            f"{event['speed']} km/h | fuel {event['fuel_level']}% "
            f"(Shard {response['ShardId']})"
        )

        time.sleep(0.1)

    logger.info(f"Fin de la transmisión. Total registros enviados: {records_sent}")


if __name__ == '__main__':
    run_producer()

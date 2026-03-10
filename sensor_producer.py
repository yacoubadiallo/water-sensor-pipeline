import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

locations = ["Paris-12", "Paris-15", "Lyon-3", "Marseille-8"]
pipelines = ["PIPE_001", "PIPE_002", "PIPE_003"]

while True:

    data = {
        "sensor_id": f"WATER_{random.randint(1000,9999)}",
        "timestamp": datetime.utcnow().isoformat(),
        "pressure": round(random.uniform(1.5,3.5),2),
        "flow_rate": round(random.uniform(10,50),2),
        "water_quality": round(random.uniform(0.7,1.0),2),
        "temperature": round(random.uniform(10,25),2),
        "location": random.choice(locations),
        "pipeline_id": random.choice(pipelines)
    }

    producer.send("water_sensor_raw", data)

    print("Sent:", data)

    time.sleep(2)
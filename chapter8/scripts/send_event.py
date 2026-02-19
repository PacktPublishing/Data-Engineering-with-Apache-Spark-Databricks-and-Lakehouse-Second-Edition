#!/usr/bin/env python3
import sys
from azure.eventhub import EventHubProducerClient, EventData

def main():
    if len(sys.argv) != 4:
        print("Usage: python send_event.py <CONNECTION_STRING> <EVENT_HUB_NAME> <JSON_FILE>")
        sys.exit(1)

    connection_string = sys.argv[1]
    event_hub_name = sys.argv[2]
    json_file = sys.argv[3]

    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=event_hub_name
    )

    sent_count = 0

    try:
        with producer:
            batch = producer.create_batch()

            with open(json_file, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if not line:
                        continue

                    event = EventData(line)

                    try:
                        batch.add(event)
                    except ValueError:
                        producer.send_batch(batch)
                        sent_count += len(batch)
                        batch = producer.create_batch()
                        batch.add(event)

            if len(batch) > 0:
                producer.send_batch(batch)
                sent_count += len(batch)

        print(f" {sent_count} events sent to Event Hub '{event_hub_name}'")

    except Exception as e:
        print(" Error occurred:", e)
        sys.exit(2)

if __name__ == "__main__":
    main()

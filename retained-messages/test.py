import paho.mqtt.client as mqtt
import threading
import time

# Config
BROKERS = [
    {"host": "nuc1", "port": 10000},
    {"host": "nuc1", "port": 10001},
    {"host": "nuc1", "port": 10002},
]
MQTT_QOS = 0
MAX_DEPTH = 10
VALUES_PER_LEVEL = 10
LOG_INTERVAL = 1000
MESSAGES_PER_SECOND = 1000  # Target messages per second

def generate_topic_paths():
    """Generator that yields all increasing-depth topic paths up to MAX_DEPTH with VALUES_PER_LEVEL."""
    def recurse(path=[], level=0):
        if level == MAX_DEPTH:
            return
        for i in range(VALUES_PER_LEVEL):
            new_path = path + [str(i)]
            yield new_path
            yield from recurse(new_path, level + 1)
    yield from recurse()

def publish_to_broker(broker_config, broker_index):
    client = mqtt.Client()
    client.connect(broker_config["host"], broker_config["port"], 60)
    client.loop_start()
    time.sleep(1)

    count = 0
    start_time = time.time()
    last_log_time = start_time  # Initialize time of the last log message

    if MESSAGES_PER_SECOND > 0:
        desired_interval = 1.0 / MESSAGES_PER_SECOND
    else:
        desired_interval = 0  # No rate limiting, publish as fast as possible

    for path_parts in generate_topic_paths():
        iteration_start_time = time.monotonic()

        topic = "/".join(["root"] + path_parts)
        payload = f"Payload for {topic}"
        info = client.publish(topic, payload, qos=MQTT_QOS, retain=True)
        info.wait_for_publish()

        count += 1

        # Log progress approximately every second
        current_time = time.time()
        if current_time - last_log_time >= 1.0:
            elapsed_total_time = current_time - start_time # Total time since publishing started
            if elapsed_total_time > 0:
                current_rate = count / elapsed_total_time
                print(f"[Broker {broker_index}] Published: {count} messages (Rate: {current_rate:.2f} msg/s)")
            else:
                print(f"[Broker {broker_index}] Published: {count} messages")
            last_log_time = current_time # Update the time of the last log

        if desired_interval > 0:
            time_taken_this_iteration = time.monotonic() - iteration_start_time
            sleep_duration = desired_interval - time_taken_this_iteration
            if sleep_duration > 0:
                time.sleep(sleep_duration)

    duration = time.time() - start_time
    print(f"[Broker {broker_index}] Done. Total published: {count} in {duration:.2f} seconds")
    client.loop_stop()
    client.disconnect()

def main():
    threads = []

    for i, broker in enumerate(BROKERS):
        t = threading.Thread(target=publish_to_broker, args=(broker, i + 1))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()

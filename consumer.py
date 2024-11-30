from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json
import uuid

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
my_name = "viktoriia"
topic_name = f'{my_name}_building_sensors'
topics_for_send = [f'{my_name}_temperature_alerts', f'{my_name}_humidity_alerts']

# Підписка на тему
consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
        if message.value["temperature"]>40:
            data = message.value
            data["message"] = "Warning! The temperature is over 40"
            producer.send(topics_for_send[0], key=str(uuid.uuid4()), value=data)
            print(f"Message sent to topic '{topics_for_send[0]}' successfully.")
        if message.value["humidity"]>80:
            data = message.value
            data["message"] = "Warning! Humidity above 80"
            producer.send(topics_for_send[1], key=str(uuid.uuid4()), value=data)
            print(f"Message sent to topic '{topics_for_send[1]}' successfully.")
        if message.value["humidity"]<20:
            data = message.value
            data["message"] = "Warning! Humidity is less than 20"
            producer.send(topics_for_send[1], key=str(uuid.uuid4()), value=data)
            print(f"Message sent to topic '{topics_for_send[1]}' successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer
producer.close()




#     try:
#         data = {
#             "sensor_id": sensor_id,
#             "timestamp": time.time(),  # Часова мітка
#             "temperature": random.randint(25, 45),
#             "humidity": random.randint(15, 85),  
#         }
#         producer.send(topic_name, key=str(uuid.uuid4()), value=data)
#         producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
#         print(f"Message {i} sent to topic '{topic_name}' successfully.")
#         time.sleep(2)
#     except Exception as e:
#         print(f"An error occurred: {e}")

# producer.close()  # Закриття producer


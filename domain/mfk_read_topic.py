from config import app_config_handler as app_config
from config import kafka_config_handler as kafka_config
from kafka import kafka_avro_consumer as abstract_avro_consumer
from model import mfk_row as model

consumer_serializers = {
    "key.deserializer": model.MfkAvroKey.get_deserializer(),
    "value.deserializer": model.MfkAvroValue.get_deserializer()
}

consumer_conf = kafka_config.get_consumer_config() | consumer_serializers

consumer = abstract_avro_consumer.KafkaAvroConsumer(consumer_conf)

topic = app_config.get_app_config()['msk.topic']

consumer.consume([topic])

import mfk_dataframe_handler as df_handler
from config import app_config_handler as app_config
from config import kafka_config_handler as kafka_config
from kafka import kafka_avro_producer as abstract_avro_producer
from model import mfk_row as model
import os

producer_serializers = {
    'key.serializer': model.MfkAvroKey.get_serializer(),
    'value.serializer': model.MfkAvroValue.get_serializer()
}

producer_conf = kafka_config.get_producer_config() | producer_serializers

script_dir = os.path.dirname(__file__)
rel_path = "../data/20210731_MFK_LISTE.csv"
csv_path = os.path.join(script_dir, rel_path)
df = df_handler.create_dataframe_from_path(
    csv_path, ";")

list_mfk_obj = df_handler.create_model_object(df)
topic = app_config.get_app_config()['msk.topic']
producer = abstract_avro_producer.KafkaAvroProducer(producer_conf)

for mfk in list_mfk_obj:
    key_object = model.MfkAvroKey(mfk.InstituteA, mfk.InstituteB)
    value_object = model.MfkAvroValue(mfk.InstituteA, mfk.PersonNumberA, mfk.InstituteB, mfk.PersonNumberB)
    producer.produce_record(topic=topic, key=key_object, value=value_object)

producer.flush()

print("{} messages were produced to topic {}!".format(abstract_avro_producer.delivered_records, topic))

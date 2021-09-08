import json
from uuid import uuid4

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from config import kafka_config_handler as kafka_config


class MfkRow:
    def __init__(self, institute_a, person_number_a, institute_b, person_number_b):
        self.InstituteA = institute_a
        self.PersonNumberA = person_number_a
        self.InstituteB = institute_b
        self.PersonNumberB = person_number_b


conf = kafka_config.get_schema_registry_config()

schema_registry_client = SchemaRegistryClient(conf)

mfk_key_schema = """
    {
        "namespace": "io.confluent.examples.clients.cloud",
        "name": "mfk_key",
        "type": "record",
        "fields": [
            {"name": "institut_a", "type": "string"},
            {"name": "institut_b", "type": "string"}
        ]
    }
"""


class MfkAvroKey(object):
    """
        Stores the deserialized Avro record for the Kafka key.
    """

    __slots__ = ["id", "institut_a", "institut_b"]

    def __init__(self, institut_a=None, institut_b=None):
        self.institut_a = institut_a
        self.institut_b = institut_b
        self.id = uuid4()

    @staticmethod
    def get_serializer():
        key_avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client,
                                             schema_str=mfk_key_schema,
                                             to_dict=MfkAvroKey.key_to_dict)
        return key_avro_serializer

    @staticmethod
    def get_deserializer():
        key_avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client,
                                                 schema_str=mfk_key_schema,
                                                 from_dict=MfkAvroKey.dict_to_name)
        return key_avro_deserializer

    @staticmethod
    def dict_to_name(obj, ctx):
        return MfkAvroKey(institut_a=obj['institut_a'], institut_b=obj['institut_b'])

    @staticmethod
    def key_to_dict(key, ctx):
        return MfkAvroKey.to_dict(key)

    def to_dict(self):
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        return dict(institut_a=self.institut_a, institut_b=self.institut_b)

    def to_json(self):
        return json.dumps(self, default=lambda o: o.to_dict(),
                          sort_keys=True, indent=4)


mfk_value_schema = """
    {
        "namespace": "io.confluent.examples.clients.cloud",
        "name": "mfk_value",
        "type": "record",
        "fields": [
            {"name": "institut_a", "type": "string"},
            {"name": "personen_nummer_a", "type": "int"},
            {"name": "institut_b", "type": "string"},
            {"name": "personen_nummer_b", "type": "int"}
        ]
    }
"""


class MfkAvroValue(object):
    """
        Stores the deserialized Avro record for the Kafka value.
    """

    __slots__ = ["id", "institut_a", "personen_nummer_a", "institut_b", "personen_nummer_b"]

    def __init__(self, institut_a=None, personen_nummer_a=None, institut_b=None, personen_nummer_b=None):
        self.institut_a = institut_a
        self.institut_b = institut_b
        self.personen_nummer_a = personen_nummer_a
        self.personen_nummer_b = personen_nummer_b
        self.id = uuid4()

    @staticmethod
    def get_serializer():
        value_avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client,
                                               schema_str=mfk_value_schema,
                                               to_dict=MfkAvroValue.value_to_dict)
        return value_avro_serializer

    @staticmethod
    def get_deserializer():
        value_avro_deserializer = AvroDeserializer(schema_str=mfk_value_schema,
                                                   schema_registry_client=schema_registry_client,
                                                   from_dict=MfkAvroValue.dict_to_name)
        return value_avro_deserializer

    @staticmethod
    def dict_to_name(obj, ctx):
        ret = MfkAvroValue(institut_a=obj['institut_a'], institut_b=obj['institut_b'],
                           personen_nummer_a=obj['personen_nummer_a'], personen_nummer_b=obj['personen_nummer_a'])
        return ret

    @staticmethod
    def value_to_dict(obj, ctx):
        ret = MfkAvroValue.to_dict(obj)
        return ret

    def to_dict(self):
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        return dict(institut_a=self.institut_a, personen_nummer_a=self.personen_nummer_a,
                    personen_nummer_b=self.personen_nummer_b, institut_b=self.institut_b)

    def to_json(self):
        return json.dumps(self, default=lambda o: o.to_dict(),
                          sort_keys=True, indent=4)

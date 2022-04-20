from confluent_kafka import avro
import os
from pathlib import Path
import application

path = os.path.abspath(__file__)
dir_path = os.path.dirname(path)

PROJ_LOC = application.config.PROJ_LOC

def load_avro_schema_from_file(schema_file):
    key_schema_string = """
    {"type": "string"}
    """

    key_schema = avro.loads(key_schema_string)
    value_schema = avro.load(os.path.join(PROJ_LOC, "avro", schema_file))

    return key_schema, value_schema
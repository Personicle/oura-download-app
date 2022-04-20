
import os
from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient
from azure.identity import DefaultAzureCredential
from azure.schemaregistry import SchemaRegistryClient
from azure.schemaregistry.serializer.avroserializer import AvroSerializer
import asyncio
import json
import logging

# from application.config import EVENTHUB_CONFIG
# from application.config import PROJ_LOC

LOG = logging.getLogger(__name__)

def send_records_to_eventhub(schema_file, records, eventhub_name):
    """
    Controller method called from data import modules of different applications
    parameters:
    schema_file: Schema file to be used from the avro schemas available
    records: list of dictionaries with the batch of records
    eventhub_name: Equivalent to kafka topic name
    """
    if type(records) is not list:
        LOG.error(f'send_records_to_eventhub receive records typed not list {str(type(records))}')
        assert type(records) is list, 'Records type is not list'

    LOG.info(f'send_records_to_eventhub called on {len(records)} records...')
    credential = DefaultAzureCredential()
    # Namespace should be similar to: '<your-eventhub-namespace>.servicebus.windows.net'

    fully_qualified_namespace = os.environ['SCHEMA_REGISTRY_FQNS']
    group_name = os.environ['SCHEMA_REGISTRY_GROUP']
    schema_registry_client = SchemaRegistryClient(fully_qualified_namespace, credential)
    serializer = AvroSerializer(client=schema_registry_client, group_name=group_name, auto_register_schemas=True)

    # define event hub producer client
    LOG.info("connection string: {}".format(os.environ['EVENTS_EVENTHUB_CONNECTION_STRING']))
    

    eventhub_producer = EventHubProducerClient.from_connection_string(
        conn_str=os.environ['EVENTS_EVENTHUB_CONNECTION_STRING'],
        eventhub_name= eventhub_name
    )
    
    
    # load schema string
    # with open(os.path.join(PROJ_LOC, "avro_modules", schema_file), "r") as schema_fp:
    #     schema_string = schema_fp.read()
    # print(schema_string)
    # call async producer with schema, producer and serializer
    # loop = asyncio.get_event_loop()
    LOG.info("sending {} to {}".format(records, eventhub_name))

    produce_records(records, eventhub_producer, serializer, None, credential)

    LOG.info("method send_records_to_eventhub finished sending records")


def produce_records(records, producer, serializer, schema_string, credentials):
    event_data_batch = producer.create_batch()

    # dict_data = {"name": "Bob", "favorite_number": 7, "favorite_color": "red"}
    # Use the serialize method to convert dict object to bytes with the given avro schema.
    # The serialize method would automatically register the schema into the Schema Registry Service and
    # schema would be cached locally for future usage.
    for record in records:
        # payload_bytes = serializer.serialize(value=record, schema=schema_string)
        # print('The bytes of serialized dict data is {}.'.format(payload_bytes))

    # passing json instead of bytes
        event_data = EventData(json.dumps(record)) #(body=payload_bytes)  # pass the bytes data to the body of an EventData
        event_data_batch.add(event_data)
    producer.send_batch(event_data_batch)
    if serializer is not None:
        serializer.close()
    producer.close()
    credentials.close()
    print('Send is done.')

if __name__ == "__main__":


    records = [ {"user_id": "test_user{}".format(i), "value": 150+i} for i in range(500)]

    schema_file_path = "/Users/vpandey/GithubRepos/external-connections-personicle/avro/test_schema.avsc"
    send_records_to_eventhub(schema_file_path, records, "test_hub")
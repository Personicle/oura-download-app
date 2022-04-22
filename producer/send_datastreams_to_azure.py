import asyncio
from azure.eventhub import EventHubProducerClient
from azure.eventhub import EventData
from datetime import datetime
import time
import json
import logging
import os

# from application.config import EVENTHUB_CONFIG
# from application.config import PROJ_LOC

LOG = logging.getLogger(__name__)
                                                                                                        
def datastream_producer(record_message):
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str=os.environ["DATASTREAM_EVENTHUB_CONNECTION_STRING"], 
                    eventhub_name=os.environ["DATASTREAM_EVENTHUB_NAME"])
  
    # Create a batch.
    event_data_batch = producer.create_batch()

    # Add records to the batch.
    event_data_batch.add(EventData(json.dumps(record_message)))
        
    # Send the batch of events to the event hub.
    producer.send_batch(event_data_batch)
    producer.close()
    LOG.info("Sending data records is done")

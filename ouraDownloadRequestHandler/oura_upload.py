import json
import os
import traceback

from producer import send_records_azure,send_datastreams_to_azure
from .utils.oura_parsers import *
from logging import getLogger

LOG = getLogger(__name__)

RECORD_PROCESSING = {
    'activity_sleep': oura_activity_parser_sleep,
    # 'activity_readiness': oura_activity_parser_readiness,
    'datastream': oura_datastream_parser,
#     'datastream_heartrate': oura_datastream_parser_heartrate,
#     'datastream_readiness': oura_datastream_parser_readiness,
#     'datastream_daily_activity': oura_datastream_parser_daily_activity
}


SCHEMA_LOC = './avro'
SCHEMA_MAPPING = {
    'heartrate': 'fitbit_stream_schema.avsc',
    'activity': 'event_schema.avsc',
    'sleep': 'event_schema.avsc'
    }

TOPIC_MAPPING = {
    'heartrate': 'google_stream_heartrate',
    'activity': 'testhub-new',
    'sleep': 'testhub-new'
}
def send_datastream_to_personicle(personicle_user_id, records, stream_name, data_type, events_topic, limit = None):
    count = 0
    record_formatter = RECORD_PROCESSING[f"{stream_name}"]
    formatted_records = record_formatter(records, personicle_user_id,data_type)
    try:
        send_datastreams_to_azure.datastream_producer(formatted_records)

        # print(formatted_records)

        # return {"success": True, "number_of_records": count}
    except Exception as e:
        LOG.error(traceback.format_exc())
        return {"success": False, "error": e}

def send_records_to_personicle(personicle_user_id, records, stream_name, event_name, events_topic, limit = None):
    count = 0
    record_formatter = RECORD_PROCESSING[f"{stream_name}_{event_name}"]
    
    formatted_records = []
    for record in records:
        # print(record)
        formatted_record = record_formatter(record, personicle_user_id,event_name)
        # send formatted record to event hub
        # events_topic.add(json.dumps(formatted_record))
        formatted_records.append(formatted_record)
        # print(formatted_record) 
        count += 1        

        if limit is not None and count <= limit:
            break
    # send data packet to data upload api instead of event hub
    # request_headers = {"accept": "application/json",
    #         "authorization": "Bearer {}".format(personicle_bearer_token)}
    # request_params = {}
    # request_data = {}
    # request_endpoint = os.environ.get("EVENT_WRITE_ENDPOINT")

    # response = requests.post(request_endpoint, headers=request_headers, data=formatted_records)
    # events_topic.set(json.dumps(formatted_records))
    try:
        send_records_azure.send_records_to_eventhub(None, formatted_records, os.environ['EVENTS_EVENTHUB_NAME'])
        # send_records_azure.send_records_to_eventhub(None, formatted_records, "testhub-new")


        # print(formatted_records)

        return {"success": True, "number_of_records": count}
    except Exception as e:
        LOG.error(traceback.format_exc())
        return {"success": False, "error": e}

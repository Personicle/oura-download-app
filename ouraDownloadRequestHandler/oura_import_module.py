import requests
from datetime import datetime, timedelta
import logging
from . import oura_upload
import os
LOG = logging.getLogger(__name__)

oura_api_url_mapping = {
    "sleep": os.environ['OURA_API_SLEEP'],
    "readiness": os.environ['OURA_API_READINESS'],
    "heartrate": os.environ['OURA_API_HEARTRATE']
}

def oura_activity_import(personicle_user_id, access_token, last_accessed_at, events_topic,event_name):
    if last_accessed_at is None:
        start_time = datetime.now().date() - timedelta(days=365)
        end_time = datetime.now().date()
    else:
        start_time = datetime.strptime(last_accessed_at, "%Y-%m-%d %H:%M:%S.%f")
        start_time = start_time.date()
        
        end_time = datetime.now().date()
    
    repeat_token = None
    call_api = True
    request_status = False
    count_sessions = 0
    while call_api:
        query_parameters = {}
        if start_time:
            query_parameters['start'] = start_time.strftime("%Y-%m-%d")
        if end_time:
            query_parameters['end'] = end_time.strftime("%Y-%m-%d")
        if repeat_token is not None:
            query_parameters['next_token'] = repeat_token
        
        query_header = {
            "Authorization": "Bearer {}".format(access_token)
        }

        LOG.info("Requesting oura data for user {} from {} to {}".format(personicle_user_id, start_time, end_time))
        
        data_response = requests.get(oura_api_url_mapping[event_name], headers=query_header, params=query_parameters)
        
        # user did not grant access to this scope
        if data_response.status_code == 403:
            request_status = False
            return request_status, 0

        res = data_response.json()[event_name]
       
        if len(res) > 0:
            request_status=True
            print(len(res))
            send_response = oura_upload.send_records_to_personicle(personicle_user_id, res, 'activity',event_name, events_topic)
            LOG.info(send_response)
        repeat_token = data_response.json().get('next_token',None)
        if repeat_token is None:
            call_api = False
        count_sessions += len(res)

    return request_status, count_sessions

def oura_activity_imports(personicle_user_id, access_token, last_accessed_at, events_topic):
    sleep_status, sleep_sessions = oura_activity_import(personicle_user_id, access_token, last_accessed_at, events_topic,"sleep")
    # # readiness_status, readiness_sessions = oura_activity_import(personicle_user_id, access_token, last_accessed_at, events_topic,"readiness")
  
    return sleep_status, sleep_sessions
    
def oura_datastream_import(personicle_user_id, access_token, last_accessed_at, events_topic, data_type):
    if last_accessed_at is None:
        start_time = datetime.now().date() - timedelta(days=365)
        end_time = datetime.now().date()
    else:
        start_time = datetime.strptime(last_accessed_at, "%Y-%m-%d %H:%M:%S.%f")
        start_time = start_time.date()
        end_time = datetime.now().date()

    repeat_token = None
    call_api = True
    request_status = False
    count_datapoints = 0

    while call_api:
        query_parameters = {}
        if start_time:
            query_parameters['start'] = start_time.strftime("%Y-%m-%d")
        if end_time:
            query_parameters['end'] = end_time.strftime("%Y-%m-%d")
        if repeat_token is not None:
            query_parameters['next_token'] = repeat_token
        
        query_header = {
            "Authorization": "Bearer {}".format(access_token)
        }

        LOG.info("Requesting oura datastream for user {} from {} to {}".format(personicle_user_id, start_time, end_time))
        
        data_response = requests.get(oura_api_url_mapping[data_type], headers=query_header, params=query_parameters)
        
        # user did not grant access to this scope
        if data_response.status_code == 403:
            request_status = False
            
            return request_status, 0

        res = data_response.json()['data']
       
        if len(res) > 0:
            request_status=True
            print(len(res))
            send_response = oura_upload.send_datastream_to_personicle(personicle_user_id, res, 'datastream',data_type, events_topic)
            LOG.info(send_response)
        repeat_token = data_response.json().get('next_token',None)
        if repeat_token is None:
            call_api = False
        count_datapoints += len(res)

    return request_status, count_datapoints

def oura_datastreams_imports(personicle_user_id, access_token, last_accessed_at, events_topic):
    heartrate_status, hearate_datapoints = oura_datastream_import(personicle_user_id, access_token, last_accessed_at, events_topic,"heartrate")

    return heartrate_status, hearate_datapoints
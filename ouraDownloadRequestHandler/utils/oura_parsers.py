from datetime import datetime
# import pytz
import json
import logging

LOG = logging.getLogger(__name__)


def oura_activity_parser_sleep(raw_event, personicle_user_id,event_name):
    """
    Format a sleep activity received from oura API to personicle event schema
    """
    new_event_record = {}
    new_event_record['individual_id'] = personicle_user_id
    # timestamp format 2021-11-25T09:27:30.000-08:00
    # print(raw_event)
    new_event_record['start_time'] = raw_event['bedtime_start']
    duration = raw_event['duration']
    new_event_record['end_time'] = raw_event['bedtime_end']

    new_event_record['event_name'] = event_name
    new_event_record['source'] = 'oura'
    new_event_record['parameters'] = json.dumps({
        "duration": duration,
        "source_device": "oura",
        "period_id": raw_event['period_id'],
        "timezone": raw_event['timezone'],
        "breath_average": raw_event['breath_average'],
        "total": raw_event['total'],
        "awake": raw_event['awake'],
        "rem": raw_event['rem'],
        "deep": raw_event['deep'],
        "light": raw_event['light'],
        "midpoint_time": raw_event['midpoint_time'],
        "efficiency": raw_event['efficiency'],
        "restless": raw_event['restless'],
        "onset_latency": raw_event['onset_latency'],
        "hr_5min": raw_event['hr_5min'],
        "hr_average": raw_event['hr_average'],
        "hr_lowest": raw_event['hr_lowest'],
        "hypnogram_5min": raw_event['hypnogram_5min'],
        "rmssd": raw_event['rmssd'],
        "rmssd_5min": raw_event['rmssd_5min'],
        "score": raw_event['score'],
        "score_alignment": raw_event['score_alignment'],
        "score_deep": raw_event['score_deep'],
        "score_disturbances": raw_event['score_disturbances'],
        "score_efficiency": raw_event['score_efficiency'],
        "score_latency": raw_event['score_latency'],
        "score_rem": raw_event['score_rem'],
        "score_total": raw_event['score_total'],
        "temperature_deviation": raw_event['temperature_deviation'],
        "temperature_trend_deviation": raw_event['temperature_trend_deviation'],
        "bedtime_start_delta": raw_event['bedtime_start_delta'],
        "bedtime_end_delta": raw_event['bedtime_end_delta'],
        "midpoint_at_delta": raw_event['midpoint_at_delta'],
        "temperature_delta": raw_event['temperature_delta'],

    })
    return new_event_record

def oura_datastream_parser_heartrate(raw_event, personicle_user_id,event_name):
    # todo
    pass
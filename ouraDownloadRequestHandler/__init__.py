import logging
import json
import datetime
import azure.functions as func
from typing import List
import traceback
from .oura_import_module import oura_activity_imports,oura_datastreams_imports
from sqlalchemy.ext.declarative import declarative_base
from .db_connection import database,users


async def main(msg: func.QueueMessage, eventsTopic: func.Out[List[str]]) -> None:
    request_message = json.loads(msg.get_body().decode('utf-8'))
    logging.basicConfig(level=logging.WARNING, str='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))

    try:
        required_args = ["individual_id", "service_name", "service_access_token", "last_accessed_at"]
        assert all([x in request_message for x in required_args]), "missing parameter in the request {}".format(json.dumps(required_args))
        
        sleep_status, sleep_sessions = oura_activity_imports(request_message["individual_id"], 
                request_message["service_access_token"], request_message['last_accessed_at'], eventsTopic)
        logging.info("Processed event request")
        logging.info(str(sleep_sessions))
        # logging.info(str(readiness_sessions))
        logging.info(sleep_status)
        # logging.info(readiness_status)
        datastream_status, datastream_response = oura_datastreams_imports(request_message["individual_id"], request_message["service_access_token"], request_message['last_accessed_at'],eventsTopic)
        logging.info(datastream_status)
        if sleep_status or datastream_status:
            await database.connect()
            update_query = users.update().where((users.c.userId == request_message['individual_id']) & (users.c.service == "oura")).values(last_accessed_at = datetime.datetime.now())
            await database.execute(update_query)
            await database.disconnect()
       
    
    except AssertionError as e:
        logging.error("Missing parameter in data download request")
        logging.error(e)
        
    except Exception as e:
        logging.error(e)
        logging.error(traceback.format_exc())
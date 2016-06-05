from pymongo import MongoClient
from retrying import retry
import requests
import json
import time
import copy
import logging

# Create database objects
client = MongoClient()
db = client.test
# Create configuration dictionary
with open('config.json') as data_file:
    config = json.load(data_file)

# Set logging
logging.basicConfig(filename='sender.log', level=logging.DEBUG)

# Set the request parameters
url = "https://{instance}/api/now/table/em_event".format(instance=config['instance'])
# Eg. User name="admin", Password="admin" for this code sample.
user = config['username']
pwd = config['password']
# Set proper headers
headers = {"Content-Type": "application/json", "Accept": "application/json"}


# Retry sending the event with an exponential backoff
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def send_event_to_sn(json_doc):
    response = requests.post(url, auth=(user, pwd), headers=headers, data=json.dumps(json_doc))
    if response.status_code != 201:
        raise IOError("Error. Received status code: {status}".format(status=response.status_code))

    return response.status_code

# Enter event loop
while True:
    # Iterate through all documents in the collection and find the unsent ones (unsent=0)
    for document in db.events.find({'em_event_status': 0}):
        # Create a deepcopy of the original doc to send back with save(). Save does an upsert if the _id is present
        original_document = copy.deepcopy(document)
        logging.info("Found document id: {doc_id}".format(doc_id=original_document['_id']))

        # Delete unnecessary keys before sending to SN
        del document['em_event_status']
        del document['_id']
        # Send event
        sent_event = send_event_to_sn(document)
        logging.info("REST status code: {rest_status}".format(rest_status=sent_event))
        # Check for successful status
        if sent_event == 200 or 201:
            # Change event status for upsert
            original_document['em_event_status'] = 1
            logging.info("Saving sent document: {doc_to_send}".format(doc_to_send=original_document))
            # Save the sent event
            db.events.save(original_document)
    # Sleep time for loop
    time.sleep(config['retry_value'])

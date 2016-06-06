from flask import Flask, request, jsonify
from pymongo import MongoClient
import json

app = Flask(__name__)
client = MongoClient()
db = client.test
api_version = 'v1'


@app.route('/api/{api_version}/event'.format(api_version=api_version), methods=['POST'])
def event_gateway():
    req_dict = request.get_json()
    # em_event_status will be an indexed field that workers use to send data to ServiceNow
    # 0 = unsent, 1 = sent
    req_dict['em_event_status'] = 0
    try:
        # Insert document into MongoDB
        result = db.events.insert_one(
            req_dict
        )

        return jsonify(status='success', id=str(result.inserted_id))

    except Exception as e:
        return str(e)


@app.route('/', methods=['GET'])
def index():
    return jsonify(status='healthy')


@app.route('/api/{api_version}/event'.format(api_version=api_version), methods=['GET'])
def event_gateway_status():
    events = {'events_to_be_processed': [], 'events_processed': []}
    for document in db.events.find({'em_event_status': 0}):
        del document['em_event_status']
        del document['_id']
        events['events_to_be_processed'].append(document)

    for document in db.events.find({'em_event_status': 1}):
        del document['em_event_status']
        del document['_id']
        events['events_processed'].append(document)

    return jsonify(events)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

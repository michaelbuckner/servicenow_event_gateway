from flask import Flask, request, jsonify
from pymongo import MongoClient

app = Flask(__name__)
client = MongoClient()
db = client.test


@app.route('/api/v1/event', methods=['POST'])
def hello_world():
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

import os
import re
import csv
import time
import traceback
import json
from concurrent import futures

from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()



def check_for_topic(project_id, topic_id):
    project_path = f"projects/{project_id}"
    existing_topics = [topic.name for topic in publisher.api.list_topics(request={"project": project_path})]
    #existing_topics = [topic.name for topic in publisher.api.list_topics(project_path)]
    print(existing_topics)
    if any([re.search(topic_id, topic) for topic in existing_topics]):
        return True
    else:
        return False

def create_topic(project_id, topic_id):
    topic_path = publisher.api.topic_path(project_id, topic_id)
    if not check_for_topic(project_id, topic_id):
        topic = publisher.api.create_topic(request={"name": topic_path})
        print(f"Created topic: {topic.name}")
    else:
        print(f"{topic_id} already exists")
    return topic_path


def get_callback(publish_future, data):
    def callback(publish_future):
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback


if __name__ == "__main__":
    #check_for_topic('bbsm-dev', 'student_information_topic')
    publish_futures = []
    try:
        #topic_path = create_topic('bbsm-dev','student_information_topic')
        topic_path = create_topic('bbsm-dev', 'dataflow_test_topic')
        # PROJECT_ID = "bbsm-dev"
        # TOPIC_ID = "dataflow_test_topic_1"
        # topic = f"projects/{PROJECT_ID}/topics/{TOPIC_ID}"
        with open('gaming_data.csv', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data = json.dumps(row)
                time.sleep(2)
                # When you publish a message, the client returns a future.
                publish_future = publisher.publish(topic_path, data.encode("utf-8"))
                # Non-blocking. Publish failures are handled in the callback function
                publish_future.add_done_callback(get_callback(publish_future, data))
                publish_futures.append(publish_future)

        # Wait for all the publish futures to resolve before exiting.
        futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    except Exception as e:
        traceback.print_exc()



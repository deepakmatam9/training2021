import os
import re
import csv
import time
import traceback
import json
from concurrent import futures
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

def check_for_subscription(project_id, subscription_id):
    project_path = f"projects/{project_id}"
    existing_subscriptions = [subscription.name for subscription in subscriber.api.list_subscriptions(request={"project": project_path})]
    #existing_subscriptions = [subscription.name for subscription in subscriber.api.list_subscriptions(project_path)]
    if any([re.search(subscription_id, subscription) for subscription in existing_subscriptions]):
        return True
    else:
        return False

def create_subscrption(project_id, topic_id, subscription_id):
    topic_path = publisher.api.topic_path(project_id, topic_id)
    subscription_path = subscriber.api.subscription_path(project_id, subscription_id)
    if not check_for_subscription(project_id, subscription_id):
        subscription  = subscriber.api.create_subscription(request={"name": subscription_path, "topic": topic_path})
        print(f"Created subscription: {subscription}")
    else:
        print(f"{subscription_id} already exists")
    return subscription_path

def callback_fn(message):
    print(f"Received {message}.")
    message.ack()


if __name__ == "__main__":
    publish_futures = []
    try:
        subscription_path = create_subscrption('bbsm-dev','dataflow_test_topic','dataflow_subscriber')
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback_fn)
        print(f"Listening for messages on {subscription_path}..\n")
        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                streaming_pull_future.result(timeout=300)
            except TimeoutError:
                streaming_pull_future.cancel()  # Trigger the shutdown.
                streaming_pull_future.result()  # Block until the shutdown is complete.

    except Exception as e:
        traceback.print_exc()
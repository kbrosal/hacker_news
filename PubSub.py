from google.cloud import pubsub_v1
from google.oauth2 import service_account
import os
import time
from dotenv import load_dotenv

load_dotenv()

os.environ['GOOGLE_CLOUD_PROJECT'] = 'data-engineering-433013'

service_account_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

credentials = service_account.Credentials.from_service_account_file(service_account_path)

publisher = pubsub_v1.PublisherClient(credentials=credentials)


project_id = 'data-engineering-433013'
topic_id = 'hacker_news_topic'
topic_name = publisher.topic_path(project_id, topic_id)

# Attempt to create the topic
try:
    publisher.create_topic(request={"name": topic_name})
    print(f"Topic created: {topic_name}")
except Exception as e:
    print(f"Topic already exists or another error occurred: {e}")

# Open and read the CSV file
with open('hacker_news.csv', 'r') as f_in:
    for line in f_in:
        data = line.encode('utf-8')
        # Publish each line to the topic
        future = publisher.publish(topic_name, data=data)
        print(future.result())
        time.sleep(1)

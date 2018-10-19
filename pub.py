# Imports the Google Cloud client library
import time
from google.cloud import pubsub_v1
proj = "dataeng-219316"
topic_name="streamingtopic"
subscription_name="sub1"

def setup_publisher(project, subscription_name):
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(
            project, subscription_name)
  topic = publisher.create_topic(topic_path)
  print('Topic created: {}'.format(topic_name))
  return publisher   

def setup_subscriber(project, topic_name, subscription_name):
  print('setup subscriber')
  subscriber = pubsub_v1.SubscriberClient()
  topic_path = subscriber.topic_path(project, topic_name)
  subscription_path = subscriber.subscription_path(
        project, subscription_name)
  subscription = subscriber.create_subscription(
            subscription_path, topic_path)
  print('Subscription created: {}'.format(subscription))
  def callback(message):
    print('Received message: {}'.format(message))
    message.ack()

  subscriber.subscribe(subscription_path, callback=callback)
  print("Subscription done")
    
def publish_messages(publisher, topic_path):

    for j in range(1,500):  
      for i in range(1,100):
        data =  u'Message is {}'.format(i)
        data = data.encode('utf-8')
        publisher.publish(topic_path, data=data)
      time.sleep(60)


def main():
  print('main program')
  pub = setup_publisher(proj, topic_name)
  topic_path = pub.topic_path(
            proj, topic_name)

  # Create subscriber
  ############## Setup subscriber i.e subsriber and a subscription  ##################
  setup_subscriber(proj, topic_name, subscription_name)

  print('Subscriber  created: {}'.format(topic_name))
  #by default the batch is 0.05 seconds
  ############## Publish messages  ##################
  publish_messages(pub, topic_path)

  ############## Delete topic ##################
  pub.delete_topic(topic_path)






############### Setup pubsub topic ##################
# Instantiates a client
if __name__ == "__main__":
    main()

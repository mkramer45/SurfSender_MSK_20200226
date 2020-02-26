import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from lxml import html
import logging


# logging.basicConfig(filename='surfsend_info.log', level=logging.INFO,
#                     format='%(levelname)s:%(message)s')


# Function for sending our producer's message to the Kafka Topic
def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        # print('Message published successfully.')
        print("""Sending Message: '"""+ value + """' To Topic: """ + """'""" + topic_name + """'""")
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


# Function for establishing the connection between Kafka Producer & Kafka Server
def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


# Function for retrieving Surf Data for some New England Beaches.
# For now, we are simply gathering Beach Name + Current Wave Height
def surf_info_fetch():

    surf_url_list = [
    'https://magicseaweed.com/Nahant-Surf-Report/1091/',
    'https://magicseaweed.com/Nantasket-Beach-Surf-Report/371/',
    'https://magicseaweed.com/Scituate-Surf-Report/372/',
    'https://magicseaweed.com/Seabrook-Beach-Surf-Report/2078/',
    'https://magicseaweed.com/Rye-Rocks-Surf-Report/368/',
    'https://magicseaweed.com/Salisbury-Surf-Report/1130/',
    ]

    surf_list = []
    
    for url in surf_url_list:

        # HTML Parsing
        page = requests.get(url)
        tree = html.fromstring(page.content)

        # Wave Height
        try:
            wave_height_raw = tree.xpath('//*[@class="rating-text text-dark"]/text()')
            wave_height = str.strip((wave_height_raw[0]) + " ft")
        except Exception:
            wave_height = "No Data Available"

        # Beach Name
        try:
            beach_name_raw = url.split('.com/')[1]
            beach_name = beach_name_raw.split('-')[0]
        except Exception:
            beach_name = "No Data Available"

        # This will be our message sent to the topic
        current_conditions = beach_name + " Wave Height: " + wave_height

        # List of messages, one for each URL
        surf_list.append(current_conditions)

    return surf_list

# Below is where our producer script comes together.
# The messages generated from the function surf_info_fetch() are published to our topic.
if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }

    surf_forecasts = surf_info_fetch()
    if len(surf_forecasts) > 0:
        kafka_producer = connect_kafka_producer()
        for i in surf_forecasts:
            publish_message(kafka_producer, 'surf_data_topic41', 'raw', i.strip())
        if kafka_producer is not None:
            kafka_producer.close()

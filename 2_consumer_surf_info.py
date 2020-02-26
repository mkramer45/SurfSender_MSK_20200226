import json
from time import sleep
import psycopg2

from kafka import KafkaConsumer

# This script servers as our consumer to the producer (producer_surfinfo.py)
# Once executed, this script will take the messages living in the topic & send them to an Aiven PostgreSQL database.
if __name__ == '__main__':
	parsed_topic_name = 'surf_data_topic41'

	consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
							 bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
	for msg in consumer:
		message_raw = msg.value.decode("utf-8")
		
		if 'flat' in message_raw:
			message_raw = message_raw.replace("ft", "")

		conn = psycopg2.connect("dbname=surf_db user=admin3 password=admin323 host=localhost")
		cur = conn.cursor()
		cur.execute("INSERT INTO surf_conditions (wave_info) VALUES (%s)", (message_raw,))
		conn.commit()
		cur.close()
		conn.close()

		print("""Adding Message to Database: '""" + message_raw + """'""")

	if consumer is not None:
		consumer.close()


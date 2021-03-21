from kafka import KafkaProducer
import json
from json import dumps
import uuid
from time import sleep
####### Kafka Variables #######
# Set message to send topic here
simple_messages = [
'I love this pony',
'This restaurant is great',
'The weather is bad today',
'I will go to the beach this weekend',
'I dont know really',
'I hate you'
]


####### Kafka producer #######

producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])

def kafka_producer():

	for data in simple_messages:
		data = {'data':data}
		producer.send('test', value=json.dumps(data).encode('utf-8'))
		producer.flush()
		sleep(0.5)
	print("Messages are sent")

####### Inıtialize producer #######
kafka_producer()

from kafka import KafkaConsumer, TopicPartition
import json
from senti import scoreText
import sys
from datetime import datetime
from elasticsearch import Elasticsearch

# command line parameter
topic = str(sys.argv[1])
partition_n = str(sys.argv[2])

print "Topic: " + topic
print "Partition: " + partition_n

# To consume latest messages and auto-commit offsets
partition = TopicPartition(str(topic), int(partition_n))
consumer = KafkaConsumer(bootstrap_servers=['<IP>:9092'], value_deserializer=lambda m: json.loads(m.decode('ascii')))
consumer.assign([partition])

es = Elasticsearch(['<IP>:9200'])

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
   text = message.value['text']
    timestamp = message.value['timestamp']
    hashtag = message.value['hashtag']

    score = scoreText(text)

    doc = {
       'text': text,
       'score': score,
       'date': timestamp,
       'hashtag': hashtag
    }

    res = es.index(index="demo-tuiter", doc_type='tuiter', body=doc)

    print ("t=%s:p=%d:o=%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          text))

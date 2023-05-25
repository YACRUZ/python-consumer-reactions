# Import some necessary modules
# pip install kafka-python
# pip install pymongo
# pip install "pymongo[srv]"
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json
import subprocess



# replace here with your mongodb url 
uri = "mongodb+srv://yamatsukino:tMDo82LbaOFq3adX@kafka-db.c4cafoj.mongodb.net/?retryWrites=true&w=majority"


# Create a new client and connect to the server
#client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection

#try:
#    client.admin.command('ping')
#    print("Pinged your deployment. You successfully connected to MongoDB!")
#except Exception as e:
#    print(e)

# Connect to MongoDB and pizza_data database

try:
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.animes
    print("MongoDB Connected successfully!")
except:
    print("Could not connect to MongoDB")

consumer = KafkaConsumer('test',bootstrap_servers=['my-kafka-0.my-kafka-headless.okteto-yacruz.svc.cluster.local:9092'])
# Parse received data from Kafka
try:
    agg_result =db.memes_info.agregate(
        [{
            "$group" :
            {
                "_id": "$name",
                "n"  : {"$sum" : 1}
            }
        }])
    db.memes_summary.delete_many({})
    for i in agg_result:
        print (i)
        summary_id = db.memes_suammry.insert_one(i)
        print("Sumary inserted with record ids", summary_id)
except Exception as e:
    print(f'group by caunght {type(e)}: ')
    print(e)
    
for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    name = record['name']

    # Create dictionary and ingest data into MongoDB
    try:
       meme_rec = {'name':name }
       print (meme_rec)
       meme_id = db.memes_info.insert_one(meme_rec)
       print("Data inserted with record ids", meme_id)

       #subprocess.call(['sh', './test.sh'])

    except:
       print("Could not insert into MongoDB")
    
    
   

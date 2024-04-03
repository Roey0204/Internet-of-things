'''
Author: Roey
Description : This objective of create this module is to compile the library to improve the flexibility of Mongodb in python use case for my "Internet of things" data logger.

'''

from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from dotenv import load_dotenv,find_dotenv
import os
import time

class MyMongo: 
    
    # Init
    def __init__(self):       
        self.load       = load_dotenv(find_dotenv())
        self.password   = os.environ.get("MONGDB_PWD")
        self.username   = os.environ.get("USER_NAME")
        self.url        = f"mongodb+srv://{self.username}:{self.password}@roeycluster.4hav9rx.mongodb.net/?retryWrites=true&w=majority&appName=RoeyCluster"
        self.data       = None
        self.convertData= None
        
    # Connect to MongoClient
    def connect(self,collection:str):
        self.client     = MongoClient(self.url)
        self.db         = self.client[os.environ.get("DB_NAME")]
        self.collection = self.db[collection]
        
    # Convert list to MongdoDb format(Dict)
    def getDocument(self,receive:list):
        received_meta      = receive[0]
        received_value     = receive[1] 
        
        self.convertData = {
        "timestamp"     :datetime.utcnow(),
        "metadata":{
            "sensorId"  :received_meta[0],
            "type"      :received_meta[1],
            "unit"      :received_meta[2],
            },
        "value"         :received_value
        }
        
        return self.convertData
      
    # Create Document    
    def create_document(self,receive:list):
        received_meta      = receive[0]
        received_value     = receive[1] 
        
        self.data = {
        "timestamp"     :datetime.utcnow(),
        "metadata":{
            "sensorId"  :received_meta[0],
            "type"      :received_meta[1],
            "unit"      :received_meta[2],
            },
        "value"         :received_value
        }
        result = self.collection.insert_one(self.data)
        return result.inserted_id      
    
    # Read operation by id
    def read_document_id(self,document_id:int):
        document = self.collection.find_one({"_id": ObjectId(str(document_id))})
        return document
    
    # Update operation by id
    def update_document_id(self,document_id, new_data):
        result = self.collection.update_one({"_id": ObjectId(document_id)}, {"$set": new_data})
        return result
    
    # Update operation by sensor id and type
    def update_document(self, sensor_id, type, new_data):
        filter = {
        "metadata.sensorId": sensor_id,
        "metadata.type": type
        }
        update_operation = {
            "$set": new_data
        }
        result = self.collection.update_many(filter, update_operation)
        return result

    # Delete operation only one via object id
    def delete_document(self,document_id:int):
        result = self.collection.delete_one({"_id": ObjectId(document_id)})
        return result.deleted_count

    # Insert many documents 
    def insert_many_documents(self,data:list):
        result = self.collection.insert_many(data)
        return result.inserted_ids

    # Delete many documents
    def delete_many_documents(self,filter:dict):
        result = self.collection.delete_many(filter)
        return result.deleted_count

    # Query documents with the specified sensorId and type
    def track_reading(self,sensor_id:int,type:str):
        filter = {
            "metadata.sensorId": sensor_id,
            "metadata.type":type            
            }
        documents = self.collection.find(filter)
        readings = [(doc["timestamp"], doc["metadata"]["type"],doc["value"],doc["metadata"]["unit"]) for doc in documents]
        return readings
    

if __name__ == "__main__":
    
    # Init
    test = MyMongo()

    # Connect to collection and db
    test.connect("data")

    # Meta Data
    sensor_id    = 1
    sensor_type  = "temperature"
    sensor_unit  = "celcius"
    
    # Value
    for i in range(1,100,5):
        sensor_value = i
        
        # Compile data
        sensor_info  = [sensor_id,sensor_type,sensor_unit]
    
        # Create Document
        doc_list = [sensor_info,sensor_value]
        time.sleep(0.5)
        document =  test.create_document(doc_list)
    
        
    # Read Document
    # doc = test.getDocument(doc_list)

    # # Update document
    # new_data = test.getDocument(doc_list)

    # document = test.update_document(3,"pressure",new_data)
    
    # Delete many Document
    # delete_filter = {"value": {"$lt": 1200}} # delete doc that less than 1200
    # deleted_count = test.delete_many_documents(delete_filter)
    
    # Get sensor package
    # received_data = test.track_reading(sensor_id,sensor_type)
    
    

    
import os
import sys
import json
import certifi
import pandas as pd
import numpy as np
import pymongo 

from dotenv import load_dotenv
from networksecurity.exception.exception import CustomException
from networksecurity.logging.logger import logging

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

ca = certifi.where()

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            logging.info("Exception occurred in __init__ method of NetworkDataExtract class")
            raise CustomException(e, sys)
        
    def cv_to_json_convertor(self, file_path):
        """
        This function converts a CSV file to a JSON file.
        """
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            logging.info("Exception occurred in cv_to_json_convertor method of NetworkDataExtract class")
            raise CustomException(e, sys)
        
    def insert_data_to_mongodb(self, records, database, collection):
        """
        This function inserts data into a MongoDB collection.
        """
        try:
            self.database = database
            self.collection = collection
            self.records = records

            self.mongo_client = pymongo.MongoClient(MONGO_URI, tlsCAFile=ca)
            
            self.database = self.mongo_client[self.database]
            self.collection = self.database[self.collection]

            self.collection.insert_many(self.records)
            logging.info("Data inserted successfully into MongoDB")

            return (len(self.records))
        except Exception as e:
            logging.info("Exception occurred in insert_data_to_mongodb method of NetworkDataExtract class")
            raise CustomException(e, sys)
        
if __name__ == "__main__":
    FILE_PATH = "Network_Data\phisingData.csv"
    DATABASE = "network_security_db"
    COLLECTION = "Network_Security"
    extractor = NetworkDataExtract()
    records = extractor.cv_to_json_convertor(FILE_PATH)
    no_of_records = extractor.insert_data_to_mongodb(records, DATABASE, COLLECTION)
    print(f"Total number of records inserted: {no_of_records}")

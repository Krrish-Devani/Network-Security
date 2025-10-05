import os

from networksecurity.exception.exception import CustomException
from networksecurity.logging.logger import logging
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient

load_dotenv()

uri = os.getenv("MONGO_URI")

# Create a new client and connect to the server
client = MongoClient(uri)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    logging.error("Failed to connect to MongoDB")
    raise CustomException("Failed to connect to MongoDB")
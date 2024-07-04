# File used for solely hooking up the database for the project
import os
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import Depends
from pymongo.database import Database
from dotenv import load_dotenv

load_dotenv()

MONGO_DETAILS = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

client = AsyncIOMotorClient(MONGO_DETAILS)
database = client[DATABASE_NAME]

def get_database() -> Database:
    return database

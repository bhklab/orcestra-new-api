from fastapi import FastAPI, Depends, Query
from api.routes import routes
from api.db import get_database
from pymongo.database import Database
from api.convert import convert
from api.models.Pipeline import PipelineOut
import logging
import os

# Define the log directory and file name
log_directory = "api/logs"
log_filename = "application.log"
log_filepath = os.path.join(log_directory, log_filename)

# Create the log directory if it does not exist
os.makedirs(log_directory, exist_ok=True)

logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        filename='api/logs/application.log',
        filemode='w' # Overwrite log file on each run for this example
)
logging.getLogger().addHandler(logging.StreamHandler())  # Also log to console
app = FastAPI()

# To test all is working fine (http://localhost:8000)
@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

app.include_router(router=routes.router, prefix='/api')

# test the database is connected fine (http://localhost:8000/test-db). Get the first document in snakemake_pipeline collection
@app.get("/test-db", response_model=PipelineOut)
async def test_db(database: Database = Depends(get_database)):
    collection = database["snakemake_pipeline"]
    document = await collection.find_one({})
    return convert(document)



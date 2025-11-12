from fastapi import FastAPI, Depends, Query
from api.routes import routes
from api.db import get_database
from pymongo.database import Database
from api.convert import convert
from api.models.Pipeline import PipelineOut
from api.logger_config import setup_logger

# Initialize root logger for app
setup_logger()


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



from fastapi import FastAPI, Depends, Query
from routes import routes
from db import get_database
from pymongo.database import Database
from convert import convert
from models.Pipeline import PipelineOut

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



from models.Pipeline import (
    CreatePipeline,
    PipelineOut,
    UpdatePipeline,
)

from fastapi import Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorCollection
from db import get_database

database = get_database()
snakemake_pipelines = database["snakemake_pipeline"]

# Check if git_url exists in database
async def git_url_exists(git_url: str, collection: AsyncIOMotorCollection) -> bool:
    url = await collection.find_one({"git_url": git_url})
    if url is not None:
        return True
    return False

# CREATE
# Add pipeline entry to database
async def add_pipeline(
    pipeline: CreatePipeline,
    collection: AsyncIOMotorCollection,
) -> PipelineOut:
    
    try:
        new_pipeline = pipeline.model_dump()
        result = await collection.insert_one(new_pipeline)
        new_pipeline['id'] = result.inserted_id
    except ValueError as e:
        await pipeline.delete_local()
        raise HTTPException(status_code=400, detail=str(e))
   
    return PipelineOut(**new_pipeline)

# Recieve a pipeline name, github url, list of output files, path to the snakefile, path to pipeline configurations, path to conda environment
async def create_pipeline(data: CreatePipeline) -> PipelineOut:
    try:
        pipeline = CreatePipeline(**data)
        if await git_url_exists(pipeline.git_url, snakemake_pipelines):
            raise ValueError("Git url already exists in database")
        
    except KeyError as e:
        raise ValueError(f"Missing required field: {e}")
    
    # validate git_url
    if not await pipeline.validate_url():
        raise HTTPException(status_code=400, detail='Invalid git url')
    
    # clone pipeline
    try:
        await pipeline.clone()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    # validate file paths exist
    try:
        await pipeline.validate_local_file_paths()
    except Exception as e:
        await pipeline.delete_local()
        raise HTTPException(status_code=400, detail=str(e))
    
    # add to database
    return await add_pipeline(pipeline, snakemake_pipelines)

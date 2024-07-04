from models.Pipeline import (
    CreatePipeline,
    PipelineOut,
    UpdatePipeline,
)

from typing import List, Dict
from fastapi import Depends
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection
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
    new_pipeline = pipeline.model_dump()
    result = await collection.insert_one(new_pipeline)
    new_pipeline['id'] = result.inserted_id

    return PipelineOut(**new_pipeline)

# Recieve a pipeline name, github url, list of output files, path to the snakefile, path to pipeline configurations, path to conda environment
async def create_pipeline(data: Dict) -> Dict:
    try:
        pipeline_name: str = data["pipeline_name"]
        git_url: str = data["git_url"]
        output_files: List[str] = data["output_files"]
        snakefile_path: str = data["snakefile_path"]
        config_file_path: str = data["config_file_path"]
        conda_env_file_path: str = data["conda_env_file_path"]

        pipeline = CreatePipeline(**data)

        if await git_url_exists(git_url, snakemake_pipelines):
            raise ValueError("Git url already exists in database")
        
        return await add_pipeline(pipeline, snakemake_pipelines)

    except KeyError as e:
        raise ValueError(f"Missing required field: {e}")

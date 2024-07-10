from models.Pipeline import (
    CreatePipeline,
    PipelineOut,
    UpdatePipeline,
)

from fastapi import Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorCollection
from db import get_database

database = get_database()
snakemake_pipelines_collection = database["snakemake_pipeline"]

# Check if git_url exists in database
async def git_url_exists(git_url: str, collection: AsyncIOMotorCollection) -> bool:
    url = await collection.find_one({"git_url": git_url})
    if url is not None:
        return True
    return False

# Add pipeline entry to database
async def add_pipeline(
    pipeline: CreatePipeline,
    collection: AsyncIOMotorCollection,
) -> None:
    
    try:
        await collection.insert_one(pipeline.model_dump())
    except ValueError as error:
        await pipeline.delete_local()
        raise HTTPException(status_code=401, detail=str(error))
   
# Recieve a pipeline name, github url, list of output files, path to the snakefile, path to pipeline configurations, path to conda environment
async def create_pipeline(data: CreatePipeline) -> None:
    try:
        pipeline = CreatePipeline(**data)
        if await git_url_exists(pipeline.git_url, snakemake_pipelines_collection):
            raise HTTPException(status_code=400, detail="Git url already exists in database")

    except KeyError as error:
        raise HTTPException(status_code=400, detail=f"Missing required field: {error}")
    
    # validate git_url
    await pipeline.validate_url()
    
    # clone pipeline
    await pipeline.clone()
    
    # validate file paths exist
    await pipeline.validate_local_file_paths()
    
    # perform a dry run of the pipeline and recieve output
    dry_run_status = await pipeline.dry_run()

    # if dry-run contains unsucessful output throw exception
    if "The order of jobs does not reflect the order of execution" not in dry_run_status:
        await pipeline.delete_local()
        raise HTTPException(status_code=400, detail=(f"Error performing dry run: {dry_run_status}"))

    # add to database
    await add_pipeline(pipeline, snakemake_pipelines_collection)

    raise HTTPException(status_code=200, detail={"clone_status": "Pipeline cloned successfully",
                                                 "configuration_checks": "Pipeline passed configuration checks",
                                                 "dry_run_status": str(dry_run_status),
                                                 "pipeline_directory": str(pipeline.fs_path),
                                                 "pipeline_database_entry": pipeline.model_dump()})
    

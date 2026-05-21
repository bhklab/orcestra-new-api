from api.models.Pipeline import CreatePipeline
from fastapi import Depends, HTTPException
from api.db import get_database
import time
import logging

logger = logging.getLogger(__name__)

database = get_database()
create_snakemake_pipeline_collection = database["create_snakemake_pipeline"]

# Recieve a pipeline name, github url, list of output files, path to the snakefile, path to pipeline configurations, path to conda environment
async def create_pipeline(data: CreatePipeline) -> CreatePipeline:
    try:
        pipeline = CreatePipeline(**data)
        logger.info("Pipeline creation process started for: %s", pipeline.pipeline_name)
        if await pipeline.git_url_exists(create_snakemake_pipeline_collection):
            raise HTTPException(status_code=400, detail="Git url already exists in database")
        if await pipeline.pipeline_name_exists(create_snakemake_pipeline_collection):
            raise HTTPException(status_code=400, detail="Pipeline name already exists in database")

    except KeyError as error:
        raise HTTPException(status_code=400, detail=f"Missing required field: {error}")
    
    # validate git_url
    await pipeline.validate_url
    

    # add to database
    await pipeline.add_pipeline(create_snakemake_pipeline_collection)
    return {"configuration_checks": "Pipeline passed configuration checks",
            "pipeline_database_entry": pipeline.model_dump()}
    

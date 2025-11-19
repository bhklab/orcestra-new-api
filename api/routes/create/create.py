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

    except KeyError as error:
        raise HTTPException(status_code=400, detail=f"Missing required field: {error}")
    
    # validate git_url
    await pipeline.validate_url
    
    # clone pipeline
    await pipeline.clone()
    logger.info("Cloned pipeline repository successfully")
    
    # validate file paths exist
    await pipeline.validate_local_file_paths()

    # Create either pixi or conda environment
    await pipeline.create_pixi_or_conda_env()

    time.sleep(10)
        
    # perform a dry run of the pipeline and recieve output
    dry_run_status = await pipeline.dry_run()
        
    # if dry-run contains unsucessful output throw exception
    if dry_run_status[0] != 0:
        if not pipeline.pixi_use:
                    await pipeline.delete_conda_env()
        await pipeline.delete_local()
        raise HTTPException(status_code=400, detail=(f"Error performing dry run: {dry_run_status[2]}"))
    
    # delete conda environment after dry run
    if not pipeline.pixi_use:
        await pipeline.delete_conda_env()

    logger.info("Pipeline dry run successful")

    # add to database
    await pipeline.add_pipeline(create_snakemake_pipeline_collection)
    return {"clone_status": "Pipeline cloned successfully",
            "configuration_checks": "Pipeline passed configuration checks",
            "dry_run_status": str(dry_run_status),
            "pipeline_directory": str(pipeline.fs_path),
            "pipeline_database_entry": pipeline.model_dump()}
    

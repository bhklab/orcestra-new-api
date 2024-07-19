from models.Pipeline import CreatePipeline
from fastapi import Depends, HTTPException
from db import get_database

database = get_database()
snakemake_pipelines_collection = database["snakemake_pipeline"]

# Recieve a pipeline name, github url, list of output files, path to the snakefile, path to pipeline configurations, path to conda environment
async def create_pipeline(data: CreatePipeline) -> CreatePipeline:
    try:
        pipeline = CreatePipeline(**data)
        if await pipeline.git_url_exists(snakemake_pipelines_collection):
            raise HTTPException(status_code=400, detail="Git url already exists in database")

    except KeyError as error:
        raise HTTPException(status_code=400, detail=f"Missing required field: {error}")
    
    # validate git_url
    await pipeline.validate_url
    
    # clone pipeline
    await pipeline.clone()
    
    # validate file paths exist
    await pipeline.validate_local_file_paths()

    # create conda environment
    await pipeline.create_env()
        
    # perform a dry run of the pipeline and recieve output
    dry_run_status = await pipeline.dry_run()
        
    # if dry-run contains unsucessful output throw exception
    if "The order of jobs does not reflect the order of execution" not in dry_run_status:
        await pipeline.delete_local()
        raise HTTPException(status_code=400, detail=(f"Error performing dry run: {dry_run_status}"))
    
    # delete conda environment after dry run
    await pipeline.delete_env()

    # add to database
    await pipeline.add_pipeline(snakemake_pipelines_collection)

    return {"clone_status": "Pipeline cloned successfully",
            "configuration_checks": "Pipeline passed configuration checks",
            "dry_run_status": str(dry_run_status),
            "pipeline_directory": str(pipeline.fs_path),
            "pipeline_database_entry": pipeline.model_dump()}
    

from models.Pipeline import CreatePipeline
from fastapi import Depends, HTTPException
from db import get_database

database = get_database()
snakemake_pipelines_collection = database["snakemake_pipeline"]

async def delete_pipeline(data: CreatePipeline) -> None:
    pipeline_entry = await snakemake_pipelines_collection.find_one(
        filter={'pipeline_name': data},
    )
    if pipeline_entry is None:
        raise HTTPException(status_code=404, detail='Pipeline not found')
    
    pipeline = CreatePipeline(**pipeline_entry)

    # delete pipeline db entry
    result = await snakemake_pipelines_collection.delete_one(
        filter={'pipeline_name': pipeline.pipeline_name},
    )
    if result.deleted_count == 0:
        raise ValueError('Pipeline not found')
    
    # delete local repository
    await pipeline.delete_local()

    return {"deletion_status": "Pipeline deleted successfully",
            "pipeline_database_entry": pipeline.model_dump()}


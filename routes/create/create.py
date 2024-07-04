from models.Pipeline import (
    CreatePipeline,
    PipelineOut,
    UpdatePipeline,
)
from fastapi import Depends
from motor.motor_asyncio import AsyncIOMotorDatabase
###############################################################################
# CREATE

async def create_pipeline(
    pipeline: CreatePipeline,
) -> PipelineOut:
    # logger.debug(f'Creating pipeline: {pipeline.pipeline_name}')

    # # check if pipeline already exists
    # existing_pipeline = await crud_pipeline.get_pipeline_by_name(pipeline.pipeline_name, db)
    # if existing_pipeline is not None:
    #     raise HTTPException(status_code=400, detail='Pipeline with this name already exists')
    
    # ## need to run new_pipeline validate and clone methods but they are async
    # if not await pipeline.validate_url():
    #     raise HTTPException(status_code=400, detail='Invalid git url')

    # try:
    #     logger.info(f'Cloning {pipeline.git_url} to {pipeline.fs_path}')
    #     await pipeline.clone()
    # except Exception as e:
    #     raise HTTPException(status_code=400, detail=str(e))

    # try:
    #     logger.info(f'Validating local paths for {pipeline.pipeline_name}')
    #     await pipeline.validate_local_file_paths()
    # except Exception as e:
    #     await pipeline.delete_local()
    #     raise HTTPException(status_code=400, detail=str(e))
    
    # try:
    #     new_pipeline = await crud_pipeline.create_pipeline(pipeline, db)
    # except ValueError as e:
    #     await pipeline.delete_local()
    #     raise HTTPException(status_code=400, detail=str(e))
    # finally:
    #     if not new_pipeline:
    #         await pipeline.delete_local()
    return "hello"
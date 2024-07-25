from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Path,
)
from pydantic import ValidationError
from models.Pipeline import CreatePipeline, RunPipeline
from routes.create.create import create_pipeline
from routes.run.run import run_pipeline
from routes.delete.delete import delete_pipeline
from typing import Dict

router = APIRouter()

@router.post('/create-pipeline', response_model=Dict)
async def create_pipeline_endpoint(data: CreatePipeline):
    try:
        return await create_pipeline(data.model_dump())
    except ValidationError as e:
        raise HTTPException(status_code=400, detail="Validation error: " + str(e.errors()))
    
@router.post('/run-pipeline', response_model=Dict)
async def run_pipeline_endpoint(data: Dict):
    try:
        return await run_pipeline(data)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail="Validation error: " + str(e.errors()))

@router.delete('/delete-pipeline', response_model=Dict)
async def delete_pipeline_endpoint(pipeline_name: str):
    try:
        return await delete_pipeline(pipeline_name)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail="Validation error: " + str(e.errors()))

    

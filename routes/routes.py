from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Path,
)
from pydantic import ValidationError
from models.Pipeline import CreatePipeline, PipelineOut
from routes.create.create import create_pipeline

router = APIRouter()

@router.post('/create-pipeline', response_model=PipelineOut)
async def create_pipeline_endpoint(data: CreatePipeline):
    try:
        return await create_pipeline(data.dict())
    except ValidationError as e:
        raise HTTPException(status_code=400, detail="Validation error: " + str(e.errors()))

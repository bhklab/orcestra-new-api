from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Path,
)
from routes.create.create import create_pipeline

router = APIRouter()

@router.post('/create-pipeline')
def create_pipeline_endpoint(data: dict):
    return create_pipeline(data)
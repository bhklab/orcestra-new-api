from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Path,
    Header,
    status,
)
from pydantic import ValidationError
from api.models.Pipeline import CreatePipeline, RunPipeline, Zenodo, JenkinsStageEvent
from api.routes.create.create import create_pipeline
from api.routes.run.run import run_pipeline
from api.routes.delete.delete import delete_pipeline
from api.routes.zenodo.zenodo import zenodo_upload
from api.routes.jenkins.update_pipeline_run_status import update_pipeline_run_status
from typing import Dict
import os
JENKINS_STAGE_EVENT_TOKEN = os.getenv("JENKINS_STAGE_EVENT_TOKEN")

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


@router.post('/internal/jenkins/stage-event', response_model=Dict)
async def update_pipeline_run_status_endpoint(
    data: JenkinsStageEvent,
    x_jenkins_token: str = Header(default="")
):
    if x_jenkins_token != JENKINS_STAGE_EVENT_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Jenkins token",
        )

    return await update_pipeline_run_status(data.model_dump())
@router.post('/zenodo', response_model=Dict)
async def zenodo_upload_endpoint(data: Zenodo):
    try:
        return await zenodo_upload(data)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail="Validation error: " + str(e.errors()))

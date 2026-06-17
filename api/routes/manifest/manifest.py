from api.models.Pipeline import RunPipeline
from fastapi import Depends, HTTPException
from api.db import get_database
from datetime import datetime, timezone
import logging
import threading
import asyncio
import os
import json

logger = logging.getLogger(__name__)
database = get_database()
create_snakemake_pipeline_collection = database["create_snakemake_pipeline"]
ran_pipelines_collection = database["run_snakemake_pipeline"]

async def get_manifest_data(data: str) -> dict:

    pipeline_name = data
    if not pipeline_name:
        raise HTTPException(status_code=400, detail="pipeline_name is required")

    create_pipeline_data = await create_snakemake_pipeline_collection.find_one({"pipeline_name": pipeline_name})

    if create_pipeline_data is None:
        raise HTTPException(status_code=400, detail="Pipeline not found")

    most_recent_run = await ran_pipelines_collection.find_one({"create_pipeline": create_pipeline_data['_id'], "status": "succeeded"}, sort = [("created_at", -1)])
    if most_recent_run is None:
        raise HTTPException(status_code=400, detail=f"{pipeline_name} has no recorded successful runs.")
    
    file_path = f'/mnt/gcs/nicholas-testing/pipelines/{pipeline_name}/{most_recent_run["run_id"]}/checksum_manifest.json'

    with open(file_path, 'r', encoding='utf-8') as file:
        manifest_file_data = json.load(file)

    return manifest_file_data

    


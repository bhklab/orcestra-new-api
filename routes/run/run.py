from models.Pipeline import (
    RunPipeline
)

from fastapi import Depends, HTTPException
from db import get_database

database = get_database()
snakemake_pipelines_collection = database["snakemake_pipeline"]

async def run_pipeline(data: RunPipeline) -> None:
	pipeline = RunPipeline(**data)
	pass
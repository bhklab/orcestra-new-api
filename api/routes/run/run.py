from api.models.Pipeline import RunPipeline
from fastapi import Depends, HTTPException
from api.db import get_database
from datetime import datetime, timezone
import logging
import threading
import asyncio
import os

logger = logging.getLogger(__name__)
database = get_database()
create_snakemake_pipeline_collection = database["create_snakemake_pipeline"]
ran_pipelines_collection = database["run_snakemake_pipeline"]
async def run_pipeline(data: RunPipeline) -> RunPipeline:
	try:
		pipeline_name = data['pipeline_name']
		pipeline_data = await create_snakemake_pipeline_collection.find_one({"pipeline_name": pipeline_name})
		if pipeline_data is None:
			raise HTTPException(status_code=404, detail="Pipeline not found")
		logger.info("%s pipeline found in database", pipeline_name)
		current_running_pipeline = await ran_pipelines_collection.find_one({"status": "running"})
		if current_running_pipeline is not None:
			raise HTTPException(status_code=400, detail=f"The {current_running_pipeline['pipeline_name']} pipeline is currently running. Please wait until it finishes.")
	
	except Exception as error:
		raise HTTPException(status_code=400, detail=str(error))
	
	pipeline = {**data, **pipeline_data}
	pipeline = RunPipeline(**pipeline)
	logger.info("Pipeline run process started for: %s", pipeline.pipeline_name)
	run_id = await pipeline.determine_run_id()
	return {**pipeline.model_dump(), "run_id": run_id}



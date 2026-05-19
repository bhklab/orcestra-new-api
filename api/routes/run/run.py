from api.models.Pipeline import RunPipeline, RunPipelineKubernetes
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

async def run_pipeline(data: RunPipeline) -> RunPipeline:
	try:
		pipeline_name = data['pipeline_name']
		pipeline_data = await create_snakemake_pipeline_collection.find_one({"pipeline_name": pipeline_name})
		if pipeline_data is None:
			raise HTTPException(status_code=404, detail="Pipeline not found")
	
	except Exception as error:
		raise HTTPException(status_code=400, detail=str(error))
	
	logger.info("%s pipeline found in database", pipeline_name)
	pipeline = {**data, **pipeline_data}
	if data['kubernetes']:
		pipeline = RunPipelineKubernetes(**pipeline)
		logger.info("Kubernetes pipeline run requested")
	else:
		pipeline = RunPipeline(**pipeline)


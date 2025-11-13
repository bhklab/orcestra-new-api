from api.models.Pipeline import RunPipeline
from fastapi import Depends, HTTPException
from api.db import get_database
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)
database = get_database()
snakemake_pipelines_collection = database["snakemake_pipeline"]

async def run_pipeline(data: RunPipeline) -> RunPipeline:
	try:
		pipeline_name = data['pipeline_name']
		pipeline_data = await snakemake_pipelines_collection.find_one({"pipeline_name": pipeline_name})
		if pipeline_data is None:
			raise HTTPException(status_code=404, detail="Pipeline not found")
	
	except Exception as error:
		raise HTTPException(status_code=400, detail=str(error))
	
	logger.info("%s pipeline found in database", pipeline_name)
	pipeline = {**data, **pipeline_data}
	pipeline = RunPipeline(**pipeline)

	# pull changes from pipeline repository
	await pipeline.pull()

	# Create either pixi or conda environment
	if not pipeline.pixi_use:
		if pipeline.pixi_use:
			await pipeline.create_pixi_env()
		elif not pipeline.pixi_use:
			await pipeline.create_conda_env()

	# run pipeline
	run_status = await pipeline.execute_pipeline()

	# if pipeline run contains unsuccessful output throw exception
	if "Complete" not in run_status:
		if not pipeline.pixi_use:
			await pipeline.delete_conda_env()
		await pipeline.delete_local()
		raise HTTPException(status_code=400, detail=(f"Error running pipeline: {run_status}"))

	logger.info("Pipeline run successful")
	# delete conda environment after run
	if not pipeline.pixi_use:
		await pipeline.delete_conda_env()

	pipeline.last_updated_at = datetime.now(timezone.utc).isoformat()

	return {
		"success": True,
		"run_status": str(run_status),
		"pipeline_database_entry": pipeline.model_dump()}

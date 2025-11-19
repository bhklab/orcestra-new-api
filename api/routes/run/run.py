from api.models.Pipeline import RunPipeline
from fastapi import Depends, HTTPException
from api.db import get_database
from datetime import datetime, timezone
import logging

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
	pipeline = RunPipeline(**pipeline)

	# pull changes from pipeline repository
	await pipeline.pull()

	# Create either pixi or conda environment
	await pipeline.create_pixi_or_conda_env()

	#Dry run pipeline just to see if anything is obviously wrong before actual execution
	await pipeline.dry_run()
	logger.info("Pipeline dry-run completed")

	# run pipeline
	run_status = await pipeline.execute_pipeline()
	# if pipeline run contains unsuccessful output throw exception
	if run_status[0] != 0:
		if not pipeline.pixi_use:
			await pipeline.delete_conda_env()
		raise HTTPException(status_code=400, detail=(f"Error running pipeline: {run_status[2]}"))

	logger.info("Pipeline run successful")
	# delete conda environment after run
	if not pipeline.pixi_use:
		await pipeline.delete_conda_env()

	pipeline.last_updated_at = datetime.now(timezone.utc).isoformat()
	await pipeline.save_run_entry()

	return {
		"success": True,
		"run_status": str(run_status[2]),
		"pipeline_database_entry": pipeline.model_dump()}

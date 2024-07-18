from models.Pipeline import RunPipeline
from fastapi import Depends, HTTPException
from db import get_database

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
	
	pipeline = {**data, **pipeline_data}
	pipeline = RunPipeline(**pipeline)

	# pull changes from pipeline repository
	await pipeline.pull()

	# create conda environment
	await pipeline.create_env()

	# run pipeline
	run_status = await pipeline.execute_pipeline()

	# delete conda environment
	await pipeline.delete_env()

	return {"run_status": str(run_status),
		    "pipeline_database_entry": pipeline.model_dump()}

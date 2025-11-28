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
	logger.info("Starting pipeline run")
	thread = threading.Thread(target=run_pipeline_in_thread, args=(pipeline,))
	thread.start()

	return {
		"success": True,
		"run_status": f"{pipeline.pipeline_name} Pipeline is running. You will receive an email soon outlining the status of your run. Thank you."
	}

def run_pipeline_in_thread(pipeline_instance):
    """A synchronous wrapper function to start an asyncio event loop in a new thread."""
    # This function runs entirely within the new thread after thread.start() is called
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        # Run the async pipeline function to completion within this new thread's loop
        loop.run_until_complete(pipeline_instance.execute_pipeline())
    except Exception as e:
        logging.exception("Error running pipeline in separate thread:")
    finally:
        loop.close()
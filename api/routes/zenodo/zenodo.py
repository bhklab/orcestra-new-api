import requests
import os
from dotenv import load_dotenv
from pathlib import Path
from api.db import get_database
from api.models.Pipeline import Zenodo


database = get_database()
create_snakemake_pipeline_collection = database["create_snakemake_pipeline"]

load_dotenv()

async def zenodo_upload (pipeline: Zenodo) -> None:
	upload_status = await pipeline.zenodo_upload()
	return upload_status


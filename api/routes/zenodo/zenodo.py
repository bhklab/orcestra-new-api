import requests
import os
from dotenv import load_dotenv
from pathlib import Path
from api.db import get_database
from api.models.Pipeline import Zenodo


database = get_database()
snakemake_pipelines_collection = database["snakemake_pipeline"]

load_dotenv()

async def zenodo_upload (pipeline: Zenodo) -> None:
	upload_status = pipeline.zenodo_upload()


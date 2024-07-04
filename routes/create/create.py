from models.Pipeline import (
    CreatePipeline,
    PipelineOut,
    UpdatePipeline,
)
from typing import List, Dict
from fastapi import Depends
from motor.motor_asyncio import AsyncIOMotorDatabase

#Recieve a github url, list of output files, path to the snakefile, path to pipeline configurations, path to conda environment
def create_pipeline(data: Dict) -> Dict:
    try:
        git_url: str = data["git_url"]
        output_files: List[str] = data["output_files"]
        snakefile_path: str = data["snakefile_path"]
        config_file_path: str = data["config_file_path"]
        conda_env_file_path: str = data["conda_env_file_path"]

        return "test"

    except KeyError as e:
        raise ValueError(f"Missing required field: {e}")
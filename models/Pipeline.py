from datetime import datetime, timezone
from pathlib import Path
from shutil import rmtree
from typing import (
    List,
    Optional,
)

from core.exec import execute_command
from git import Repo
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from core.git import validate_github_repo, clone_github_repo
from models.common import PyObjectId
from fastapi import HTTPException
import yaml
import os
import re

class SnakemakePipeline(BaseModel):
    git_url: str
    output_files: List[str]
    snakefile_path: str = Field(
        default="Snakefile",
    )
    config_file_path: str = Field(
        default="config/config.yaml",
    )
    conda_env_file_path: str = Field(
        default="pipeline_env.yaml",
    )

class CreatePipeline(SnakemakePipeline):
    pipeline_name: str
    conda_env_name: Optional[str] = None
    created_at: Optional[str] = datetime.now(timezone.utc).isoformat()
    last_updated_at: Optional[str] = datetime.now(timezone.utc).isoformat()

    model_config: ConfigDict = {
        "json_schema_extra": {
            "example": {
                "pipeline_name": "snakemake_bioconductor",
                "git_url": "https://github.com/jjjermiah/5_snakemake_bioconductor.git",
                "output_files": ["results/output.txt"],
                "snakefile_path": "workflow/Snakefile",
                "config_file_path": "workflow/config/config.yaml",
                "conda_env_file_path": "workflow/envs/pipeline_env.yaml",
            },
        }
    }
    @property
    def fs_path(self) -> Path:
        return Path.home() / "pipelines" / self.pipeline_name
    
    async def validate_url(self) -> bool:
        await validate_github_repo(self.git_url)
    
    async def clone(self):
        await clone_github_repo(self.git_url, self.fs_path)
    
    async def validate_local_file_paths(self) -> bool:
        """After cloning, need to validate that the paths provided exist.

        when creating, we ask for snakefile, config and conda env file paths

        Returns:
            bool: True if all paths exist

        Raises:
            AssertionError: If any of the paths do not exist.
        """
        
        if not self.fs_path.exists():
            await self.delete_local()
            raise HTTPException(status_code=400, detail=f"Path: '{self.fs_path}' does not exist.")
        if not (self.fs_path / self.snakefile_path).exists():
            await self.delete_local()
            raise HTTPException(status_code=400, detail=f"Snakefile: '{self.snakefile_path}' does not exist.")
        if not (self.fs_path / self.config_file_path).exists():
            await self.delete_local()
            raise HTTPException(status_code=400, detail=f"Config file: '{self.config_file_path}' does not exist.")
        if not (self.fs_path / self.conda_env_file_path).exists():
            await self.delete_local()
            raise HTTPException(status_code=400, detail=f"Conda configuration file: '{self.conda_env_file_path}' does not exist.")
        return True
    

    async def get_env_name(self):
        env_file_path = os.path.join(self.fs_path, self.conda_env_file_path)
        with open(env_file_path, 'r') as file:
            env_data = yaml.safe_load(file)
            
        env_name = env_data.get('name')
            
        if not env_name:
            raise HTTPException(status_code=400, detail="Environment name not found in yaml file")
            
        return env_name
    
    
    async def env_exists(self, env_name):
        command = "conda env list"
        exit_status, output, error = await execute_command(command, self.fs_path)

        if exit_status != 0:
            raise HTTPException(status_code=400, detail=f"Error getting conda environments: {error.decode()}")

        env_exists = env_name in output
        return env_exists
    
    async def pull(self) -> None:
        repo = await pull_latest_pipeline(self.fs_path)
        _commit_history = repo.iter_commits()  # unused for now

        try:
            await self.validate_local_file_paths()
        except AssertionError as error:
            raise Exception(f"Error validating local paths: {error}")

    async def delete_local(self) -> None:
        rmtree(self.fs_path)

    async def dry_run(self) -> None:
        """Dry run the pipeline.

        Should be able to run `snakemake -n --use-conda`
        make use of the `execute_command` function from `orcestrator.core.exec`

        Notes:
        - the prod environment has snakemake & conda installed already
        - we expect the curator to have the conda env file as well
        """
        # extract Snakefile name from path to use in bash execution
        match = re.search(r'[^/]*$', self.snakefile_path)
        snakefile_name = self.snakefile_path
        if match:
            snakefile_name = match.group(0)
        
        command = f"snakemake -s {snakefile_name} -n --use-conda"


        # remove /Snakefile from end of snakefile_path to allow traversal to parent folder
        snakefile_path = re.sub(r'/[^/]*$', '', self.snakefile_path)
        if (snakefile_path == self.snakefile_path): # if snakefile isn't in a sub directory make value empty
            snakefile_path = ""
        
        cwd = f"{self.fs_path}/{snakefile_path}"
        
        try:
            exit_status, output, error = await execute_command(command, cwd)
        except Exception as error:
            await self.delete_local()
            raise HTTPException(status_code=400, detail=f"Error performing dry run: {error}")   
        
class UpdatePipeline(SnakemakePipeline):
    # remove the pipeline_name from the update
    pass

class PipelineOut(SnakemakePipeline):
    pipeline_name: str
    id: PyObjectId = Field(alias="_id", default=None)

    # If needed, add extra fields that should be included in the response
    conda_env_name: Optional[str] = None
    created_at: Optional[str] = None
    last_updated_at: Optional[str] = None


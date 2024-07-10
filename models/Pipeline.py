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
from motor.motor_asyncio import AsyncIOMotorCollection
from db import get_database

database = get_database()
snakemake_pipelines_collection = database["snakemake_pipeline"]

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
        """Returns the path for the pipeline's directory.
        """
        return Path.home() / "pipelines" / self.pipeline_name
    
    async def validate_url(self) -> bool:
        """First, we need to validate that the provided Git URL is correct
           and it is associated with a repository.

           Calls `validate_github_repo` function from `core.git`.

        Returns:
            bool: True if Git URL is valid.

        Raises:
            HTTPException: If Git URL is invalid.
            HTTPException: If a valid repository is not found.
        """

        await validate_github_repo(self.git_url)
    

    async def git_url_exists(self, collection: AsyncIOMotorCollection) -> bool:
        """Next, we need to check whether the provided Git URL already exists in db.

        Returns:
            bool: True if Git URL does exist and False otherwise
        """

        url = await collection.find_one({"git_url": self.git_url})
        if url is not None:
            return True
        return False
    

    async def clone(self):
        """After Git URL validation, the repository needs to be cloned.

           Calls `clone_github_rep` function from `core.git`.
        
        Returns: 
            Repo: The cloned repository object.

        Raises:
            Exception: If the destination directory already exists.
            GitCommandError: If there is an error while cloning the repository.
            Exception: If there is an unexpected error during the cloning process.
        """

        await clone_github_repo(self.git_url, self.fs_path)
    

    async def validate_local_file_paths(self) -> bool:
        """After cloning, need to validate that the paths provided exist.

        When creating, we ask for snakefile, config and conda env file paths

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

    async def delete_local(self) -> None:
        """At any point in configuration process if there is an error, 
           we want to delete the local cloned repository.
           
           This ensures there are no unused repositories.
        """

        rmtree(self.fs_path)

    async def dry_run(self) -> str:
        """Dry run the pipeline.

        Should be able to run `snakemake -n --use-conda`
        make use of the `execute_command` function from `core.exec`

        Notes:
        - the prod environment has snakemake & conda installed already
        - we expect the curator to have the conda env file as well

        Returns: 
            Str: The output of the dry run

        Raises:
            HTTPException: If there is an error performing the dry run.
        """

        command = f"snakemake -s {self.snakefile_path} -n --use-conda"
        cwd = f"{self.fs_path}"

        try:
            output = await execute_command(command, cwd)

            # format output
            output = str(output).replace("\\n", "")
            output = output.replace("\\", "")
            
            return output
        except Exception as error:
            await self.delete_local()
            raise HTTPException(status_code=400, detail=f"Error performing dry run: {error}")   
    

    async def add_pipeline(self, collection: AsyncIOMotorCollection,) -> None:
        """After all the above steps, the pipeline entry is added to the db.

        Raises:
            HTTPException: If there is an error adding entry to db.
        """

        try:
            await collection.insert_one(self.model_dump())
        except ValueError as error:
            await self.delete_local()
            raise HTTPException(status_code=401, detail=str(error))
        
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


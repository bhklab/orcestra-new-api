from datetime import datetime, timezone
from pathlib import Path
from shutil import rmtree
from typing import (
    List,
    Optional,
)

from api.core.exec import execute_command
from git import Repo
from pydantic import (
    BaseModel, 
    ConfigDict,
    Field,
)

from api.core.git import validate_github_repo, clone_github_repo, pull_github_repo, pull_latest_pipeline
from api.models.common import PyObjectId
from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorCollection
from api.db import get_database
import os
import shutil
import logging

logger = logging.getLogger(__name__)

database = get_database()
snakemake_pipelines_collection = database["snakemake_pipeline"]

class SnakemakePipeline(BaseModel):
    git_url: str
    pipeline_name: str
    output_files: List[str]
    snakefile_path: str = Field(
        default="Snakefile",
    )
    config_file_path: Optional[str] = Field(
        default="config/config.yaml",
    )
    conda_env_file_path: Optional[str] = Field(
        default="pipeline_env.yaml",
    )
    created_at: Optional[str] = datetime.now(timezone.utc).isoformat()
    last_updated_at: Optional[str] = datetime.now(timezone.utc).isoformat()
    pixi_use: bool

    @property
    def fs_path(self) -> Path:
        """Returns the path for the pipeline's directory.
        """
        return Path.home() / "pipelines" / self.pipeline_name	
    
    async def delete_local(self) -> None:
        """Delete cloned repository if an error is encountered.
           
           This ensures there are no unused repositories.
        """

        rmtree(self.fs_path)
    
    async def validate_local_file_paths(self) -> bool:
        """Validate provided file paths exist in cloned repository.

        When creating, user enters snakefile, config and conda env file paths.

        Returns:
            bool: True if all paths exist

        Raises:
            AssertionError: If any of the paths do not exist.
        """
        logger.info("Validating repository paths")
        if self.pixi_use:
            if not self.fs_path.exists():
                await self.delete_local()
                raise HTTPException(status_code=400, detail=f"Path: '{self.fs_path}' does not exist.")
            if not (self.fs_path / self.snakefile_path).exists():
                await self.delete_local()
                raise HTTPException(status_code=400, detail=f"Snakefile: '{self.snakefile_path}' does not exist.")
            if not (self.fs_path / "pixi.toml").exists():
                await self.delete_local()
                raise HTTPException(status_code=400, detail=f"pixi config: '{self.snakefile_path}' does not exist despite indicating this pipeline will be using pixi")

        else:
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
        
        logger.info("Repository paths are valid")
        return True
    
    async def create_conda_env(self) -> None:
        """Create conda environment.

        Runs `conda env create -f {env_file_path} -n {env_name}` and
        makes use of the `execute_command` function from `core.exec`

        Raises:
            HTTPException: If there is an error creating the conda environment.
        """
        logger.info("Creating conda environment for pipeline")  
        try:
            env_file_path = self.conda_env_file_path
            env_name = self.pipeline_name
            cwd = f"{self.fs_path}"
            create_cmd = f"conda env create -f {env_file_path} -n {env_name}"
            exit_status, stdout, stderr = await execute_command(create_cmd, cwd)

            if exit_status != 0:
                await self.delete_conda_env()
                await self.delete_local()
                raise HTTPException(status_code=400, detail=f"Error creating conda environment: {stderr}")
        except Exception as error:
            await self.delete_conda_env()
            await self.delete_local()
            raise HTTPException(status_code=400, detail=str(error))
        
        logger.info("Created conda environment successfully")
        
    async def create_pixi_env(self) -> None:
        """Create Pixi environment.

        Runs `pixi install` to install all the needed packages to run the pipeline

        Raises:
            HTTPException: If there is an error creating the Pixi environment.
        """
        logger.info("Creating Pixi environment for pipeline")

        try:
            cwd = f"{self.fs_path}"
            create_cmd = f"pixi install"
            exit_status, stdout, stderr = await execute_command(create_cmd, cwd)

            if exit_status != 0:
                await self.delete_local()
                raise HTTPException(status_code=400, detail=f"Error creating pixi environment: {stderr}")
        except Exception as error:
            await self.delete_local()
            raise HTTPException(status_code=400, detail=str(error))
        
        logger.info("Created Pixi environment successfully")
        
    
    async def delete_conda_env(self) -> None:
        """Delete conda environment.

        Raises:
            HTTPException: If conda environment does not exist.
        """

        try:
            print(Path.cwd())
            env_path = f"{Path.cwd()}/.pixi/envs/default/envs/{self.pipeline_name}"
            if os.path.exists(env_path):
                shutil.rmtree(env_path)
            else:
                await self.delete_local()
                raise HTTPException(status_code=400, detail=f"Environment {self.pipeline_name} does not exist at {env_path}")
        except Exception as error:
            await self.delete_local()
            raise HTTPException(status_code=400, detail=str(error))


class CreatePipeline(SnakemakePipeline):

    model_config: ConfigDict = {
        "json_schema_extra": {
            "example": {
                "pipeline_name": "snakemake_bioconductor",
                "git_url": "https://github.com/jjjermiah/5_snakemake_bioconductor.git",
                "output_file": "",
                "output_files": ["results/output.txt"],
                "snakefile_path": "workflow/Snakefile",
                "config_file_path": "workflow/config/config.yaml",
                "conda_env_file_path": "workflow/envs/pipeline_env.yaml",
                "pixi_use": False
            },
        }
    }
    @property
    
    async def validate_url(self) -> bool:
        """Confirm pipeline's Git URL is a valid repository.

           Calls `validate_github_repo` function from `core.git`.
        """

        await validate_github_repo(self.git_url)
    

    async def git_url_exists(self, collection: AsyncIOMotorCollection) -> bool:
        """
            Verify pipeline's Git URL is not already in database.

            Returns:
                bool: True if Git URL does exist and False otherwise
        """
        logger.info("Checking if Git URL already exists in database")
        url = await collection.find_one({"git_url": self.git_url})
        if url is not None:
            logger.info("Git URL already exists in database")
            return True
        logger.info("Git URL does not exist in database; continuing")
        return False
    

    async def clone(self):
        """Clone GitHub repository.

           Calls `clone_github_repo` function from `core.git`.
        """

        await clone_github_repo(self.git_url, self.fs_path)


    async def dry_run(self) -> str:
        """Dry run the pipeline.

        Should be able to run `snakemake -n`
        make use of the `execute_command` function from `core.exec`

        Notes:
        - the prod environment has snakemake & conda installed already
        - we expect the curator to have the conda env file if needed
        - If a pixi env is being used utilize pixi environment workflow
        - If conda env is being used utilize conda environment workflow

        Returns: 
            Str: The output of the dry run

        Raises:
            HTTPException: If there is an error performing the dry run.
        """
        logger.info("Starting dry run for pipeline")
        if self.pixi_use:
            command = f"pixi run snakemake -s {self.snakefile_path} -n"
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

        elif not self.pixi_use:
            env_name = self.pipeline_name
            command = f"source activate {env_name} && snakemake -s {self.snakefile_path} -n --use-conda"
            cwd = f"{self.fs_path}"
            try:
                output = await execute_command(command, cwd)

                # format output
                output = str(output).replace("\\n", "")
                output = output.replace("\\", "")
                
                return output
            except Exception as error:
                await self.delete_local()
                await self.delete_conda_env()
                raise HTTPException(status_code=400, detail=f"Error performing dry run: {error}")   
        

    async def add_pipeline(self, collection: AsyncIOMotorCollection,) -> None:
        """Add pipeline entry into the database.

        Raises:
            HTTPException: If there is an error adding entry to db.
        """
        logger.info("Adding pipeline to database")
        try:
            await collection.insert_one(self.model_dump())
        except ValueError as error:
            await self.delete_local()
            raise HTTPException(status_code=401, detail=str(error))

class RunPipeline(SnakemakePipeline):

    force_run: bool
    # preserved_directories: Optional[List[str]]
    release_notes: str
    
    async def pull(self) -> None:
        """Pulls changes from GitHub Repository.

           Calls `pull_latest_pipeline` function from `core.git`.
        """
        repo = await pull_latest_pipeline(self.fs_path)
        _commit_history = repo.iter_commits()  # unused for now

        try:
            await self.validate_local_file_paths()
        except AssertionError as ae:
            await self.delete_local()
            raise Exception(f"Error validating local paths: {ae}")


    async def execute_pipeline (self) -> None:
        """Run the pipeline.

        Runs `snakemake -s` with the additional force run option by making
        use of the `execute_command` function from `core.exec`

        Notes:
        - the prod environment has snakemake & conda installed already
        - If a pixi env is being used utilize pixi environment workflow
        - If conda env is being used utilize conda environment workflow

        Returns: 
            Str: The output of the execution

        Raises:
            HTTPException: If there is an error running the pipeline.
        """
        logger.info("Running pipeline")
        
        if self.force_run:
            force_run = "--forcerun"
        else:
            force_run = ""

        if self.pixi_use:
            command = f"pixi run snakemake -s {self.snakefile_path} {force_run} --cores 4"
            cwd = f"{self.fs_path}"

            try:
                output = await execute_command(command, cwd)

                # format output
                output = str(output).replace("\\n", " ")
                output = output.replace("\\", " ")

                return output
            
            except Exception as error:
                await self.delete_local()
                raise HTTPException(status_code=400, detail=f"Error running pipeline: {error}")
        elif not self.pixi_use:
            env_name = self.pipeline_name
            command = f"source activate {env_name} && snakemake -s {self.snakefile_path} --use-conda {force_run} --cores 4"
            cwd = f"{self.fs_path}"

            try:
                output = await execute_command(command, cwd)

                # format output
                output = str(output).replace("\\n", " ")
                output = output.replace("\\", " ")

                return output
            
            except Exception as error:
                await self.delete_conda_env()
                await self.delete_local()
                raise HTTPException(status_code=400, detail=f"Error running pipeline: {error}")
        
class Zenodo(BaseModel):

    pipeline_name: str

    """
        Create new Zenodo entry for dataset and upload output files

        Returns:
            bool: True
    """
    async def zenodo_upload (self) -> bool:
        pipeline_data = await snakemake_pipelines_collection.find_one({"pipeline_name": self.pipeline_name})

        headers = {"Content-Type": "application/json"}
        params = {'access_token': os.getenv("SANDBOX_TOKEN")}

        # Create Zenodo entry for new dataset
        try:
            r = requests.post('https://sandbox.zenodo.org/api/deposit/depositions',
                params=params,
                json={    
                    "metadata": {
                        "title": self.pipeline_name,
                        "upload_type": "dataset",
                        "description": "Dataset description from config file",
                }},
                headers=headers
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Error uploading dataset to zenodo: {error}")

        if r.status_code == '401' or r.status_code == '400':
            raise HTTPException(status_code=r.status_code, detail=f"Error uploading dataset to zenodo with error code: {r.status_code}")

        # retrieve path to put files on zenodo
        bucket_url = r.json()["links"]["bucket"]

        # get files to be uploaded to Zenodo
        path = Path.home() / "pipelines" / self.pipeline_name / "results"
        files = os.listdir(path)

        # upload output files to new Zenodo entry
        try:
            for filename in files:
                with open(f"{path}/{filename}", "rb") as fp:
                    r = requests.put(
                        "%s/%s" % (bucket_url, filename),
                        data=fp,
                        params=params,
                    )
        except Exception as error:
            raise HTTPException(status_code=r.status_code, detail=f"Error uploading files to new Zenodo entry: {r.status_code}")

        if r.status_code == '401' or r.status_code == '400':
            raise HTTPException(status_code=r.status_code, detail=f"Error uploading dataset to zenodo with error code: {r.status_code}")

        return False
    

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
    



from datetime import datetime, timezone
from typing_extensions import Self
from copy import deepcopy
from pathlib import Path
from shutil import rmtree
import json
import httpx
from typing import (
    List,
    Optional,
    Dict, 
    Any,
    Tuple,
    Union,
    Literal
)

from api.core.exec import execute_command
from api.core.sendgird_email import send_email
from api.core.inject_deps import inject_deps_conda, inject_deps_pixi
from git import Repo
from pydantic import (
    BaseModel, 
    ConfigDict,
    Field,
    model_validator,
    PrivateAttr,
)

from api.core.git import validate_github_repo, clone_github_repo, pull_latest_pipeline, fetch_latest_commit_id
from api.models.common import PyObjectId
from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorCollection
from api.db import get_database
import os
import shutil
import requests
import logging
from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger(__name__)

database = get_database()
create_snakemake_pipeline_collection = database["create_snakemake_pipeline"]
ran_pipelines_collection = database["run_snakemake_pipeline"]
zenodo_sandbox_collection = database["zenodo_sandbox"]
JENKINS_USERNAME = os.environ["JENKINS_USERNAME"]
JENKINS_API_TOKEN = os.environ["JENKINS_API_TOKEN"]

class SnakemakePipeline(BaseModel):
    git_url: str
    pipeline_name: str
    """
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
    """
class JenkinsStageEvent(BaseModel):
    run_id: str = Field(..., min_length=1)
    pipeline_name: str = Field(..., min_length=1)
    stage: str = Field(..., min_length=1)
    status: Literal["queued", "running", "succeeded", "failed", "aborted"]
    message: Optional[str] = None
    jenkins_build_url: Optional[str] = None
    email: Optional[str] = None

class CreatePipeline(SnakemakePipeline):

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
        return False

    async def pipeline_name_exists(self, collection: AsyncIOMotorCollection) -> bool:
        """
            Verify pipeline name is not already in database.

            Returns:
                bool: True if pipeline name does exist and False otherwise
        """
        logger.info("Checking if pipeline name already exists in database")
        name = await collection.find_one({"pipeline_name": self.pipeline_name})
        if name is not None:
            logger.info("Pipeline name already exists in database")
            return True
        return False
    

    async def clone(self):
        """Clone GitHub repository.

           Calls `clone_github_repo` function from `core.git`.
        """

        await clone_github_repo(self.git_url, self.fs_path)
        

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

class RunPipeline(BaseModel):
    pipeline_name: str
    commit_id: Optional[str] = ""
    branch: Optional[str] = ""
    email: str = ""
    output_directories: List[str]
    snakefile_path: Optional[str] = Field(
        default="Snakefile",
    )
    config_file_path: Optional[str] = Field(
        default="config/config.yaml",
    )
    conda_env_file_path: Optional[str] = Field(
        default="pipeline_env.yaml",
    )
    pixi_use: bool = False
    large_machine_use: bool = False
    pipeline_run_command: Optional[str] = ""
    qc_command: Optional[str] = ""
    qc_output_directory: Optional[str] = "" 
    
    @staticmethod
    def model_to_dict(model: BaseModel) -> dict:
        if hasattr(model, "model_dump"):
            return model.model_dump(exclude_unset=True)

        return model.dict(exclude_unset=True)

    async def determine_run_id(self) -> str:
        """Determine run id for pipeline run.

        Run id is determined by concatenating pipeline name with current timestamp.

        Returns:
            str: run id for pipeline run.
        """
        run_id = 1
        most_recent_run = await ran_pipelines_collection.find_one({"pipeline_name": self.pipeline_name}, sort = [("created_at", -1)])
        if most_recent_run:
            run_id = int(most_recent_run["run_id"]) + 1
        return str(run_id)

    async def trigger_jenkins_pipeline_pixi(self, run_id: str, repo_url: str) -> None:
        """
        Trigger the Jenkins parameterized job using Basic Auth and
        application/x-www-form-urlencoded body.
        """

        if self.large_machine_use:
            endpoint = os.getenv("JENKINS_PIXI_RUN_PIPELINE_LARGE_MACHINE")
        else:
            endpoint = "PLACEHOLDER FOR SMALLER MACHINE"

        form_data = {
            "PIPELINE_NAME": self.pipeline_name,
            "RUN_ID": run_id,
            "REPO_URL": repo_url,

            "COMMIT_ID": self.commit_id or "",
            "BRANCH": self.branch or "",
            "EMAIL": self.email or "",

            "OUTPUT_DIRECTORIES_JSON": json.dumps(self.output_directories or []),

            "SNAKEFILE_PATH": self.snakefile_path or "Snakefile",
            "CONFIG_FILE_PATH": self.config_file_path or "config/config.yaml",

            "PIPELINE_RUN_COMMAND": self.pipeline_run_command or "",
            "QC_COMMAND": self.qc_command or "",
            "QC_OUTPUT_DIRECTORY": self.qc_output_directory or "",
        }
        

        logger.info("Triggering Jenkins job: %s", endpoint)
        logger.info("Jenkins form params: %s", {k: v for k, v in form_data.items() if k != "JENKINS_API_TOKEN"})

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
            response = await client.post(
                endpoint,
                data=form_data,
                auth=(JENKINS_USERNAME, JENKINS_API_TOKEN),
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )

        if response.status_code not in {200, 201, 202, 302}:
            logger.error(
                "Failed to trigger Jenkins job. status=%s body=%s",
                response.status_code,
                response.text,
            )

            raise HTTPException(
                status_code=502,
                detail={
                    "message": "Failed to trigger Jenkins job",
                    "jenkins_status_code": response.status_code,
                    "jenkins_response": response.text,
                    "jenkins_endpoint": endpoint,
                },
            )

        return {
            "jenkins_status_code": response.status_code,
            "jenkins_queue_url": response.headers.get("Location"),
            "jenkins_job_url": endpoint,
        }

        

class Zenodo(BaseModel):
    
    #forbid extra fields in json schema
    model_config = ConfigDict(extra='forbid')
    #Enforced fields for json schema
    pipeline_name: str = Field(json_schema_extra = {"not_metadata": True})
    access_right: str = Field(default = "open")
    creators: List[dict[str, str]] = Field(default_factory = lambda: [{"name": "Haibe-Kains, Benjamin",
             "orcid": '0000-0002-7684-0079',
             "type": "ContactPerson"}], json_schema_extra = {"merge": True})
    keywords: List[str] = Field(default_factory = lambda: ["ORCESTRA", "Snakemake", "Multimodal", "Canonical"], json_schema_extra = {"merge": True})
    references: List[str] = Field(default_factory = lambda: [], json_schema_extra = {"merge": True})
    dataset_type: str = Field(json_schema_extra = {"not_metadata": True})
    
    #Private fields not exposed to user in json schema
    _title: str = PrivateAttr()
    _version: str = PrivateAttr()
    _preserve_doi: bool = PrivateAttr(default = True)
    _upload_type : str = PrivateAttr(default = "dataset")
    _description: str = PrivateAttr(default = "generated by ORCESTRA. Metadata can be found on https://orcestra.ca")



    @model_validator(mode="after")
    def merge_defaults_public(self) -> Self:
        #Merge default values with user provided values for fields with 'merge' metadata
        for name, field_info in self.model_fields.items():
            
            if field_info.json_schema_extra and "merge" in field_info.json_schema_extra and field_info.json_schema_extra['merge']:
                
                default_value = field_info.get_default(call_default_factory = True)
                current_value = getattr(self, name)

                # Check if the current value is different from the original default
                if default_value and current_value != default_value:
                    # Perform the merge (append operation)
                    merged_list = default_value + current_value
                    setattr(self, name, merged_list)

        return self


    async def _validate_pipeline_exists(self) -> Union[Tuple[Dict[str, Any], bool], None]:
        """
        Validate that the pipeline exists in the database and retrieve most recent run information.
        Returns:
            Union[Tuple[Dict[str, Any], bool], None]: Most recent Existing zenodo entry and a boolean indicating whether to update existing entry,
            or None if no existing entry is found.
        Raises:
            HTTPException: If the pipeline does not exist in the database or has no recorded runs.
        """
        
        #check that pipeline exists in create pipeline collection
        create_pipeline_data = await create_snakemake_pipeline_collection.find_one({"pipeline_name": self.pipeline_name})
        if not create_pipeline_data:
            raise HTTPException(status_code=400, detail=f"Pipeline with name '{self.pipeline_name}' does not exist in database.")
        most_recent_run = await ran_pipelines_collection.find_one({"create_pipeline": create_pipeline_data['_id']}, sort = [("date", -1)])
        if not most_recent_run:
            raise HTTPException(status_code=400, detail=f"Pipeline with name '{self.pipeline_name}' has no recorded runs in database.")
        
        #update + set private attributes
        self._title = self.pipeline_name
        self._version = str(most_recent_run["version"])
        self._description = f"{self.dataset_type} {self._description}"

        #check if this version has already been uploaded to zenodo sanbox
        existing_zenodo_entry = await zenodo_sandbox_collection.find_one({"title": self._title}, sort = [("date_uploaded", -1)])
        #If it exists and version matches most recent run version, delete existing db entry and update current zenodo record
        if existing_zenodo_entry and existing_zenodo_entry["version"] == str(most_recent_run["version"]):
            logger.info("Zenodo Sandbox entry for this pipeline version already exists in database. Deleting database entry to allow re-upload but modifying existing zenodo record.")
            await zenodo_sandbox_collection.delete_one({"_id": existing_zenodo_entry["_id"]})
            return existing_zenodo_entry, True
        elif existing_zenodo_entry:
            logger.info("Entry exists for previous version of this pipeline in database. Proceeding with new version creation in same entry.")
            return existing_zenodo_entry, False
        else:
            logger.info("No existing Zenodo Sandbox entry for this pipeline found in database. Proceeding with new entry creation.")
            return None


    def _structure_metadata(self) -> dict:
        """
        Structure metadata for zenodo upload.
        """
        metadata = {}
        #Iterate through public attributes and add to metadata dictionary.
        for name, field_info in self.model_fields.items():
                
                #skip this field if marked as not_metadata
                if field_info.json_schema_extra and "not_metadata" in field_info.json_schema_extra and field_info.json_schema_extra['not_metadata']:
                    continue
                metadata[name] = getattr(self, name)
        
        #Iterate through private attributes and add to metadata dictionary with underscore removed from name.
        for name in self.__private_attributes__:
                
                value = getattr(self, name)
                metadata[name[1:]] = value

        return metadata
    
    def _create_new_zenodo_entry(self, metadata: dict) -> tuple[int, str]:
        """
        Create new zenodo entry.
        Returns:
            tuple: deposition id and bucket url for new entry.
        Raises:
            HTTPException: If there is an error creating the entry.
        """
        
        headers = {"Content-Type": "application/json"}
        params = {'access_token': os.getenv("SANDBOX_TOKEN")}

        metadata = self._structure_metadata()
        # Create Zenodo entry for new dataset
        try:
            r = requests.post('https://sandbox.zenodo.org/api/deposit/depositions',
                params=params,
                json= {"metadata": metadata},
                headers=headers
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Error uploading dataset to zenodo: {error}")

        if r.status_code == '401' or r.status_code == '400':
            raise HTTPException(status_code=r.status_code, detail=f"Error uploading dataset to zenodo with error code: {r.status_code}")
        logger.info("Zenodo entry created successfully")
        return r.json()["id"], r.json()["links"]["bucket"]
    

    def _update_existing_zenodo_entry(self, existing_entry: dict, metadata: dict) -> None:
        """
        Update existing zenodo entry with new metadata.
        Raises:
            HTTPException: If there is an error updating the entry."""
        
        data = {"metadata": metadata}
        url = "https://sandbox.zenodo.org/api/deposit/depositions/{}".format(existing_entry["deposit_id"])
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}"}

        try:
            r = requests.put(url, data=json.dumps(data), headers=headers)

        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Error updating existing zenodo entry: {error}")
        
        if r.status_code == '401' or r.status_code == '400':
            raise HTTPException(status_code=r.status_code, detail=f"Error uploading dataset to zenodo with error code: {r.status_code}")
        logger.info("Zenodo entry updated successfully")

    
    def _new_version(self, existing_entry: dict, metadata: dict) -> tuple[int, str]:

        """
        Create new version of existing zenodo entry.
        Returns:
            tuple: deposition id and bucket url for new version.
        Raises:
            HTTPException: If there is an error creating new version.
        """

        url = "https://sandbox.zenodo.org/api/deposit/depositions/{}/actions/newversion".format(existing_entry["deposit_id"])
        headers = {"Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}"}

        try:
            r = requests.post(url, headers=headers)

        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Error creating new version of existing zenodo entry: {error}")
        
        if r.status_code == '401' or r.status_code == '400':
            raise HTTPException(status_code=r.status_code, detail=f"Error uploading dataset to zenodo with error code: {r.status_code}")
        logger.info("New version of zenodo entry created successfully")

        #grab new deposition id and bucket url to upload files to new version
        latest_draft_url = r.json()["links"]["latest_draft"]
        r = requests.get(latest_draft_url, headers=headers)
        r.raise_for_status()
        new_deposit_id = r.json()["id"]
        bucket_url = r.json()["links"]["bucket"]
        #First, update the new version entry with updated metadata
        self._update_existing_zenodo_entry({"deposit_id": new_deposit_id}, metadata)
        return new_deposit_id, bucket_url

    
    async def zenodo_upload(self) -> dict[str, str]:

        """
        Upload most recent pipeline run results to Zenodo sandbox.
        Returns:
            dict: Dictionary containing sandbox URL and success status.
        Raises:
            HTTPException: If there is an error during the upload process.
        
        """
        
        #Upload pipeline results to Zenodo sandbox.
        logger.info("Checking that pipeline exists in database and retrieving most recent run information")
        operation = await self._validate_pipeline_exists()
        metadata = self._structure_metadata()
        #Determine whether to create new entry, update existing entry, or create new version
        if operation:
            existing_zenodo_entry, update_existing = operation
            if update_existing:
                logger.info("Updating existing Zenodo sandbox entry")
                self._update_existing_zenodo_entry(existing_zenodo_entry, metadata)
                deposit_id = existing_zenodo_entry["deposit_id"]
                bucket_url = existing_zenodo_entry["bucket_url"]            
            else:
                logger.info("Creating new version of existing Zenodo sandbox entry")
                deposit_id, bucket_url = self._new_version(existing_zenodo_entry, metadata)

        else:
            logger.info("Creating new Zenodo sandbox entry")
            deposit_id, bucket_url = self._create_new_zenodo_entry(metadata)

        #Upload files to zenodo sandbox only if new entry or new version
        if not operation or not update_existing:
            # get files to be uploaded to Zenodo
            path = Path.home() / "pipelines" / self.pipeline_name / "results"
            files = os.listdir(path)
            logger.info("Uploading files to Zenodo")
            headers = {"Content-Type": "application/json"}
            params = {'access_token': os.getenv("SANDBOX_TOKEN")}
            # upload output files to new Zenodo entry
            try:
                for filename in files:
                    with open(f"{path}/{filename}", "rb") as fp:
                        r = requests.put(
                            "%s/%s" % (bucket_url, filename),
                            data=fp,
                            params=params,
                        )
                logger.info("Files uploaded to Zenodo successfully")
            except Exception as error:
                raise HTTPException(status_code=r.status_code, detail=f"Error uploading files to new Zenodo entry: {r.status_code}")

            if r.status_code == '401' or r.status_code == '400':
                raise HTTPException(status_code=r.status_code, detail=f"Error uploading dataset to zenodo with error code: {r.status_code}")
        
            #publish the new zenodo entry in the sandbox envirnment to activate download links
            try:
                r = requests.post(
                    f'https://sandbox.zenodo.org/api/deposit/depositions/{deposit_id}/actions/publish',
                    params=params,
                    headers=headers
                )
                logger.info("Zenodo sandbox entry published successfully")
            except Exception as error:
                raise HTTPException(status_code=r.status_code, detail=f"Error publishing new Zenodo entry: {r.status_code}")
        
            #create download links for each file uploaded and to list
            download_links = {}
            for filename in files:
                download_links[filename] = f"https://sandbox.zenodo.org/records/{deposit_id}/files/{filename}?download=1"

        #create entry in zenodo_sandbox collection in db
        zenodo_entry = metadata
        zenodo_entry["deposit_id"] = deposit_id

        #bucket url and download links only set if new entry or new version else set to existing values in most recent zenodo entry
        zenodo_entry["download_links"] = download_links if not operation or not update_existing else existing_zenodo_entry["download_links"]
        zenodo_entry["bucket_url"] = bucket_url if not operation or not update_existing else existing_zenodo_entry["bucket_url"]

        zenodo_entry["date_uploaded"] = datetime.now(timezone.utc).isoformat()
        zenodo_entry["sandbox_url"] = f"https://sandbox.zenodo.org/records/{deposit_id}"
        zenodo_entry["dataset_type"] = self.dataset_type
        await zenodo_sandbox_collection.insert_one(zenodo_entry)
        return {"sandbox_url": zenodo_entry["sandbox_url"],
                "success": True}

    
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
    



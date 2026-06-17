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
from api.core.checksum import calculate_checksums
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
    new_release: bool = False
    
    @staticmethod
    def model_to_dict(model: BaseModel) -> dict:
        if hasattr(model, "model_dump"):
            return model.model_dump(exclude_unset=True)

        return model.dict(exclude_unset=True)

    async def determine_run_version_id(self) -> tuple[str, str]:
        """Determine run id and version for pipeline run.

        Returns:
            tuple[str, str]: run id and version for pipeline run.
        """
        run_id = 1
        most_recent_run = await ran_pipelines_collection.find_one({"pipeline_name": self.pipeline_name}, sort = [("created_at", -1)])
        if most_recent_run:
            run_id = int(most_recent_run["run_id"]) + 1
        most_recent_successful_run = await ran_pipelines_collection.find_one({"pipeline_name": self.pipeline_name, "status": "succeeded"}, sort = [("created_at", -1)])
        version = 1
        if most_recent_successful_run:
            if self.new_release:
                version = round(int(float(most_recent_successful_run["version"]) + 1), 1)
            else:
                version = round(float(float(most_recent_successful_run["version"]) + 0.1), 1)

        return str(run_id), str(version)

    async def trigger_jenkins_pipeline_pixi(self, run_id: str, repo_url: str) -> None:
        """
        Trigger the Jenkins parameterized job using Basic Auth and
        application/x-www-form-urlencoded body.
        """

        if self.pixi_use:
            endpoint = os.getenv("JENKINS_PIXI_RUN_PIPELINE")


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
            "LARGE_MACHINE_USE": str(self.large_machine_use).lower(),

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
    creators: List[dict[str, Any]] = Field(default_factory = lambda: [{"type": "personal", "name": "Haibe-Kains, Benjamin", "orcid": "0000-0002-7684-0079", "role": "contactperson", "affiliations": ["University of Toronto", "University Health Network"]}], json_schema_extra = {"merge": True, "preprocessing": True})
    subjects: List[dict[str, str]] = Field(default_factory = lambda: [])
    references: List[dict[str, str]] = Field(default_factory = lambda: [], json_schema_extra = {"merge": True})
    description: str = Field(default = "generated by ORCESTRA. Metadata can be found on https://orcestra.ca")
    resource_type: str = Field(default = "dataset")
    
    #Private fields not exposed to user in json schema
    _title: str = PrivateAttr()
    _version: str = PrivateAttr()
    _run_id: str = PrivateAttr(default = "")
    _git_url: str = PrivateAttr(default = None)
    _run_object: Any = PrivateAttr(default = None)
    


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
        most_recent_run = await ran_pipelines_collection.find_one({"create_pipeline": create_pipeline_data['_id'], "status": "succeeded"}, sort = [("created_at", -1)])
        if not most_recent_run:
            raise HTTPException(status_code=400, detail=f"Pipeline with name '{self.pipeline_name}' has no recorded successful runs in database.")
        
        #update + set private attributes
        self._title = self.pipeline_name
        self._version = str(most_recent_run["version"])
        self._run_id = str(most_recent_run["run_id"])
        self._git_url = create_pipeline_data["git_url"]
        self._run_object = most_recent_run["_id"]

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
        payload = {
            "access": {
                "record": "public",
                "files": "public",
            },
            "files": {
                "enabled": True,
            },
            "metadata": {
                "resource_type": {
                    "id": "dataset",  # or "software" if this record is software
                },
                "title": "Your record title",
                "publication_date": "2026-06-03",
                "publisher": "BHK Lab",
                "version": "1.0.0",
                "creators": [
                    {
                        "person_or_org": {
                            "type": "personal",
                            "family_name": "Haibe-Kains",
                            "given_name": "Benjamin",
                            "identifiers": [
                                {
                                    "scheme": "orcid",
                                    "identifier": "0000-0002-7684-0079",
                                }
                            ],
                        }
                    }
                ],
                "description": "generated by ORCESTRA. Metadata can be found on https://orcestra.ca",
                "subjects": [
                    {
                        "subject": "ORCESTRA"
                    },
                    {
                        "subject": "Snakemake"
                    },
                    {
                        "subject": "Multimodal"
                    },
                    {
                        "subject": "Canonical"
                    },
                    {
                        "subject": "Python"
                    },
                    {
                        "subject": "C++"
                    },
                    {
                        "subject": "C"
                    }
                ],
                "references": [
                    {
                        "reference": "Benjamin Franklin. 1950"
                    }
                ],
                "related_identifiers": [
                    {
                        "identifier": "https://github.com/MessereN/4_snakemake_download_data.git",
                        "relation_type": {
                            "id": "issupplementedby"
                        },
                        "scheme": "url",
                        "resource_type": {
                            "id": "software"
                        },
                    }
                ],
            },
        }
        """
        payload = {
            "access": {
                "record": "public",
                "files": "public"
            },
            "files": {
                "enabled": True
            }
        }
        metadata = {
                    "resource_type": {
                        "id": self.resource_type
                    },
                    "publication_date": datetime.now(timezone.utc).date().isoformat(),
                    "publisher": "BHK Lab",
                }
        #Iterate through public attributes and add to metadata dictionary.
        for name, field_info in self.model_fields.items():
                
                #skip this field if marked as not_metadata
                if (field_info.json_schema_extra and "not_metadata" in field_info.json_schema_extra and field_info.json_schema_extra['not_metadata']) or name == "resource_type":
                    continue
                
                if field_info.json_schema_extra and "preprocessing" in field_info.json_schema_extra and field_info.json_schema_extra['preprocessing']:
                    if name == "creators":
                        #convert creators to format required by zenodo api by calling _convert_creator function for each creator in list
                        metadata[name] = [self._convert_creator(creator) for creator in getattr(self, name)]
                    
                else:
                    metadata[name] = getattr(self, name)

        #Iterate through private attributes and add to metadata dictionary with underscore removed from name.
        for name in self.__private_attributes__:
                
                if name == "_run_id" or name == "_run_object":
                    continue

                if name == "_git_url":
                    metadata["related_identifiers"] = [
                        {
                            "identifier": self._git_url,
                            "relation_type": {
                                "id": "issupplementedby"
                            },
                            "scheme": "url",
                            "resource_type": {
                                "id": "software"
                            },
                        }
                    ]
                
                else:
                    value = getattr(self, name)
                    metadata[name[1:]] = value

        payload["metadata"] = metadata
        return payload
    
    def _convert_creator(self, creator: dict[str, str]) -> dict:
        """Convert creator information to format required by zenodo api.
        Args:
            creator (dict[str, str]): Creator information with keys "type", "name", and optionally "orcid"/"affiliation"/"role".
         Returns:
            dict: Creator information formatted for zenodo api.
        
        """

        name = creator["name"]

        if creator.get("type").lower() == "organizational":
            return {
                "person_or_org": {
                    "type": "organizational",
                    "name": name,
                }
            }

        if "," in name:
            family_name, given_name = [part.strip() for part in name.split(",", 1)]
        else:
            parts = name.strip().split()
            given_name = " ".join(parts[:-1])
            family_name = parts[-1] if parts else ""

        
        doc = {}
        person_or_org = {
            "type": "personal",
            "given_name": given_name,
            "family_name": family_name,
        }

        if creator.get("orcid"):
            person_or_org["identifiers"] = [
                {
                    "scheme": "orcid",
                    "identifier": creator["orcid"],
                }
            ]
        doc["person_or_org"] = person_or_org
        
        if creator.get("affiliations"):
            doc["affiliations"] = []
            for affiliation in creator["affiliations"]:
                doc["affiliations"].append({"name": affiliation})
        
        if creator.get("role"):
            doc["role"] = {"id": creator["role"]}
            

        return doc

    def _reserve_draft_doi(self, draft_id: str) -> dict[str, Any]:
        try:
            response = requests.post(
                f"https://sandbox.zenodo.org/api/records/{draft_id}/draft/pids/doi",
                headers={
                    "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}",
                    "Content-Type": "application/json",
                }
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Error reserving DOI for draft: {error}")

        response.raise_for_status()
        return response.json()

    def _delete_draft(self, draft_id: str) -> None:
        try:
            response = requests.delete(
                f"https://sandbox.zenodo.org/api/records/{draft_id}/draft",
                headers={
                    "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}",
                }
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Error deleting draft: {error}")

        if response.status_code not in (200, 204):
            raise HTTPException(status_code=response.status_code, detail=f"Error deleting draft with status code: {response.status_code}")
        logger.info("Draft deleted successfully")


    def _create_new_zenodo_entry(self, payload: dict) -> dict[str, Any]:
        """
        Create new zenodo entry.
        Returns:
            dict: Dictionary containing record information.
        Raises:
            HTTPException: If there is an error creating the entry.
        """
        
        headers = {
            "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}",
            "Content-Type": "application/json"
        }

        # Create Zenodo entry for new dataset
        try:
            r = requests.post('https://sandbox.zenodo.org/api/records',
                json=payload,
                headers=headers
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Error uploading dataset to zenodo: {error}")

        if r.status_code == '401' or r.status_code == '400' or r.status_code == '500':
            raise HTTPException(status_code=r.status_code, detail=f"Error uploading dataset to zenodo with error code: {r.status_code}")
        logger.info("Zenodo entry created successfully")
        
        create_doi = self._reserve_draft_doi(r.json()["id"])
        return create_doi
    

    def _update_existing_zenodo_entry(self, existing_entry: dict, payload: dict) -> dict[str, Any]:
        """
        Update existing zenodo entry with new metadata.
        Raises:
            HTTPException: If there is an error updating the entry."""
        
        url = "https://sandbox.zenodo.org/api/records/{}/draft".format(existing_entry["record_id"])
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}"}
        auth_headers = {
            "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}",
        }

        json_headers = {
            "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}",
            "Content-Type": "application/json",
        }

        try:
            r = requests.post(url, headers=auth_headers)

        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Error creating new editable draft for exisitng entry: {error}")
        
        if r.status_code not in (200, 201):
            r = requests.get(
                f"https://sandbox.zenodo.org/api/records/{existing_entry['record_id']}/draft",
                headers=auth_headers,
            )
        r.raise_for_status()
        draft = r.json()
        payload_v2 = payload.copy()

        try:
            r = requests.put(
                f"https://sandbox.zenodo.org/api/records/{existing_entry['record_id']}/draft",
                headers=json_headers,
                json=payload_v2,
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Error updating existing entry on Zenodo with new metadata: {error}")

        logger.info(f"Updating existing Zenodo entry with new metadata. Status code: {r.status_code}")
        logger.info(f"Response from Zenodo: {r.text}")
        r.raise_for_status()
        
        logger.info("Zenodo entry updated successfully")
        return r.json()


    def _new_version(self, existing_entry: dict, payload: dict) -> dict[str, Any]:

        """
        Create new version of existing zenodo entry.
        Returns:
            tuple: deposition id and bucket url for new version.
        Raises:
            HTTPException: If there is an error creating new version.
        """
        auth_headers = {
            "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}",
        }

        json_headers = {
            "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}",
            "Content-Type": "application/json",
        }

        try:
            r = requests.post(f"https://sandbox.zenodo.org/api/records/{existing_entry['record_id']}/versions", headers=auth_headers)
        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Error creating new version for existing entry: {error}")
        
        if r.status_code not in (200, 201):
            raise HTTPException(status_code=r.status_code, detail=f"Error creating new version for existing entry with error code: {r.status_code}")
        
        logger.info(f"New version created successfully for {existing_entry['title']}")
        record_id = r.json()["id"]

        try:
            r = requests.put(
                f"https://sandbox.zenodo.org/api/records/{record_id}/draft",
                headers=json_headers,
                json=payload,
            )
        except Exception as error:
            raise HTTPException(status_code=400, detail=f"Error updating new version with metadata: {error}")
        
        if r.status_code not in (200, 201):
            self._delete_draft(record_id)
            logger.error(f"Error updating new version with metadata. Status code: {r.status_code}")
            raise HTTPException(status_code=r.status_code, detail=f"Error updating new version with metadata with error code: {r.status_code}. Message: {r.text}")


        logger.info(f"Updated new version with metadata. Status code: {r.status_code}")
        logger.info(f"Response from Zenodo: {r.text}")
        return r.json()

    def _compute_checksums(self) -> Tuple[bool, dict]:

        """
        Compute checksums for output files to be uploaded to Zenodo and compare to initial checksums in checksum manifest.
        Returns:
            Tuple[bool, dict]: Boolean indicating whether checksums match and dictionary of initial checksums from manifest.
        """

        logger.info("Computing checksums for output files to be uploaded to Zenodo")
        path = Path(f"/mnt/gcs/nicholas-testing/pipelines/{self.pipeline_name}/{self._run_id}")
        with open(path  / 'checksum_manifest.json', 'r', encoding='utf-8') as file:
            initial_checksums = json.load(file)["files"]

        if not path.exists():
            logger.warning(f"Output path does not exist: {path}")
            return False, initial_checksums
        files = [str(p) for p in path.rglob('*') if p.is_file()]
        for file in files:
            filename = file.split('/')[-1]
            if filename == 'checksum_manifest.json':
                continue
            if filename not in initial_checksums:
                logger.warning(f"File {filename} not found in initial checksum manifest.")
                return False, initial_checksums
            calculated_checksums = calculate_checksums(Path(file))
            if calculated_checksums["sha256"] != initial_checksums[filename]["sha256"] or calculated_checksums["md5"] != initial_checksums[filename]["md5"]:
                logger.warning(f"Checksum mismatch for file {filename}. Expected sha256: {initial_checksums[filename]['sha256']}, calculated sha256: {calculated_checksums['sha256']}. Expected md5: {initial_checksums[filename]['md5']}, calculated md5: {calculated_checksums['md5']}")
                return False, initial_checksums

        logger.info("All checksums match for output files to be uploaded to Zenodo")
        return True, initial_checksums

    def _md5_zenodo_validation(self, record_files: List[dict], local_checksums: dict) -> bool:
        """
        Validate that md5 checksums for files uploaded to Zenodo match local checksums.
        Args:
            record_files (List[dict]): List of files in zenodo record with their metadata.
            local_checksums (dict): Dictionary of local checksums for files to be uploaded.
        Returns:
            bool: True if all md5 checksums match and False otherwise.
        """
        logger.info("Validating md5 checksums for files uploaded to Zenodo")
        for record_file in record_files:
            filename = record_file["key"]
            zenodo_md5 = record_file["checksum"].split("md5:")[-1]
            if filename not in local_checksums:
                logger.warning(f"File {filename} not found in local checksums for md5 validation.")
                return False
            local_md5 = local_checksums[filename]["md5"]
            if zenodo_md5 != local_md5:
                logger.warning(f"MD5 checksum mismatch for file {filename}. Local md5: {local_md5}, Zenodo md5: {zenodo_md5}")
                return False

        logger.info("All md5 checksums match for files uploaded to Zenodo")
        return True


    
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
        checksums_valid, file_metadata = self._compute_checksums()
        if not checksums_valid:
            raise HTTPException(status_code=400, detail=f"Output files for this pipeline run not found in expected location: /mnt/gcs/nicholas-testing/pipelines/{self.pipeline_name}/{self._run_id} or checksums do not match" )
        payload = self._structure_metadata()
        #Determine whether to create new entry, update existing entry, or create new version
        if operation:
            existing_zenodo_entry, update_existing = operation
            if update_existing:
                logger.info("Updating existing Zenodo sandbox entry")
                record = self._update_existing_zenodo_entry(existing_zenodo_entry, payload)           
            else:
                logger.info("Creating new version of existing Zenodo sandbox entry")
                record = self._new_version(existing_zenodo_entry, payload)

        else:
            logger.info("Creating new Zenodo sandbox entry")
            record = self._create_new_zenodo_entry(payload)

        record_id = record["id"]
        #Upload files to zenodo sandbox only if new entry or new version
        if not operation or not update_existing:
            # get files to be uploaded to Zenodo
            path_prefix = "/mnt/gcs/nicholas-testing/pipelines"
            path = Path(path_prefix) / self.pipeline_name / self._run_id
            
            files = [str(p) for p in Path(path).rglob('*') if p.is_file()]
            files.remove(str(path / 'checksum_manifest.json')) # remove checksum manifest from list of files to upload
            # upload output files to new Zenodo entry
            logger.info(f"Files to upload: {files}")
            if not files:
                logger.warning("No files found to upload to Zenodo")
                self._delete_draft(record_id)
                raise HTTPException(status_code=400, detail="No files found to upload to Zenodo")
            try:
                response = requests.post(
                    f"https://sandbox.zenodo.org/api/records/{record_id}/draft/files",
                    headers={
                        "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}",
                        "Content-Type": "application/json",
                    },
                    json=[
                        {"key": filename.split('/')[-1]}
                        for filename in files
                    ],
                )
            except Exception as error:
                raise HTTPException(status_code=400, detail=f"Error uploading filenames to zenodo: {error}")

            try:
                logger.info("Uploading files to Zenodo")
                for filename in files:
                    path = Path(filename)
                    with path.open("rb") as fp:
                        response = requests.put(
                            f"https://sandbox.zenodo.org/api/records/{record_id}/draft/files/{filename.split('/')[-1]}/content",
                            headers={
                                "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}",
                                "Content-Type": "application/octet-stream",
                            },
                            data=fp,
                        )
                    response = requests.post(
                        f"https://sandbox.zenodo.org/api/records/{record_id}/draft/files/{filename.split('/')[-1]}/commit",
                        headers={
                            "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}",
                        },
                    )

                logger.info("Files uploaded to Zenodo successfully")
            except Exception as error:
                raise HTTPException(status_code= '401', detail=f"Error uploading files to new Zenodo entry: {error}")

            if response.status_code == '401' or response.status_code == '400':
                raise HTTPException(status_code=response.status_code, detail=f"Error uploading dataset to zenodo with error code: {response.status_code}")

            #create download links for each file uploaded and to list
            download_links = {}
            for filename in files:
                download_links[filename.split('/')[-1]] = f"https://sandbox.zenodo.org/records/{record_id}/files/{filename.split('/')[-1]}?download=1"



        #publish the zenodo entry in the sandbox environment to activate download links
        try:
            response = requests.post(
                f"https://sandbox.zenodo.org/api/records/{record_id}/draft/actions/publish",
                headers={
                    "Authorization": f"Bearer {os.getenv('SANDBOX_TOKEN')}",
                },
            )
            logger.info("Zenodo sandbox entry published successfully")
        except Exception as error:
            logger.error(f"Error publishing Zenodo entry: {error}")
            raise HTTPException(status_code=response.status_code, detail=f"Error publishing new Zenodo entry: {response.status_code}")

        #check zenodo checksum md5 with local file checksums and if mismatch, delete zenodo entry and raise error
        if not self._md5_zenodo_validation(response.json()["files"], file_metadata):
            self._delete_draft(record_id)
            logger.error("MD5 checksum validation failed for files uploaded to Zenodo. Deleting Zenodo entry.")
            raise HTTPException(status_code=400, detail="MD5 checksum validation failed for files uploaded to Zenodo. The Zenodo entry has been deleted. Please try uploading again.")
        
        #create entry in zenodo_sandbox collection in db
        zenodo_entry = payload['metadata']
        zenodo_entry["file_metadata"] = file_metadata
        zenodo_entry["run_object_id"] = self._run_object
        zenodo_entry["record_id"] = record_id 
        zenodo_entry["doi"] = record.get("doi", "") if record.get("doi") else response.json().get("doi", "") # use doi from record
        zenodo_entry["parent_id"] = (
            record.get("conceptrecid")
            or record.get("metadata")
                .get("relations", {})
                .get("version", [{}])[0]
                .get("parent", {})
                .get("pid_value")
            )

        #bucket url and download links only set if new entry or new version else set to existing values in most recent zenodo entry
        zenodo_entry["download_links"] = download_links if not operation or not update_existing else existing_zenodo_entry["download_links"]

        zenodo_entry["date_uploaded"] = datetime.now(timezone.utc).isoformat()
        zenodo_entry["sandbox_url"] = f"https://sandbox.zenodo.org/records/{zenodo_entry['record_id']}"
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
    



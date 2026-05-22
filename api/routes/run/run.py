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
ran_pipelines_collection = database["run_snakemake_pipeline"]
async def run_pipeline(data: RunPipeline) -> dict:
    try:
        pipeline_name = data.pipeline_name

        pipeline_data = await create_snakemake_pipeline_collection.find_one(
            {"pipeline_name": pipeline_name},
            {"_id": 0},
        )

        if pipeline_data is None:
            raise HTTPException(status_code=404, detail="Pipeline not found")

        logger.info("%s pipeline found in database", pipeline_name)

        current_active_pipeline = await ran_pipelines_collection.find_one(
            {
                "status": {
                    "$in": ["queued", "running"]
                }
            },
            {"_id": 0},
        )

        if current_active_pipeline is not None:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"The {current_active_pipeline['pipeline_name']} pipeline "
                    f"is currently {current_active_pipeline['status']}. "
                    "Please wait until it finishes."
                ),
            )

        repo_url = pipeline_data.get("git_url")

        if not repo_url:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Pipeline record is missing repo_url. "
                    "repo_url is required to trigger the Jenkins job."
                ),
            )

        request_data = RunPipeline.model_to_dict(data)

        # Remove fields that are stored in Mongo but are not part of RunPipeline.
        pipeline_data_for_model = dict(pipeline_data)
        pipeline_data_for_model.pop("repo_url", None)

        # Request values override stored values.
        pipeline_dict = {
            **pipeline_data_for_model,
            **request_data,
        }

        pipeline = RunPipeline(**pipeline_dict)

        logger.info("Pipeline run process started for: %s", pipeline.pipeline_name)

        run_id = await pipeline.determine_run_id()

        if pipeline.pixi_use:
            if not pipeline.pipeline_run_command or pipeline.pipeline_run_command.strip() == "":
                pipeline.pipeline_run_command = "pixi run snakemake --cores 4"

            now = datetime.now(timezone.utc)

            queued_record = {
                "run_id": run_id,
                "pipeline_name": pipeline.pipeline_name,
                "status": "queued",
                "current_stage": "Queued",
                "message": "Pipeline submitted to Jenkins queue",

                "jenkins_build_url": None,
                "jenkins_queue_url": None,

                "repo_url": repo_url,
                "commit_id": pipeline.commit_id,
                "branch": pipeline.branch,
                "email": pipeline.email,

                "output_directories": pipeline.output_directories,
                "snakefile_path": pipeline.snakefile_path,
                "config_file_path": pipeline.config_file_path,

                "pixi_use": pipeline.pixi_use,
                "large_machine_use": pipeline.large_machine_use,
                "pipeline_run_command": pipeline.pipeline_run_command,
                "qc_command": pipeline.qc_command,
                "qc_output_directory": pipeline.qc_output_directory,

                "updated_at": now,
            }

        await ran_pipelines_collection.update_one(
            {
                "run_id": run_id,
                "pipeline_name": pipeline.pipeline_name,
            },
            {
                "$setOnInsert": {
                    "created_at": now,
                },
                "$set": queued_record,
            },
            upsert=True,
        )

        try:
            jenkins_result = await pipeline.trigger_jenkins_pipeline_pixi(
                run_id=run_id,
                repo_url=repo_url,
            )

        except Exception as error:
            await ran_pipelines_collection.update_one(
                {
                    "run_id": run_id,
                    "pipeline_name": pipeline.pipeline_name,
                },
                {
                    "$set": {
                        "status": "failed",
                        "current_stage": "Jenkins Submission",
                        "message": f"Failed to submit pipeline to Jenkins: {str(error)}",
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )

            raise

        await ran_pipelines_collection.update_one(
            {
                "run_id": run_id,
                "pipeline_name": pipeline.pipeline_name,
            },
            {
                "$set": {
                    "jenkins_queue_url": jenkins_result.get("jenkins_queue_url"),
                    "message": "Pipeline queued in Jenkins",
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

        return {
            **(pipeline.model_dump() if hasattr(pipeline, "model_dump") else pipeline.dict()),
            "run_id": run_id,
            "repo_url": repo_url,
            "status": "queued",
            **jenkins_result,
        }

    except HTTPException:
        raise

    except Exception as error:
        logger.exception("Failed to run pipeline")
        raise HTTPException(status_code=500, detail=str(error))



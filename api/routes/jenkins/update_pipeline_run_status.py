from api.models.Pipeline import JenkinsStageEvent
from fastapi import Depends, HTTPException
from api.db import get_database
from datetime import datetime, timezone
import logging
import threading
import asyncio
import os
from typing import Dict
from api.core.sendgird_email import send_email

logger = logging.getLogger(__name__)
database = get_database()
ran_snakemake_pipeline_collection = database["run_snakemake_pipeline"]
async def update_pipeline_run_status(data: JenkinsStageEvent) -> Dict:
    data = JenkinsStageEvent(**data)
    logger.info("Received Jenkins stage event: %s", data)
    now = datetime.now(timezone.utc)

    filter_doc = {
        "run_id": data.run_id,
        "pipeline_name": data.pipeline_name,
    }

    update_doc = {
        "$set": {
            "run_id": data.run_id,
            "pipeline_name": data.pipeline_name,
            "status": data.status,
            "current_stage": data.stage,
            "message": data.message,
            "jenkins_build_url": data.jenkins_build_url,
            "updated_at": now,
        },
        "$setOnInsert": {
            "created_at": now,
        },
    }

    result = await ran_snakemake_pipeline_collection.update_one(
        filter_doc,
        update_doc,
        upsert=True,
    )
    logger.info("Updated pipeline run status in database: %s", result.raw_result)

    if data.status.lower() in ["failed", "aborted", "succeded"] and data.email:
        logger.info(f"Sending email notification for pipeline {data.pipeline_name} run status update. Status: {data.status} to {data.email}")
        await send_email(
            to_email=data.email,
            pipeline_name=data.pipeline_name,
            run_status=data.status,
            error_output=f"Run ID: {data.run_id} - {data.message}",
        )
    return {
        "ok": True,
        "run_id": data.run_id,
        "pipeline_name": data.pipeline_name,
        "status": data.status,
        "current_stage": data.stage,
        "matched_count": result.matched_count,
        "modified_count": result.modified_count,
        "upserted": result.upserted_id is not None,
    }
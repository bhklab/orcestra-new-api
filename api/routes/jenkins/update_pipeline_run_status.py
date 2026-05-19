from api.models.Pipeline import JenkinsStageEvent
from fastapi import Depends, HTTPException
from api.db import get_database
from datetime import datetime, timezone
import logging
import threading
import asyncio
import os
from typing import Dict

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
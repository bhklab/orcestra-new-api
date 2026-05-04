from __future__ import annotations
import argparse
from pathlib import Path
import sys
import yaml
from api.core.exec import execute_command
import shlex


REQUIRED_CHANNELS = ["conda-forge", "bioconda"]

# Install these via conda (bioconda)
CONDA_INJECT = [
    "snakemake-interface-common",
    "snakemake-interface-executor-plugins",
    "snakemake-interface-logger-plugins",
    "snakemake-interface-report-plugins",
    "snakemake-interface-scheduler-plugins",
    "snakemake-interface-storage-plugins",
    "snakemake-storage-plugin-gcs"
]

# Install these via pip (PyPI)
PIP_INJECT = [
    "git+https://github.com/MessereN/snakemake-executor-plugin-kubernetes.git@feature/podtolerationsandselectors",
    "snakemake-executor-plugin-kubernetes @ git+https://github.com/MessereN/snakemake-executor-plugin-kubernetes.git@feature/podtolerationsandselectors"

]

def inject_deps_conda(pipeline: SnakemakePipeline) -> str:
    """Injects necessary dependencies into the conda environment for kubernetes execution."""
async def inject_deps_pixi(pipeline: SnakemakePipeline) -> None:
    """Injects necessary dependencies into the pixi environment for kubernetes execution."""
    cwd = pipeline.fs_path
    command = f"pixi add {' '.join(CONDA_INJECT)} && pixi add --pypi {shlex.quote(PIP_INJECT[1])}"
    
    try:
        exit_status, stdout, stderr = await execute_command(command, cwd)
        if exit_status != 0:
            raise Exception(f"Error injecting dependencies into pixi environment: {stderr}")
    except Exception as error:
        raise error
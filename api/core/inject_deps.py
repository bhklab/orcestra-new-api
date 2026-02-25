from __future__ import annotations
import argparse
from pathlib import Path
import sys
import yaml

REQUIRED_CHANNELS = ["conda-forge", "bioconda"]

# Install these via conda (bioconda)
CONDA_INJECT = [
    "snakemake-interface-common",
    "snakemake-interface-executor-plugins",
    "snakemake-interface-logger-plugins",
    "snakemake-interface-report-plugins",
    "snakemake-interface-scheduler-plugins",
    "snakemake-interface-storage-plugins",
]

# Install these via pip (PyPI)
PIP_INJECT = [
    "git+https://github.com/MessereN/snakemake-executor-plugin-kubernetes.git@feature/podtolerationsandselectors",
    "snakemake-storage-plugin-gcs",
]

REMOVE_DEPS = 'snakemake'

def inject_deps(pipeline: SnakemakePipeline) -> str:
    """Injects necessary dependencies into the conda environment for kubernetes execution."""
    # Load existing conda environment YAML
    with open(pipeline.fs_path / pipeline.conda_env_file_path, 'r') as file:
        env_data = yaml.safe_load(file)

    # Ensure required channels are present
    channels = env_data.get('channels', [])
    for channel in REQUIRED_CHANNELS:
        if channel not in channels:
            channels.append(channel)
    env_data['channels'] = channels

    # Inject conda dependencies
    dependencies = env_data.get('dependencies', [])
    for dep in CONDA_INJECT:
        if dep not in dependencies:
            dependencies.append(dep)
    
    # Ensure pip is present as conda dependency
    if "pip" not in dependencies:
        dependencies.append("pip")

    # Inject pip dependencies under a pip section
    pip_blocks = [dep for dep in dependencies if isinstance(dep, dict) and 'pip' in dep]

    if pip_blocks:
        pip_list = pip_blocks[0].get('pip') or []
        for dep in PIP_INJECT:
            if dep not in pip_list:
                pip_list.append(dep)
        pip_blocks[0]['pip'] = pip_list
    else:
        dependencies.append({'pip': list(PIP_INJECT)})

    env_data['dependencies'] = dependencies

    kubs_conda_path = f"{pipeline.pipeline_name}_kubs_exec.yaml"

    # Save the updated conda environment YAML
    with open(str(pipeline.fs_path) + "/" + kubs_conda_path, 'w') as file:
        yaml.dump(env_data, file)

    return kubs_conda_path

def remove_snakemake_deps(pipeline: SnakemakePipeline) -> str:
    """Removes snakemake-interface dependencies from the conda environment after kubernetes execution."""
    with open(pipeline.fs_path / pipeline.conda_env_file_path, 'r') as file:
        env_data = yaml.safe_load(file)

    dependencies = env_data.get('dependencies', [])
    dependencies = [dep for dep in dependencies if not (isinstance(dep, str) and REMOVE_DEPS in dep)]
    env_data['dependencies'] = dependencies

    default_kubs_conda_path = f"{pipeline.pipeline_name}_kubs_default.yaml"

    #remove pip dependencies as well
    pip_deps = [dep for dep in dependencies if isinstance(dep, dict) and 'pip' in dep]
    if pip_deps:
        pip_deps[0]['pip'] = [dep for dep in pip_deps[0]['pip'] if not any(REMOVE_DEPS in dep for dep in PIP_INJECT)]

    with open(str(pipeline.fs_path) +  "/" + default_kubs_conda_path, 'w') as file:
        yaml.dump(env_data, file)

    return default_kubs_conda_path

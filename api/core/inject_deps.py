from __future__ import annotations
import argparse
from pathlib import Path
import sys
import yaml

REQUIRED_CHANNELS = ["conda-forge", "bioconda"]

# Install these via conda (bioconda)
CONDA_INJECT = [
    "snakemake-minimal",
    "snakemake-interface-common",
    "snakemake-interface-executor-plugins",
    "snakemake-interface-logger-plugins",
    "snakemake-interface-report-plugins",
    "snakemake-interface-scheduler-plugins",
    "snakemake-interface-storage-plugins",
]

# Install these via pip (PyPI)
PIP_INJECT = [
    "snakemake-executor-plugin-kubernetes",
    "snakemake-storage-plugin-gcs",
]


def inject_deps(conda_env_file_path: str) -> None:
    """Injects necessary dependencies into the conda environment for kubernetes execution."""
    # Load existing conda environment YAML
    with open(conda_env_file_path, 'r') as file:
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
    
    # Inject pip dependencies under a pip section
    pip_deps = [dep for dep in dependencies if isinstance(dep, dict) and 'pip' in dep]
    if pip_deps:
        pip_deps[0]['pip'].extend([dep for dep in PIP_INJECT if dep not in pip_deps[0]['pip']])
    else:
        dependencies.append({'pip': PIP_INJECT})

    env_data['dependencies'] = dependencies

    # Save the updated conda environment YAML
    with open(conda_env_file_path, 'w') as file:
        yaml.dump(env_data, file)
"""This module contains functions for executing commands in a shell."""

import asyncio
import subprocess
from typing import List, Tuple, Union

async def execute_command(command: Union[str, List[str]], cwd: str) -> Tuple[int, str, str]:
    """This function executes a command in a shell asynchronously.

    It captures both the standard output and the standard error of the command
    and returns them.
    """
    if isinstance(command, list):
        command = ' '.join(command)

    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd
    )
    stdout, stderr = await process.communicate()
    exit_status = process.returncode
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    if exit_status != 0:
        stderr = f"Command '{command}' failed with exit status {exit_status}\n" \
            f"Standard output:\n{stdout}\n" \
            f"Standard error:\n{stderr}"
    return exit_status, stdout, stderr


async def main() -> None:
    """Main function for testing the module."""
    stdout, stderror = await execute_command('snakemake --version')

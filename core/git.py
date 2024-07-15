from pathlib import Path

import aiohttp
from aiohttp import InvalidURL
from git import GitCommandError, Repo
from fastapi import HTTPException


async def validate_github_repo(url: str) -> bool:
    """This function validates a GitHub repository.

    Args:
      url (str): The URL of the GitHub repository to validate.

    Returns:
      bool: True if the repository is valid (status code 200), False otherwise.

    """
    if not url.endswith(".git"):
        raise HTTPException(status_code=400, detail="Invalid GitHub URL")
    elif not url.startswith("https://github.com"):
        raise HTTPException(status_code=400, detail="Invalid GitHub URL")
    
    link = url.split("/")
    owner = link[-2]
    repo = link[-1][:-4]
    
    api_url = f"https://api.github.com/repos/{owner}/{repo}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                if response.status == 200:
                    return True
                elif response.status == 404:
                    raise HTTPException(status_code=404, detail="Repository not found")
    except InvalidURL:
        raise HTTPException(status_code=400, detail="Invalid GitHub URL")
    except Exception as error:
        raise HTTPException(status_code=400, detail=str(error))
        
async def clone_github_repo(url: str, dest: Path) -> Repo:
    """Clone a GitHub repository.

    Args:
      url (str): The URL of the GitHub repository to clone.
      dest (Path): The destination path where the repository will be cloned.

    Returns:
      Repo: The cloned repository object.

    Raises:
      Exception: If the destination directory already exists.
      GitCommandError: If there is an error while cloning the repository.
      Exception: If there is an unexpected error during the cloning process.
    """
    # check if folder already exists
    if not dest.exists():
        dest.mkdir(parents=True)
    else:
        raise HTTPException(status_code=400, detail=f"Directory: {dest} already exists. Change your pipeline name or remove from VM")
    
    try:
        return Repo.clone_from(url, dest)
    except GitCommandError as git_error:
        raise HTTPException(status_code=400, detail=f"The repository was not clonable due to a git command error: {git_error}")
    except Exception as error:
        raise HTTPException(status_code=400, detail=f"The repository was not clonable: {error}")
    

async def pull_github_repo(repo: Repo) -> Repo:
    """Pull changes from a GitHub repository.

    Args:
      repo (Repo): The repository object to pull changes from.

    Returns:
      Repo: The repository object with the pulled changes.

    Raises:
      GitCommandError: If there is an error while pulling the changes.
    """
    try:
        repo.remotes.origin.pull()
        return repo
    except GitCommandError as git_error:
        raise git_error

async def pull_latest_pipeline(dest: Path) -> Repo:
    """Pull the latest changes from a GitHub repository.

    Args:
      dest (Path): The destination path where the repository is cloned.

    Returns:
      Repo: The repository object with the pulled changes.

    Raises:
      GitCommandError: If there is an error while pulling the changes.
    """
    try:
        repo = Repo(dest)
        return await pull_github_repo(repo)
    except GitCommandError as git_error:
        raise git_error


from pathlib import Path

import aiohttp
from aiohttp import InvalidURL
from git import GitCommandError, Repo
from fastapi import HTTPException

#Needs to be addressed in order to check valid web page but not a valid github respository (404 respository)
async def validate_github_repo(url: str) -> bool:
    """This function validates a GitHub repository.

    Args:
      url (str): The URL of the GitHub repository to validate.

    Returns:
      bool: True if the repository is valid (status code 200), False otherwise.

    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return True
    except InvalidURL:
        raise HTTPException(status_code=400, detail="Invalid GitHub URL")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
        
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
        raise HTTPException(status_code=400, detail=f"Directory: {dest} already exists. Remove from VM or change pipeline name")
    
    try:
        return Repo.clone_from(url, dest)
    except GitCommandError as git_error:
        raise git_error
    except Exception as e:
        raise e

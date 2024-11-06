import os
import git
import shutil
import re
from typing import Dict, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from .logger import logger

class GitSyncError(Exception):
    """Custom exception for git sync operations"""
    pass

class GitRepositorySync:
    def __init__(self, base_backup_dir: str = 'temp_backups', max_workers: int = 4):
        """
        Initialize GitRepositorySync
        
        Args:
            base_backup_dir (str): Base directory for repository backups
            max_workers (int): Maximum number of concurrent sync operations
        """
        self.base_backup_dir = base_backup_dir
        self.max_workers = max_workers
        os.makedirs(self.base_backup_dir, exist_ok=True)
        logger.info(f"Initialized GitRepositorySync with backup dir: {base_backup_dir}")

    def validate_repo_url(self, url: str) -> bool:
        """
        Validate repository URL format
        
        Args:
            url (str): Repository URL to validate
        
        Returns:
            bool: True if URL is valid, False otherwise
        """
        pattern = r'^https?://github\.com/[\w-]+/[\w.-]+(?:\.git)?$'
        return bool(re.match(pattern, url))

    def clone_repository(self, repo_url: str, local_path: str) -> bool:
        """
        Clone a repository to the specified local path
        
        Args:
            repo_url (str): URL of the repository to clone
            local_path (str): Local path to clone the repository
        
        Returns:
            bool: True if successful, False otherwise
        
        Raises:
            GitSyncError: If URL validation fails or cloning fails
        """
        try:
            if not self.validate_repo_url(repo_url):
                raise GitSyncError(f"Invalid repository URL: {repo_url}")

            logger.info(f"Cloning repository from {repo_url} to {local_path}")

            # Remove existing directory if it exists
            if os.path.exists(local_path):
                shutil.rmtree(local_path)

            # Ensure the local path exists
            os.makedirs(local_path, exist_ok=True)
            
            # Clone with SSL verification enabled
            git.Repo.clone_from(
                repo_url,
                local_path,
                verify=True,
                depth=1  # Shallow clone for better performance
            )
            
            logger.info(f"Successfully cloned repository to {local_path}")
            return True

        except Exception as e:
            logger.error(f"Error cloning repository {repo_url}: {e}")
            # Clean up on failure
            if os.path.exists(local_path):
                shutil.rmtree(local_path)
            raise GitSyncError(f"Failed to clone repository: {e}")

    def sync_repository(self, local_path: str) -> Dict[str, str]:
        """
        Synchronize a local repository
        
        Args:
            local_path (str): Local path of the repository
        
        Returns:
            Dict with sync details
        
        Raises:
            GitSyncError: If sync operation fails
        """
        try:
            logger.info(f"Starting sync for repository at {local_path}")
            
            # Open the repository
            repo = git.Repo(local_path)
            
            # Fetch all remotes
            origin = repo.remotes.origin
            origin.fetch()
            
            # Pull latest changes
            origin.pull()
            
            result = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'branch': repo.active_branch.name,
                'latest_commit': str(repo.head.commit)
            }
            
            logger.info(f"Successfully synced repository at {local_path}")
            return result

        except Exception as e:
            error_msg = f"Error syncing repository at {local_path}: {e}"
            logger.error(error_msg)
            return {
                'status': 'failed',
                'error': str(e)
            }

    def backup_repository(self, local_path: str, repo_name: str) -> str:
        """
        Create a backup of a repository
        
        Args:
            local_path (str): Local path of the repository
            repo_name (str): Name of the repository
        
        Returns:
            str: Path to the backup
        
        Raises:
            GitSyncError: If backup operation fails
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(self.base_backup_dir, f"{repo_name}_{timestamp}")
            
            logger.info(f"Creating backup of {repo_name} at {backup_path}")
            
            # Copy the entire repository
            shutil.copytree(local_path, backup_path)
            
            logger.info(f"Successfully created backup at {backup_path}")
            return backup_path

        except Exception as e:
            error_msg = f"Error creating backup for {repo_name}: {e}"
            logger.error(error_msg)
            raise GitSyncError(error_msg)

    def sync_multiple_repositories(self, repositories: list) -> Dict[str, Dict]:
        """
        Synchronize multiple repositories in parallel
        
        Args:
            repositories: List of repository configurations
        
        Returns:
            Dict containing sync results for each repository
        """
        results = {}
        
        def sync_repo(repo):
            try:
                sync_result = self.sync_repository(repo['local_path'])
                if sync_result['status'] == 'success':
                    backup_path = self.backup_repository(repo['local_path'], repo['name'])
                    sync_result['backup_path'] = backup_path
                return repo['name'], sync_result
            except Exception as e:
                logger.error(f"Error processing repository {repo['name']}: {e}")
                return repo['name'], {'status': 'failed', 'error': str(e)}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_repo = {
                executor.submit(sync_repo, repo): repo 
                for repo in repositories
            }
            
            for future in future_to_repo:
                try:
                    name, result = future.result()
                    results[name] = result
                except Exception as e:
                    repo = future_to_repo[future]
                    results[repo['name']] = {
                        'status': 'failed',
                        'error': str(e)
                    }

        return results

    def cleanup_old_backups(self, max_backups: int = 5) -> None:
        """
        Clean up old backups keeping only the most recent ones
        
        Args:
            max_backups: Maximum number of backups to keep per repository
        """
        try:
            # Group backups by repository
            backup_dirs = {}
            for item in os.listdir(self.base_backup_dir):
                if os.path.isdir(os.path.join(self.base_backup_dir, item)):
                    repo_name = item.split('_')[0]
                    if repo_name not in backup_dirs:
                        backup_dirs[repo_name] = []
                    backup_dirs[repo_name].append(item)

            # Keep only the most recent backups for each repository
            for repo_name, backups in backup_dirs.items():
                if len(backups) > max_backups:
                    # Sort by timestamp (newest first)
                    backups.sort(reverse=True)
                    # Remove old backups
                    for backup in backups[max_backups:]:
                        backup_path = os.path.join(self.base_backup_dir, backup)
                        shutil.rmtree(backup_path)
                        logger.info(f"Removed old backup: {backup}")

        except Exception as e:
            logger.error(f"Error cleaning up old backups: {e}")
            raise GitSyncError(f"Failed to clean up old backups: {e}")

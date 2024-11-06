import os
import git
import shutil
import re
import time
from typing import Dict, Optional, List, Callable
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from .logger import logger

class GitSyncError(Exception):
    """Custom exception for git sync operations"""
    pass

class GitRepositorySync:
    def __init__(self, base_backup_dir: str = 'temp_backups', max_workers: int = 4, max_retries: int = 3):
        """
        Initialize GitRepositorySync
        
        Args:
            base_backup_dir (str): Base directory for repository backups
            max_workers (int): Maximum number of concurrent sync operations
            max_retries (int): Maximum number of retry attempts for git operations
        """
        self.base_backup_dir = base_backup_dir
        self.max_workers = max_workers
        self.max_retries = max_retries
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
        try:
            parsed = urlparse(url)
            # Check for valid protocol
            if parsed.scheme not in ['http', 'https']:
                return False
            
            # Check for valid git hosting domains
            valid_domains = [
                'github.com',
                'gitlab.com',
                'bitbucket.org',
                'dev.azure.com'
            ]
            
            if not any(domain in parsed.netloc for domain in valid_domains):
                return False
            
            # Check for username/repository format
            path_parts = parsed.path.strip('/').split('/')
            return len(path_parts) >= 2  # At least username/repo
            
        except Exception:
            return False

    def retry_operation(self, operation: Callable, *args, **kwargs) -> Any:
        """
        Retry an operation with exponential backoff
        
        Args:
            operation: Function to retry
            *args: Arguments for the function
            **kwargs: Keyword arguments for the function
        
        Returns:
            Result of the operation
        
        Raises:
            GitSyncError: If all retries fail
        """
        last_error = None
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    wait_time = (2 ** attempt) * 1  # Exponential backoff
                    logger.warning(f"Operation failed, retrying in {wait_time}s: {str(e)}")
                    time.sleep(wait_time)
                    continue
                break
        
        raise GitSyncError(f"Operation failed after {self.max_retries} attempts: {str(last_error)}")

    def clone_repository(self, repo_url: str, local_path: str, progress_callback: Optional[Callable] = None) -> bool:
        """
        Clone a repository to the specified local path
        
        Args:
            repo_url (str): URL of the repository to clone
            local_path (str): Local path to clone the repository
            progress_callback: Optional callback for progress updates
        
        Returns:
            bool: True if successful, False otherwise
        
        Raises:
            GitSyncError: If URL validation fails or cloning fails
        """
        try:
            if not self.validate_repo_url(repo_url):
                raise GitSyncError(f"Invalid repository URL: {repo_url}")

            logger.info(f"Cloning repository from {repo_url} to {local_path}")

            if progress_callback:
                progress_callback("Starting clone operation", 0)

            # Remove existing directory if it exists
            if os.path.exists(local_path):
                shutil.rmtree(local_path)

            # Ensure the local path exists
            os.makedirs(local_path, exist_ok=True)
            
            def clone_with_progress():
                return git.Repo.clone_from(
                    repo_url,
                    local_path,
                    verify=True,
                    progress=progress_callback if progress_callback else None
                )
            
            # Clone with retry mechanism
            self.retry_operation(clone_with_progress)
            
            if progress_callback:
                progress_callback("Clone completed successfully", 100)
            
            logger.info(f"Successfully cloned repository to {local_path}")
            return True

        except Exception as e:
            logger.error(f"Error cloning repository {repo_url}: {e}")
            # Clean up on failure
            if os.path.exists(local_path):
                shutil.rmtree(local_path)
            raise GitSyncError(f"Failed to clone repository: {e}")

    def sync_repository(self, local_path: str, progress_callback: Optional[Callable] = None) -> Dict[str, str]:
        """
        Synchronize a local repository
        
        Args:
            local_path (str): Local path of the repository
            progress_callback: Optional callback for progress updates
        
        Returns:
            Dict with sync details
        
        Raises:
            GitSyncError: If sync operation fails
        """
        try:
            logger.info(f"Starting sync for repository at {local_path}")
            
            if progress_callback:
                progress_callback("Starting sync operation", 0)
            
            # Open the repository
            repo = git.Repo(local_path)
            
            def fetch_and_pull():
                # Fetch all remotes
                origin = repo.remotes.origin
                origin.fetch()
                if progress_callback:
                    progress_callback("Fetch completed, starting pull", 50)
                
                # Pull latest changes
                origin.pull()
                if progress_callback:
                    progress_callback("Pull completed", 100)
            
            # Sync with retry mechanism
            self.retry_operation(fetch_and_pull)
            
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

    def backup_repository(self, local_path: str, repo_name: str, progress_callback: Optional[Callable] = None) -> str:
        """
        Create a backup of a repository
        
        Args:
            local_path (str): Local path of the repository
            repo_name (str): Name of the repository
            progress_callback: Optional callback for progress updates
        
        Returns:
            str: Path to the backup
        
        Raises:
            GitSyncError: If backup operation fails
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(self.base_backup_dir, f"{repo_name}_{timestamp}")
            
            logger.info(f"Creating backup of {repo_name} at {backup_path}")
            
            if progress_callback:
                progress_callback("Starting backup operation", 0)
            
            def copy_with_progress():
                shutil.copytree(local_path, backup_path)
                if progress_callback:
                    progress_callback("Backup completed", 100)
            
            # Backup with retry mechanism
            self.retry_operation(copy_with_progress)
            
            logger.info(f"Successfully created backup at {backup_path}")
            return backup_path

        except Exception as e:
            error_msg = f"Error creating backup for {repo_name}: {e}"
            logger.error(error_msg)
            raise GitSyncError(error_msg)

    def sync_multiple_repositories(self, repositories: list, progress_callback: Optional[Callable] = None) -> Dict[str, Dict]:
        """
        Synchronize multiple repositories in parallel
        
        Args:
            repositories: List of repository configurations
            progress_callback: Optional callback for progress updates
        
        Returns:
            Dict containing sync results for each repository
        """
        results = {}
        total_repos = len(repositories)
        completed = 0
        
        def sync_repo(repo):
            nonlocal completed
            try:
                if progress_callback:
                    progress_callback(f"Processing {repo['name']}", (completed * 100) // total_repos)
                
                sync_result = self.sync_repository(repo['local_path'])
                if sync_result['status'] == 'success':
                    backup_path = self.backup_repository(repo['local_path'], repo['name'])
                    sync_result['backup_path'] = backup_path
                
                completed += 1
                if progress_callback:
                    progress_callback(f"Completed {repo['name']}", (completed * 100) // total_repos)
                
                return repo['name'], sync_result
            except Exception as e:
                logger.error(f"Error processing repository {repo['name']}: {e}")
                completed += 1
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

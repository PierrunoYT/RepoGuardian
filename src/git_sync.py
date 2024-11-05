import os
import git
from typing import List, Dict
from datetime import datetime

class GitRepositorySync:
    def __init__(self, base_backup_dir: str = 'temp_backups'):
        """
        Initialize GitRepositorySync
        
        Args:
            base_backup_dir (str): Base directory for repository backups
        """
        self.base_backup_dir = base_backup_dir
        os.makedirs(self.base_backup_dir, exist_ok=True)

    def clone_repository(self, repo_url: str, local_path: str) -> bool:
        """
        Clone a repository to the specified local path
        
        Args:
            repo_url (str): URL of the repository to clone
            local_path (str): Local path to clone the repository
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Remove existing directory if it exists to prevent partial clones
            if os.path.exists(local_path):
                import shutil
                shutil.rmtree(local_path)

            # Ensure the local path exists
            os.makedirs(local_path, exist_ok=True)
            
            # Clone the repository
            git.Repo.clone_from(repo_url, local_path)
            return True
        except Exception as e:
            print(f"Error cloning repository {repo_url}: {e}")
            # Remove the local path if clone fails
            if os.path.exists(local_path):
                import shutil
                shutil.rmtree(local_path)
            return False

    def sync_repository(self, local_path: str) -> Dict[str, str]:
        """
        Synchronize a local repository
        
        Args:
            local_path (str): Local path of the repository
        
        Returns:
            Dict with sync details
        """
        try:
            # Open the repository
            repo = git.Repo(local_path)
            
            # Fetch all remotes
            origin = repo.remotes.origin
            origin.fetch()
            
            # Pull latest changes
            origin.pull()
            
            return {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'branch': repo.active_branch.name,
                'latest_commit': str(repo.head.commit)
            }
        except Exception as e:
            print(f"Error syncing repository at {local_path}: {e}")
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
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(self.base_backup_dir, f"{repo_name}_{timestamp}")
        
        try:
            # Copy the entire repository
            import shutil
            shutil.copytree(local_path, backup_path)
            return backup_path
        except Exception as e:
            print(f"Error creating backup for {repo_name}: {e}")
            return ""

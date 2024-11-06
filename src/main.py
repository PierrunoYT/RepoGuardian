import os
import json
import argparse
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

from .database import RepositoryDatabase, DatabaseError
from .git_sync import GitRepositorySync, GitSyncError
from .logger import logger

class ConfigError(Exception):
    """Custom exception for configuration errors"""
    pass

class GitHubBackupTool:
    def __init__(self, config_path: str = 'config/repositories.json', 
                 interactive_setup: bool = False,
                 max_workers: int = 4):
        """
        Initialize the GitHub Backup Tool
        
        Args:
            config_path (str): Path to the repositories configuration file
            interactive_setup (bool): Whether to start interactive setup if config doesn't exist
            max_workers (int): Maximum number of concurrent operations
        """
        self.config_path = config_path
        self.max_workers = max_workers
        
        try:
            # Ensure config directory exists
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            
            # Initialize components
            self.db = RepositoryDatabase()
            self.git_sync = GitRepositorySync(max_workers=max_workers)
            
            logger.info(f"Initialized GitHubBackupTool with config: {config_path}")
            
            # Check if config exists, if not, start interactive setup if enabled
            if not os.path.exists(config_path) and interactive_setup:
                self.interactive_setup()
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    def validate_config(self, config: Dict) -> bool:
        """
        Validate configuration format
        
        Args:
            config: Configuration dictionary to validate
        
        Returns:
            bool: True if valid, False otherwise
        """
        if not isinstance(config, dict):
            return False
        
        if 'repositories' not in config:
            return False
        
        if not isinstance(config['repositories'], list):
            return False
        
        required_fields = {'name', 'url', 'local_path'}
        for repo in config['repositories']:
            if not all(field in repo for field in required_fields):
                return False
            
            # Validate repository URL
            if not self.git_sync.validate_repo_url(repo['url']):
                return False
        
        return True

    def interactive_setup(self) -> None:
        """
        Interactive setup to configure repositories
        
        Raises:
            ConfigError: If configuration creation fails
        """
        try:
            logger.info("Starting interactive setup")
            print("Welcome to GitHub Repository Backup and Sync Tool!")
            print("Let's set up your repositories.")
            
            repositories = []
            while True:
                print("\nAdd a new repository:")
                name = input("Repository Name (e.g., my-project): ").strip()
                url = input("Repository URL (e.g., https://github.com/username/repo): ").strip()
                local_path = input("Local Backup Path (default: repos/{name}): ").strip()
                
                if not local_path:
                    local_path = os.path.join('repos', name)
                
                if not self.git_sync.validate_repo_url(url):
                    print("Invalid repository URL format. Please try again.")
                    continue
                
                repositories.append({
                    "name": name,
                    "url": url,
                    "local_path": local_path
                })
                
                add_more = input("\nDo you want to add another repository? (y/n): ").lower()
                if add_more != 'y':
                    break
            
            # Save configuration
            config = {"repositories": repositories}
            with open(self.config_path, 'w') as f:
                json.dump(config, f, indent=4)
            
            logger.info(f"Configuration saved to {self.config_path}")
            print(f"\nConfiguration saved to {self.config_path}")
            
        except Exception as e:
            error_msg = f"Interactive setup failed: {e}"
            logger.error(error_msg)
            raise ConfigError(error_msg)

    def load_repositories(self) -> List[Dict]:
        """
        Load repositories from configuration file
        
        Returns:
            List of repository configurations
        
        Raises:
            ConfigError: If configuration loading or validation fails
        """
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            if not self.validate_config(config):
                raise ConfigError("Invalid configuration format")
            
            logger.info(f"Successfully loaded {len(config['repositories'])} repositories")
            return config['repositories']
            
        except FileNotFoundError:
            error_msg = f"Config file not found at {self.config_path}"
            logger.error(error_msg)
            return []
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON in config file: {e}"
            logger.error(error_msg)
            raise ConfigError(error_msg)
        except Exception as e:
            error_msg = f"Error loading repositories: {e}"
            logger.error(error_msg)
            raise ConfigError(error_msg)

    def process_repository(self, repo: Dict) -> Dict:
        """
        Process a single repository (clone/sync/backup)
        
        Args:
            repo: Repository configuration
        
        Returns:
            Dict containing processing results
        """
        try:
            name = repo['name']
            url = repo['url']
            local_path = repo['local_path']
            
            # Ensure local path exists
            os.makedirs(local_path, exist_ok=True)
            
            # Clone if necessary
            if not os.path.exists(os.path.join(local_path, '.git')):
                logger.info(f"Cloning {name} from {url}")
                if not self.git_sync.clone_repository(url, local_path):
                    return {'status': 'failed', 'error': 'Clone failed'}
            
            # Sync repository
            logger.info(f"Syncing {name}")
            sync_result = self.git_sync.sync_repository(local_path)
            
            if sync_result['status'] == 'success':
                # Create backup
                logger.info(f"Creating backup for {name}")
                backup_path = self.git_sync.backup_repository(local_path, name)
                
                # Update database
                with self.db:
                    repo_id = self.db.add_repository(name, url, local_path)
                    self.db.update_last_sync(repo_id, datetime.now().isoformat())
                
                sync_result['backup_path'] = backup_path
            
            return sync_result
            
        except Exception as e:
            error_msg = f"Error processing repository {repo['name']}: {e}"
            logger.error(error_msg)
            return {'status': 'failed', 'error': str(e)}

    def backup_and_sync(self) -> None:
        """
        Backup and synchronize all configured repositories
        """
        try:
            repositories = self.load_repositories()
            
            if not repositories:
                logger.warning("No repositories configured")
                print("No repositories configured. Please add repositories first.")
                return
            
            logger.info(f"Starting backup and sync for {len(repositories)} repositories")
            
            # Process repositories in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_repo = {
                    executor.submit(self.process_repository, repo): repo 
                    for repo in repositories
                }
                
                for future in future_to_repo:
                    repo = future_to_repo[future]
                    try:
                        result = future.result()
                        status = result['status']
                        if status == 'success':
                            print(f"Successfully processed {repo['name']}")
                            print(f"Backup path: {result.get('backup_path', 'N/A')}")
                        else:
                            print(f"Failed to process {repo['name']}: {result.get('error', 'Unknown error')}")
                    except Exception as e:
                        logger.error(f"Error processing {repo['name']}: {e}")
                        print(f"Error processing {repo['name']}: {e}")
            
            # Clean up old backups
            self.git_sync.cleanup_old_backups()
            
        except Exception as e:
            logger.error(f"Backup and sync operation failed: {e}")
            raise

    def run(self) -> None:
        """
        Run the backup and sync process
        """
        try:
            logger.info("Starting backup tool")
            self.backup_and_sync()
        except Exception as e:
            logger.error(f"An error occurred during backup and sync: {e}")
            raise
        finally:
            self.db.close()
            logger.info("Backup tool finished")

def main():
    """Main entry point for the application"""
    try:
        parser = argparse.ArgumentParser(description='GitHub Repository Backup and Sync Tool')
        parser.add_argument('--config', default='config/repositories.json', 
                          help='Path to repositories configuration file')
        parser.add_argument('--workers', type=int, default=4,
                          help='Maximum number of concurrent operations')
        parser.add_argument('--setup', action='store_true',
                          help='Run interactive setup')
        args = parser.parse_args()

        backup_tool = GitHubBackupTool(
            config_path=args.config,
            interactive_setup=args.setup,
            max_workers=args.workers
        )
        backup_tool.run()
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        print(f"Error: {e}")
        exit(1)

if __name__ == '__main__':
    main()

import os
import json
import signal
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor
import threading
from tqdm import tqdm

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
        self.max_workers = max(1, min(max_workers, 10))  # Ensure between 1 and 10
        self.shutdown_event = threading.Event()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        try:
            # Ensure config directory exists
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            
            # Initialize components
            self.db = RepositoryDatabase()
            self.git_sync = GitRepositorySync(max_workers=self.max_workers)
            
            logger.info(f"Initialized GitHubBackupTool with config: {config_path}")
            
            # Check if config exists, if not, start interactive setup if enabled
            if not os.path.exists(config_path) and interactive_setup:
                self.interactive_setup()
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.shutdown_event.set()

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
            
            # Validate local path is absolute or relative to config directory
            if not os.path.isabs(repo['local_path']):
                repo['local_path'] = os.path.join(
                    os.path.dirname(self.config_path),
                    repo['local_path']
                )
        
        return True

    def interactive_setup(self) -> None:
        """
        Interactive setup to configure repositories
        
        Raises:
            ConfigError: If configuration creation fails
        """
        try:
            logger.info("Starting interactive setup")
            print("\n=== GitHub Repository Backup and Sync Tool Setup ===")
            print("Configure your repositories for backup and synchronization.")
            
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

    def process_repository(self, repo: Dict, progress_bar: Optional[tqdm] = None) -> Dict:
        """
        Process a single repository (clone/sync/backup)
        
        Args:
            repo: Repository configuration
            progress_bar: Optional progress bar for updates
        
        Returns:
            Dict containing processing results
        """
        try:
            if self.shutdown_event.is_set():
                return {'status': 'cancelled', 'error': 'Operation cancelled'}

            name = repo['name']
            url = repo['url']
            local_path = repo['local_path']
            
            def update_progress(message: str, percentage: int):
                if progress_bar:
                    progress_bar.set_description(f"{name}: {message}")
                    progress_bar.update(percentage - progress_bar.n)
            
            # Ensure local path exists
            os.makedirs(local_path, exist_ok=True)
            
            # Clone if necessary
            if not os.path.exists(os.path.join(local_path, '.git')):
                logger.info(f"Cloning {name} from {url}")
                if not self.git_sync.clone_repository(url, local_path, update_progress):
                    return {'status': 'failed', 'error': 'Clone failed'}
            
            # Sync repository
            logger.info(f"Syncing {name}")
            sync_result = self.git_sync.sync_repository(local_path, update_progress)
            
            if sync_result['status'] == 'success':
                # Create backup
                logger.info(f"Creating backup for {name}")
                backup_path = self.git_sync.backup_repository(local_path, name, update_progress)
                
                # Update database
                with self.db:
                    repo_id = self.db.add_repository(name, url, local_path)
                    self.db.update_last_sync(repo_id, datetime.now().isoformat())
                
                sync_result['backup_path'] = backup_path
            
            if progress_bar:
                progress_bar.update(100 - progress_bar.n)  # Ensure we reach 100%
            
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
            
            # Create progress bars for each repository
            progress_bars = {
                repo['name']: tqdm(
                    total=100,
                    desc=f"Processing {repo['name']}",
                    position=i,
                    leave=True
                )
                for i, repo in enumerate(repositories)
            }
            
            results = {}
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_repo = {
                    executor.submit(
                        self.process_repository,
                        repo,
                        progress_bars[repo['name']]
                    ): repo for repo in repositories
                }
                
                for future in future_to_repo:
                    repo = future_to_repo[future]
                    try:
                        result = future.result()
                        results[repo['name']] = result
                    except Exception as e:
                        logger.error(f"Error processing {repo['name']}: {e}")
                        results[repo['name']] = {
                            'status': 'failed',
                            'error': str(e)
                        }
            
            # Close progress bars
            for bar in progress_bars.values():
                bar.close()
            
            # Print summary
            print("\nOperation Summary:")
            for name, result in results.items():
                status = result['status']
                if status == 'success':
                    print(f"✓ {name}: Success (Backup: {result.get('backup_path', 'N/A')})")
                else:
                    print(f"✗ {name}: Failed - {result.get('error', 'Unknown error')}")
            
            # Clean up old backups if not shutting down
            if not self.shutdown_event.is_set():
                self.git_sync.cleanup_old_backups()
            
            # Print statistics
            stats = self.db.get_sync_statistics()
            print("\nRepository Statistics:")
            print(f"Total Repositories: {stats['total_repositories']}")
            print(f"Active Repositories: {stats['active_repositories']}")
            print(f"Last Sync: {stats['last_sync_time'] or 'Never'}")
            
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
            if self.shutdown_event.is_set():
                logger.info("Shutting down gracefully")
            self.db.close()
            logger.info("Backup tool finished")

def main():
    """Main entry point for the application"""
    try:
        parser = argparse.ArgumentParser(
            description='GitHub Repository Backup and Sync Tool',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        parser.add_argument('--config', 
                          default='config/repositories.json', 
                          help='Path to repositories configuration file')
        parser.add_argument('--workers', 
                          type=int,
                          default=4,
                          help='Maximum number of concurrent operations (1-10)')
        parser.add_argument('--setup', 
                          action='store_true',
                          help='Run interactive setup')
        args = parser.parse_args()

        backup_tool = GitHubBackupTool(
            config_path=args.config,
            interactive_setup=args.setup,
            max_workers=args.workers
        )
        backup_tool.run()
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        print("\nOperation cancelled by user")
        exit(1)
    except Exception as e:
        logger.error(f"Application failed: {e}")
        print(f"\nError: {e}")
        exit(1)

if __name__ == '__main__':
    main()

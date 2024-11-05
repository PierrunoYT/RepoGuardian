import os
import json
import argparse
from datetime import datetime

from database import RepositoryDatabase
from git_sync import GitRepositorySync

class GitHubBackupTool:
    def __init__(self, config_path='config/repositories.json'):
        """
        Initialize the GitHub Backup Tool
        
        Args:
            config_path (str): Path to the repositories configuration file
        """
        self.config_path = config_path
        self.db = RepositoryDatabase()
        self.git_sync = GitRepositorySync()
        
        # Ensure config directory exists
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        # Check if config exists, if not, start interactive setup
        if not os.path.exists(config_path):
            self.interactive_setup()

    def interactive_setup(self):
        """
        Interactive setup to configure repositories
        """
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
        
        print(f"\nConfiguration saved to {self.config_path}")

    def load_repositories(self):
        """
        Load repositories from configuration file
        
        Returns:
            List of repository configurations
        """
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)['repositories']
        except FileNotFoundError:
            print(f"Config file not found at {self.config_path}")
            return []
        except json.JSONDecodeError:
            print(f"Invalid JSON in config file at {self.config_path}")
            return []

    def backup_and_sync(self):
        """
        Backup and synchronize all configured repositories
        """
        repositories = self.load_repositories()
        
        if not repositories:
            print("No repositories configured. Please add repositories first.")
            return
        
        for repo_config in repositories:
            name = repo_config['name']
            url = repo_config['url']
            local_path = repo_config['local_path']
            
            # Ensure local path exists
            os.makedirs(local_path, exist_ok=True)
            
            # Check if repository is already cloned
            if not os.path.exists(os.path.join(local_path, '.git')):
                print(f"Cloning {name} from {url}")
                self.git_sync.clone_repository(url, local_path)
            
            # Sync repository
            print(f"Syncing {name}")
            sync_result = self.git_sync.sync_repository(local_path)
            
            # Create backup
            print(f"Creating backup for {name}")
            backup_path = self.git_sync.backup_repository(local_path, name)
            
            # Update database
            repo_id = self.db.add_repository(name, url, local_path)
            self.db.update_last_sync(repo_id, datetime.now().isoformat())
            
            # Print results
            print(f"Sync Status for {name}: {sync_result.get('status', 'Unknown')}")
            print(f"Backup Path: {backup_path}")

    def run(self):
        """
        Run the backup and sync process
        """
        try:
            self.backup_and_sync()
        except Exception as e:
            print(f"An error occurred during backup and sync: {e}")
        finally:
            self.db.close()

def main():
    parser = argparse.ArgumentParser(description='GitHub Repository Backup and Sync Tool')
    parser.add_argument('--config', default='config/repositories.json', 
                        help='Path to repositories configuration file')
    args = parser.parse_args()

    backup_tool = GitHubBackupTool(config_path=args.config)
    backup_tool.run()

if __name__ == '__main__':
    main()

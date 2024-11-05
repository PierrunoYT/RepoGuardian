# GitHub Repository Backup and Sync Tool

## Features
- Local backup of GitHub repositories
- Offline synchronization
- Personal project archival

## Setup
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure your GitHub repositories in `config/repositories.json`

## Usage
Run the main script to backup and sync repositories:
```
python src/main.py
```

## Configuration
Create a `config/repositories.json` file with your repositories:
```json
{
    "repositories": [
        {
            "name": "repo-name",
            "url": "https://github.com/username/repo",
            "local_path": "/path/to/local/backup"
        }
    ]
}

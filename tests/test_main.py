import os
import json
import pytest
from unittest.mock import patch, MagicMock
import tempfile
import shutil
import git

from src.main import GitHubBackupTool

@pytest.fixture
def temp_config_dir():
    """Create a temporary directory for configuration files"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    try:
        shutil.rmtree(temp_dir)
    except (PermissionError, OSError) as e:
        print(f"Warning: Could not remove temporary directory {temp_dir}: {e}")

@pytest.fixture
@patch('src.main.GitRepositorySync')
@patch('src.main.RepositoryDatabase')
def backup_tool(mock_db, mock_git_sync, temp_config_dir):
    """Create a GitHubBackupTool instance with a temporary config path"""
    config_path = os.path.join(temp_config_dir, 'repositories.json')
    
    # Set up mock instances
    mock_git_sync_instance = MagicMock()
    mock_git_sync_instance.clone_repository.return_value = True
    mock_git_sync_instance.sync_repository.return_value = {'status': 'success'}
    mock_git_sync_instance.backup_repository.return_value = os.path.join(temp_config_dir, 'backups', 'test-repo')
    mock_git_sync.return_value = mock_git_sync_instance

    mock_db_instance = MagicMock()
    mock_db_instance.add_repository.return_value = 1
    mock_db.return_value = mock_db_instance

    return GitHubBackupTool(config_path=config_path)

def test_init_without_config(temp_config_dir):
    """Test initialization when config file doesn't exist"""
    config_path = os.path.join(temp_config_dir, 'repositories.json')
    with patch('src.main.GitRepositorySync'), patch('src.main.RepositoryDatabase'):
        backup_tool = GitHubBackupTool(config_path=config_path, interactive_setup=False)
        assert not os.path.exists(config_path)

@patch('builtins.input', side_effect=['test-repo', 'https://github.com/test/repo', '', 'n'])
def test_interactive_setup(mock_input, backup_tool):
    """Test interactive setup of repositories"""
    config_path = backup_tool.config_path

    backup_tool.interactive_setup()

    # Verify config file was created
    assert os.path.exists(config_path)

    # Check config contents
    with open(config_path, 'r') as f:
        config = json.load(f)

    assert len(config['repositories']) == 1
    repo = config['repositories'][0]
    assert repo['name'] == 'test-repo'
    assert repo['url'] == 'https://github.com/test/repo'
    assert repo['local_path'] == os.path.join('repos', 'test-repo')

def test_load_repositories(backup_tool, temp_config_dir):
    """Test loading repositories from config file"""
    config_path = backup_tool.config_path

    # Create a test config file
    test_repos = {
        "repositories": [
            {
                "name": "test-repo-1",
                "url": "https://github.com/test/repo1",
                "local_path": "repos/test-repo-1"
            },
            {
                "name": "test-repo-2",
                "url": "https://github.com/test/repo2",
                "local_path": "repos/test-repo-2"
            }
        ]
    }

    with open(config_path, 'w') as f:
        json.dump(test_repos, f)

    # Load repositories
    loaded_repos = backup_tool.load_repositories()

    assert len(loaded_repos) == 2
    assert loaded_repos[0]['name'] == 'test-repo-1'
    assert loaded_repos[1]['name'] == 'test-repo-2'

def test_load_repositories_file_not_found(backup_tool):
    """Test loading repositories when file is not found"""
    # Use a non-existent config path
    backup_tool.config_path = '/path/to/non/existent/config.json'
    
    loaded_repos = backup_tool.load_repositories()
    assert loaded_repos == []

def test_load_repositories_invalid_json(backup_tool, temp_config_dir):
    """Test loading repositories with invalid JSON"""
    config_path = backup_tool.config_path

    # Write invalid JSON
    with open(config_path, 'w') as f:
        f.write('Invalid JSON')

    loaded_repos = backup_tool.load_repositories()
    assert loaded_repos == []

def test_backup_and_sync(backup_tool, temp_config_dir):
    """Test backup and sync process"""
    config_path = backup_tool.config_path

    # Create a test config file
    test_repos = {
        "repositories": [
            {
                "name": "test-repo",
                "url": "https://github.com/test/repo",
                "local_path": os.path.join(temp_config_dir, 'repos', 'test-repo')
            }
        ]
    }

    with open(config_path, 'w') as f:
        json.dump(test_repos, f)

    # Create the local path
    repo_path = os.path.join(temp_config_dir, 'repos', 'test-repo')
    os.makedirs(repo_path, exist_ok=True)

    # Run backup and sync
    backup_tool.backup_and_sync()

    # Get the mock instances from the backup_tool
    mock_git_sync = backup_tool.git_sync
    mock_db = backup_tool.db

    # Verify method calls
    mock_git_sync.sync_repository.assert_called_once()
    mock_git_sync.backup_repository.assert_called_once()
    mock_db.add_repository.assert_called_once()
    mock_db.update_last_sync.assert_called_once()

def test_run_method(backup_tool):
    """Test the run method handles exceptions"""
    with patch.object(backup_tool, 'backup_and_sync', side_effect=Exception("Test error")):
        with patch.object(backup_tool.db, 'close') as mock_close:
            backup_tool.run()
            mock_close.assert_called_once()

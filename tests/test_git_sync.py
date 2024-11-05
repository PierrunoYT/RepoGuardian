import os
import pytest
import shutil
from unittest.mock import Mock, patch
from src.git_sync import GitRepositorySync

@pytest.fixture
def git_sync():
    """Create a GitRepositorySync instance with a temporary base backup directory"""
    return GitRepositorySync(base_backup_dir='temp_backups')

def test_init(tmp_path):
    """Test initialization of GitRepositorySync"""
    sync = GitRepositorySync(base_backup_dir=str(tmp_path))
    assert os.path.exists(str(tmp_path))

@patch('git.Repo.clone_from')
def test_clone_repository_success(mock_clone, tmp_path, git_sync):
    """Test successful repository cloning"""
    repo_url = 'https://github.com/test/repo.git'
    local_path = str(tmp_path / 'repo')
    
    # Mock successful clone
    mock_clone.return_value = None
    
    result = git_sync.clone_repository(repo_url, local_path)
    
    assert result is True
    mock_clone.assert_called_once_with(repo_url, local_path)
    assert os.path.exists(local_path)

@patch('git.Repo.clone_from')
def test_clone_repository_failure(mock_clone, tmp_path, git_sync):
    """Test repository cloning failure"""
    repo_url = 'https://github.com/test/repo.git'
    local_path = str(tmp_path / 'repo')
    
    # Simulate clone failure
    mock_clone.side_effect = Exception("Clone failed")
    
    result = git_sync.clone_repository(repo_url, local_path)
    
    assert result is False
    assert not os.path.exists(local_path)

@patch('git.Repo')
def test_sync_repository_success(mock_repo, tmp_path, git_sync):
    """Test successful repository synchronization"""
    local_path = str(tmp_path / 'repo')
    os.makedirs(local_path)
    
    # Create mock repo and its attributes
    mock_repo_instance = Mock()
    mock_repo_instance.remotes.origin = Mock()
    mock_repo_instance.active_branch.name = 'main'
    mock_repo_instance.head.commit = Mock(return_value='abc123')
    mock_repo.return_value = mock_repo_instance
    
    result = git_sync.sync_repository(local_path)
    
    assert result['status'] == 'success'
    assert 'timestamp' in result
    assert result['branch'] == 'main'
    assert 'latest_commit' in result

@patch('git.Repo')
def test_sync_repository_failure(mock_repo, tmp_path, git_sync):
    """Test repository synchronization failure"""
    local_path = str(tmp_path / 'repo')
    os.makedirs(local_path)
    
    # Simulate sync failure
    mock_repo.side_effect = Exception("Sync failed")
    
    result = git_sync.sync_repository(local_path)
    
    assert result['status'] == 'failed'
    assert 'error' in result

def test_backup_repository(tmp_path, git_sync):
    """Test repository backup"""
    # Create a mock repository to backup
    repo_path = str(tmp_path / 'repo')
    os.makedirs(repo_path)
    with open(os.path.join(repo_path, 'test_file.txt'), 'w') as f:
        f.write('test content')
    
    backup_path = git_sync.backup_repository(repo_path, 'test_repo')
    
    # Use os.path.normpath to handle different path separators
    assert os.path.normpath(backup_path).startswith(os.path.normpath('temp_backups/test_repo_'))
    assert os.path.exists(backup_path)
    assert os.path.exists(os.path.join(backup_path, 'test_file.txt'))

def test_backup_repository_failure(tmp_path, git_sync):
    """Test repository backup failure"""
    # Non-existent repository path
    repo_path = str(tmp_path / 'non_existent_repo')
    
    backup_path = git_sync.backup_repository(repo_path, 'test_repo')
    
    assert backup_path == ""

# Clean up temporary directories after tests
def teardown_module(module):
    try:
        if os.path.exists('temp_backups'):
            shutil.rmtree('temp_backups', ignore_errors=True)
    except (PermissionError, OSError) as e:
        print(f"Warning: Could not remove temp_backups directory: {e}")

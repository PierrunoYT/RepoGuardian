import os
import sqlite3
import pytest
from datetime import datetime
from src.database import RepositoryDatabase

@pytest.fixture
def temp_db_path(tmp_path):
    """Create a temporary database path for testing"""
    return str(tmp_path / 'test_repositories.db')

def test_database_initialization(temp_db_path):
    """Test database initialization creates the correct table"""
    db = RepositoryDatabase(temp_db_path)
    
    # Check table exists
    db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='repositories'")
    assert db.cursor.fetchone() is not None
    
    db.close()

def test_add_repository(temp_db_path):
    """Test adding a repository to the database"""
    db = RepositoryDatabase(temp_db_path)
    
    # Add a repository
    repo_id = db.add_repository(
        name='Test Repo', 
        url='https://github.com/test/repo', 
        local_path='/path/to/local/repo'
    )
    
    # Verify repository was added
    db.cursor.execute('SELECT * FROM repositories WHERE id = ?', (repo_id,))
    repo = db.cursor.fetchone()
    
    assert repo is not None
    assert repo[1] == 'Test Repo'
    assert repo[2] == 'https://github.com/test/repo'
    assert repo[3] == '/path/to/local/repo'
    
    db.close()

def test_get_repositories(temp_db_path):
    """Test retrieving repositories"""
    db = RepositoryDatabase(temp_db_path)
    
    # Add multiple repositories
    db.add_repository('Repo1', 'https://github.com/test/repo1', '/path/1')
    db.add_repository('Repo2', 'https://github.com/test/repo2', '/path/2')
    
    # Get repositories
    repos = db.get_repositories()
    
    assert len(repos) == 2
    assert repos[0]['name'] == 'Repo1'
    assert repos[1]['name'] == 'Repo2'
    
    db.close()

def test_update_last_sync(temp_db_path):
    """Test updating last sync time"""
    db = RepositoryDatabase(temp_db_path)
    
    # Add a repository
    repo_id = db.add_repository('Test Repo', 'https://github.com/test/repo', '/path/to/repo')
    
    # Update last sync time
    sync_time = datetime.now().isoformat()
    db.update_last_sync(repo_id, sync_time)
    
    # Verify sync time was updated
    db.cursor.execute('SELECT last_sync FROM repositories WHERE id = ?', (repo_id,))
    updated_sync_time = db.cursor.fetchone()[0]
    
    assert updated_sync_time == sync_time
    
    db.close()

def test_get_active_repositories(temp_db_path):
    """Test retrieving only active repositories"""
    db = RepositoryDatabase(temp_db_path)
    
    # Add multiple repositories
    repo1_id = db.add_repository('Repo1', 'https://github.com/test/repo1', '/path/1')
    repo2_id = db.add_repository('Repo2', 'https://github.com/test/repo2', '/path/2')
    
    # Deactivate one repository
    db.cursor.execute('UPDATE repositories SET is_active = 0 WHERE id = ?', (repo2_id,))
    db.conn.commit()
    
    # Get active repositories
    active_repos = db.get_repositories(active_only=True)
    
    assert len(active_repos) == 1
    assert active_repos[0]['name'] == 'Repo1'
    
    db.close()

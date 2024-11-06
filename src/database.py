import sqlite3
import os
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager
from datetime import datetime
from .logger import logger

class DatabaseError(Exception):
    """Custom exception for database operations"""
    pass

class RepositoryDatabase:
    def __init__(self, db_path: str = 'config/repositories.db'):
        """
        Initialize the SQLite database for tracking repositories
        
        Args:
            db_path (str): Path to the SQLite database file
        
        Raises:
            DatabaseError: If database initialization fails
        """
        try:
            # Ensure config directory exists
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            self.db_path = db_path
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row  # Enable row factory for dict-like access
            self.cursor = self.conn.cursor()
            
            # Create repositories table if not exists
            self._init_database()
            logger.info(f"Database initialized successfully at {db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise DatabaseError(f"Database initialization failed: {e}")

    def _init_database(self):
        """Initialize database schema and indexes"""
        try:
            # Create repositories table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS repositories (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    last_sync DATETIME,
                    is_active BOOLEAN DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create indexes for frequently queried columns
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_repo_name 
                ON repositories(name)
            ''')
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_repo_url 
                ON repositories(url)
            ''')
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_repo_active 
                ON repositories(is_active)
            ''')

            # Create trigger for updating updated_at
            self.cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS update_timestamp 
                AFTER UPDATE ON repositories
                BEGIN
                    UPDATE repositories 
                    SET updated_at = CURRENT_TIMESTAMP 
                    WHERE id = NEW.id;
                END
            ''')

            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise DatabaseError(f"Failed to initialize database schema: {e}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    @contextmanager
    def transaction(self):
        """Context manager for database transactions"""
        try:
            yield
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Transaction failed: {e}")
            raise DatabaseError(f"Transaction failed: {e}")

    def add_repository(self, name: str, url: str, local_path: str) -> int:
        """
        Add a new repository or update if exists
        
        Args:
            name (str): Repository name
            url (str): Repository URL
            local_path (str): Local backup path
        
        Returns:
            int: ID of the inserted/updated repository
        
        Raises:
            DatabaseError: If repository addition fails
        """
        try:
            with self.transaction():
                # Check if repository already exists
                self.cursor.execute('''
                    SELECT id FROM repositories 
                    WHERE name = ? OR url = ? OR local_path = ?
                ''', (name, url, local_path))
                existing = self.cursor.fetchone()

                if existing:
                    # Update existing repository
                    self.cursor.execute('''
                        UPDATE repositories 
                        SET name = ?, url = ?, local_path = ?, is_active = 1
                        WHERE id = ?
                    ''', (name, url, local_path, existing['id']))
                    repo_id = existing['id']
                    logger.info(f"Updated repository: {name} (ID: {repo_id})")
                else:
                    # Insert new repository
                    self.cursor.execute('''
                        INSERT INTO repositories (name, url, local_path) 
                        VALUES (?, ?, ?)
                    ''', (name, url, local_path))
                    repo_id = self.cursor.lastrowid
                    logger.info(f"Added repository: {name} (ID: {repo_id})")

                return repo_id
        except Exception as e:
            raise DatabaseError(f"Failed to add/update repository: {e}")

    def get_repositories(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Retrieve repositories from the database
        
        Args:
            active_only (bool): Only return active repositories
        
        Returns:
            List of repository dictionaries
        
        Raises:
            DatabaseError: If repository retrieval fails
        """
        try:
            query = 'SELECT * FROM repositories'
            params: List[Any] = []
            
            if active_only:
                query += ' WHERE is_active = ?'
                params.append(1)
            
            self.cursor.execute(query, params)
            repositories = [dict(row) for row in self.cursor.fetchall()]
            
            logger.info(f"Retrieved {len(repositories)} repositories")
            return repositories
        except Exception as e:
            logger.error(f"Failed to retrieve repositories: {e}")
            raise DatabaseError(f"Failed to retrieve repositories: {e}")

    def get_repository_by_id(self, repo_id: int) -> Optional[Dict[str, Any]]:
        """
        Get repository by ID
        
        Args:
            repo_id (int): Repository ID
        
        Returns:
            Optional[Dict]: Repository data or None if not found
        """
        try:
            self.cursor.execute('SELECT * FROM repositories WHERE id = ?', (repo_id,))
            row = self.cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            raise DatabaseError(f"Failed to get repository: {e}")

    def update_repository(self, repo_id: int, **kwargs) -> None:
        """
        Update repository attributes
        
        Args:
            repo_id (int): Repository ID
            **kwargs: Attributes to update
        
        Raises:
            DatabaseError: If update fails
        """
        try:
            valid_fields = {'name', 'url', 'local_path', 'is_active', 'last_sync'}
            update_fields = {k: v for k, v in kwargs.items() if k in valid_fields}
            
            if not update_fields:
                return
            
            with self.transaction():
                query = '''
                    UPDATE repositories 
                    SET {} 
                    WHERE id = ?
                '''.format(', '.join(f'{k} = ?' for k in update_fields))
                
                params = list(update_fields.values()) + [repo_id]
                self.cursor.execute(query, params)
                
                logger.info(f"Updated repository ID {repo_id}")
        except Exception as e:
            raise DatabaseError(f"Failed to update repository: {e}")

    def update_last_sync(self, repo_id: int, sync_time: str) -> None:
        """
        Update the last sync time for a repository
        
        Args:
            repo_id (int): Repository ID
            sync_time (str): Timestamp of last sync
        
        Raises:
            DatabaseError: If update fails
        """
        try:
            self.update_repository(repo_id, last_sync=sync_time)
        except Exception as e:
            raise DatabaseError(f"Failed to update last sync time: {e}")

    def deactivate_repository(self, repo_id: int) -> None:
        """
        Deactivate a repository
        
        Args:
            repo_id (int): Repository ID
        
        Raises:
            DatabaseError: If deactivation fails
        """
        try:
            self.update_repository(repo_id, is_active=0)
        except Exception as e:
            raise DatabaseError(f"Failed to deactivate repository: {e}")

    def reactivate_repository(self, repo_id: int) -> None:
        """
        Reactivate a deactivated repository
        
        Args:
            repo_id (int): Repository ID
        
        Raises:
            DatabaseError: If reactivation fails
        """
        try:
            self.update_repository(repo_id, is_active=1)
        except Exception as e:
            raise DatabaseError(f"Failed to reactivate repository: {e}")

    def delete_repository(self, repo_id: int) -> None:
        """
        Permanently delete a repository
        
        Args:
            repo_id (int): Repository ID
        
        Raises:
            DatabaseError: If deletion fails
        """
        try:
            with self.transaction():
                self.cursor.execute('DELETE FROM repositories WHERE id = ?', (repo_id,))
                logger.info(f"Deleted repository ID {repo_id}")
        except Exception as e:
            raise DatabaseError(f"Failed to delete repository: {e}")

    def get_sync_statistics(self) -> Dict[str, Any]:
        """
        Get repository sync statistics
        
        Returns:
            Dict containing sync statistics
        """
        try:
            stats = {
                'total_repositories': 0,
                'active_repositories': 0,
                'inactive_repositories': 0,
                'last_sync_time': None,
                'never_synced': 0
            }
            
            self.cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active,
                    SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) as inactive,
                    MAX(last_sync) as last_sync,
                    SUM(CASE WHEN last_sync IS NULL THEN 1 ELSE 0 END) as never_synced
                FROM repositories
            ''')
            
            row = self.cursor.fetchone()
            if row:
                stats.update({
                    'total_repositories': row['total'],
                    'active_repositories': row['active'],
                    'inactive_repositories': row['inactive'],
                    'last_sync_time': row['last_sync'],
                    'never_synced': row['never_synced']
                })
            
            return stats
        except Exception as e:
            raise DatabaseError(f"Failed to get sync statistics: {e}")

    def close(self) -> None:
        """Close database connection"""
        try:
            if hasattr(self, 'conn'):
                self.conn.close()
                logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

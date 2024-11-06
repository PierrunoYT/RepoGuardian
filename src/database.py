import sqlite3
import os
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
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
            self.cursor = self.conn.cursor()
            
            # Create repositories table if not exists
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS repositories (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    last_sync DATETIME,
                    is_active BOOLEAN DEFAULT 1
                )
            ''')
            self.conn.commit()
            logger.info(f"Database initialized successfully at {db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise DatabaseError(f"Database initialization failed: {e}")

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
        Add a new repository to the database
        
        Args:
            name (str): Repository name
            url (str): Repository URL
            local_path (str): Local backup path
        
        Returns:
            int: ID of the inserted repository
        
        Raises:
            DatabaseError: If repository addition fails
        """
        try:
            with self.transaction():
                self.cursor.execute('''
                    INSERT INTO repositories (name, url, local_path) 
                    VALUES (?, ?, ?)
                ''', (name, url, local_path))
                repo_id = self.cursor.lastrowid
                logger.info(f"Added repository: {name} (ID: {repo_id})")
                return repo_id
        except Exception as e:
            raise DatabaseError(f"Failed to add repository: {e}")

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
            columns = [col[0] for col in self.cursor.description]
            repositories = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            
            logger.info(f"Retrieved {len(repositories)} repositories")
            return repositories
        except Exception as e:
            logger.error(f"Failed to retrieve repositories: {e}")
            raise DatabaseError(f"Failed to retrieve repositories: {e}")

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
            with self.transaction():
                self.cursor.execute('''
                    UPDATE repositories 
                    SET last_sync = ? 
                    WHERE id = ?
                ''', (sync_time, repo_id))
                logger.info(f"Updated last sync time for repository ID {repo_id}")
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
            with self.transaction():
                self.cursor.execute('''
                    UPDATE repositories 
                    SET is_active = 0 
                    WHERE id = ?
                ''', (repo_id,))
                logger.info(f"Deactivated repository ID {repo_id}")
        except Exception as e:
            raise DatabaseError(f"Failed to deactivate repository: {e}")

    def close(self) -> None:
        """Close database connection"""
        try:
            if hasattr(self, 'conn'):
                self.conn.close()
                logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

import sqlite3
import os
from typing import List, Dict, Any

class RepositoryDatabase:
    def __init__(self, db_path: str = 'config/repositories.db'):
        """
        Initialize the SQLite database for tracking repositories
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        # Ensure config directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
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

    def add_repository(self, name: str, url: str, local_path: str) -> int:
        """
        Add a new repository to the database
        
        Args:
            name (str): Repository name
            url (str): Repository URL
            local_path (str): Local backup path
        
        Returns:
            int: ID of the inserted repository
        """
        self.cursor.execute('''
            INSERT INTO repositories (name, url, local_path) 
            VALUES (?, ?, ?)
        ''', (name, url, local_path))
        self.conn.commit()
        return self.cursor.lastrowid

    def get_repositories(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Retrieve repositories from the database
        
        Args:
            active_only (bool): Only return active repositories
        
        Returns:
            List of repository dictionaries
        """
        query = 'SELECT * FROM repositories'
        if active_only:
            query += ' WHERE is_active = 1'
        
        self.cursor.execute(query)
        columns = [col[0] for col in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def update_last_sync(self, repo_id: int, sync_time: str):
        """
        Update the last sync time for a repository
        
        Args:
            repo_id (int): Repository ID
            sync_time (str): Timestamp of last sync
        """
        self.cursor.execute('''
            UPDATE repositories 
            SET last_sync = ? 
            WHERE id = ?
        ''', (sync_time, repo_id))
        self.conn.commit()

    def close(self):
        """Close database connection"""
        self.conn.close()

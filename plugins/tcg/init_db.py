"""
Legacy database initialization module for backward compatibility.

This module now uses the new database module for consistency.
"""

from database import DatabaseManager
import os

DB_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(DB_DIR, "tcg.db")

def init_db():
    """Initialize the database using the new DatabaseManager."""
    db_manager = DatabaseManager()
    db_manager.init_database()
    print(f"Initialized database at {DB_PATH}")

if __name__ == "__main__":
    init_db()

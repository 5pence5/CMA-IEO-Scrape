"""
Database module for caching downloaded files to prevent redundant downloads.

This module provides a SQLite-based caching system that tracks:
- Downloaded file metadata (URL, path, hash, size)
- HTTP caching headers (ETag, Last-Modified)
- Document classification and case information
- Download timestamps for auditing
"""

import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Tuple


class FileCache:
    """Manages a SQLite database for tracking downloaded files."""

    def __init__(self, db_path: Path):
        """
        Initialize the file cache database.

        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self._ensure_connection()
        self._initialize_schema()

    def _ensure_connection(self):
        """Ensure database connection is established."""
        if self.conn is None:
            self.conn = sqlite3.connect(str(self.db_path))
            self.conn.row_factory = sqlite3.Row

    def _initialize_schema(self):
        """Create database tables if they don't exist."""
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS file_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    local_path TEXT NOT NULL,
                    file_hash TEXT,
                    file_size INTEGER,
                    etag TEXT,
                    last_modified TEXT,
                    download_timestamp TEXT NOT NULL,
                    case_title TEXT,
                    case_url TEXT,
                    case_path TEXT,
                    doc_type TEXT,
                    doc_title TEXT,
                    doc_date_display TEXT,
                    last_verified TEXT
                )
            """)

            # Create indexes for common queries
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_url ON file_cache(url)
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_local_path ON file_cache(local_path)
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_case_url ON file_cache(case_url)
            """)

    def compute_file_hash(self, file_path: Path) -> Optional[str]:
        """
        Compute SHA256 hash of a file.

        Args:
            file_path: Path to the file

        Returns:
            Hex digest of the file hash, or None if file doesn't exist or is empty
        """
        if not file_path.exists() or file_path.stat().st_size == 0:
            return None

        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(8192), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()
        except Exception:
            return None

    def get_cached_file(self, url: str) -> Optional[Dict]:
        """
        Retrieve cached file information by URL.

        Args:
            url: The document URL

        Returns:
            Dictionary with cache information, or None if not cached
        """
        self._ensure_connection()
        cursor = self.conn.execute(
            "SELECT * FROM file_cache WHERE url = ?", (url,)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None

    def verify_cached_file(self, url: str) -> bool:
        """
        Verify that a cached file still exists and has the correct hash.

        Args:
            url: The document URL

        Returns:
            True if file exists and hash matches, False otherwise
        """
        cached = self.get_cached_file(url)
        if not cached:
            return False

        local_path = Path(cached["local_path"])

        # Check if file exists
        if not local_path.exists():
            return False

        # Check if file size matches
        if local_path.stat().st_size != cached["file_size"]:
            return False

        # Check if hash matches (if available)
        if cached["file_hash"]:
            current_hash = self.compute_file_hash(local_path)
            if current_hash != cached["file_hash"]:
                return False

        # Update last_verified timestamp
        with self.conn:
            self.conn.execute(
                "UPDATE file_cache SET last_verified = ? WHERE url = ?",
                (datetime.utcnow().isoformat(), url),
            )

        return True

    def add_or_update_file(
        self,
        url: str,
        local_path: Path,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        case_title: Optional[str] = None,
        case_url: Optional[str] = None,
        case_path: Optional[str] = None,
        doc_type: Optional[str] = None,
        doc_title: Optional[str] = None,
        doc_date_display: Optional[str] = None,
    ) -> bool:
        """
        Add or update a file in the cache.

        Args:
            url: The document URL
            local_path: Path where the file is stored
            etag: HTTP ETag header value
            last_modified: HTTP Last-Modified header value
            case_title: Title of the case
            case_url: URL of the case page
            case_path: Path component of the case URL
            doc_type: Document classification
            doc_title: Document title
            doc_date_display: Display date for the document

        Returns:
            True if successful, False otherwise
        """
        if not local_path.exists():
            return False

        file_hash = self.compute_file_hash(local_path)
        file_size = local_path.stat().st_size
        timestamp = datetime.utcnow().isoformat()

        try:
            with self.conn:
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO file_cache (
                        url, local_path, file_hash, file_size, etag, last_modified,
                        download_timestamp, case_title, case_url, case_path,
                        doc_type, doc_title, doc_date_display, last_verified
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        url,
                        str(local_path),
                        file_hash,
                        file_size,
                        etag,
                        last_modified,
                        timestamp,
                        case_title,
                        case_url,
                        case_path,
                        doc_type,
                        doc_title,
                        doc_date_display,
                        timestamp,
                    ),
                )
            return True
        except Exception as e:
            print(f"Error adding file to cache: {e}")
            return False

    def invalidate_url(self, url: str):
        """
        Remove a URL from the cache.

        Args:
            url: The document URL to invalidate
        """
        with self.conn:
            self.conn.execute("DELETE FROM file_cache WHERE url = ?", (url,))

    def get_cache_stats(self) -> Dict:
        """
        Get statistics about the cache.

        Returns:
            Dictionary with cache statistics
        """
        self._ensure_connection()
        cursor = self.conn.execute(
            "SELECT COUNT(*) as total, SUM(file_size) as total_size FROM file_cache"
        )
        row = cursor.fetchone()

        # Count files by type
        cursor = self.conn.execute(
            "SELECT doc_type, COUNT(*) as count FROM file_cache GROUP BY doc_type"
        )
        by_type = {row["doc_type"]: row["count"] for row in cursor.fetchall()}

        # Count verified vs unverified
        cursor = self.conn.execute(
            "SELECT COUNT(*) as verified FROM file_cache WHERE last_verified IS NOT NULL"
        )
        verified = cursor.fetchone()["verified"]

        return {
            "total_files": row["total"],
            "total_size_bytes": row["total_size"] or 0,
            "total_size_mb": round((row["total_size"] or 0) / (1024 * 1024), 2),
            "files_by_type": by_type,
            "verified_files": verified,
        }

    def clean_orphaned_entries(self) -> int:
        """
        Remove cache entries where the local file no longer exists.

        Returns:
            Number of entries removed
        """
        self._ensure_connection()
        cursor = self.conn.execute("SELECT url, local_path FROM file_cache")
        orphaned = []

        for row in cursor.fetchall():
            if not Path(row["local_path"]).exists():
                orphaned.append(row["url"])

        if orphaned:
            with self.conn:
                self.conn.executemany(
                    "DELETE FROM file_cache WHERE url = ?", [(url,) for url in orphaned]
                )

        return len(orphaned)

    def get_files_needing_verification(self, max_age_days: int = 7) -> List[Dict]:
        """
        Get files that haven't been verified recently.

        Args:
            max_age_days: Maximum age in days before verification is needed

        Returns:
            List of file records needing verification
        """
        self._ensure_connection()
        from datetime import timedelta

        cutoff = (datetime.utcnow() - timedelta(days=max_age_days)).isoformat()
        cursor = self.conn.execute(
            """
            SELECT * FROM file_cache
            WHERE last_verified IS NULL OR last_verified < ?
            """,
            (cutoff,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def export_to_dict_list(self) -> List[Dict]:
        """
        Export all cache entries to a list of dictionaries.

        Returns:
            List of all cache entries
        """
        self._ensure_connection()
        cursor = self.conn.execute("SELECT * FROM file_cache ORDER BY download_timestamp DESC")
        return [dict(row) for row in cursor.fetchall()]

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

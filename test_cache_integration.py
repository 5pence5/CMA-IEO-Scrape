#!/usr/bin/env python3
"""
Simple integration test for the file caching system.
"""

import tempfile
import shutil
from pathlib import Path
from cache_db import FileCache


def test_cache_basic_operations():
    """Test basic cache operations"""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create a test file
        test_file = tmpdir / "test.pdf"
        test_file.write_bytes(b"test content")

        # Initialize cache
        cache_db = tmpdir / "cache.db"
        cache = FileCache(cache_db)

        # Test adding a file
        url = "https://example.com/test.pdf"
        success = cache.add_or_update_file(
            url=url,
            local_path=test_file,
            etag="test-etag",
            last_modified="2025-01-01",
            case_title="Test Case",
            doc_type="IEO"
        )
        assert success, "Failed to add file to cache"

        # Test retrieving cached file
        cached = cache.get_cached_file(url)
        assert cached is not None, "Failed to retrieve cached file"
        assert cached["url"] == url
        assert cached["etag"] == "test-etag"
        assert cached["file_hash"] is not None

        # Test verification
        is_valid = cache.verify_cached_file(url)
        assert is_valid, "File verification failed"

        # Test stats
        stats = cache.get_cache_stats()
        assert stats["total_files"] == 1
        assert stats["total_size_bytes"] > 0

        # Test invalidation
        cache.invalidate_url(url)
        cached = cache.get_cached_file(url)
        assert cached is None, "File should be invalidated"

        # Test cleaning orphaned entries
        cache.add_or_update_file(
            url=url,
            local_path=test_file,
        )
        test_file.unlink()  # Delete the file
        removed = cache.clean_orphaned_entries()
        assert removed == 1, f"Expected 1 orphaned entry, got {removed}"

        cache.close()

        print("✓ All cache operations working correctly")


def test_hash_computation():
    """Test file hash computation"""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        cache = FileCache(tmpdir / "cache.db")

        # Create test files
        file1 = tmpdir / "file1.pdf"
        file2 = tmpdir / "file2.pdf"
        file1.write_bytes(b"content1")
        file2.write_bytes(b"content2")

        hash1 = cache.compute_file_hash(file1)
        hash2 = cache.compute_file_hash(file2)

        assert hash1 != hash2, "Different files should have different hashes"
        assert len(hash1) == 64, "SHA256 hash should be 64 characters"

        # Same content should produce same hash
        file3 = tmpdir / "file3.pdf"
        file3.write_bytes(b"content1")
        hash3 = cache.compute_file_hash(file3)
        assert hash1 == hash3, "Same content should produce same hash"

        cache.close()

        print("✓ Hash computation working correctly")


def test_cache_persistence():
    """Test that cache persists across sessions"""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        cache_db = tmpdir / "cache.db"
        test_file = tmpdir / "test.pdf"
        test_file.write_bytes(b"persistent content")

        url = "https://example.com/persistent.pdf"

        # First session - add file
        cache1 = FileCache(cache_db)
        cache1.add_or_update_file(url=url, local_path=test_file)
        cache1.close()

        # Second session - retrieve file
        cache2 = FileCache(cache_db)
        cached = cache2.get_cached_file(url)
        assert cached is not None, "Cache should persist across sessions"
        assert cached["url"] == url
        cache2.close()

        print("✓ Cache persistence working correctly")


if __name__ == "__main__":
    test_cache_basic_operations()
    test_hash_computation()
    test_cache_persistence()
    print("\n✓✓✓ All tests passed! ✓✓✓")

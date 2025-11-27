# Copyright 2024 Bytedance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import os
import shutil
import time
from pathlib import Path
from typing import Dict, Optional

import structlog

logger = structlog.stdlib.get_logger()


class CodeCache:
    """
    Manages a persistent cache of code files to avoid redundant I/O operations
    when the same code is executed multiple times with different inputs.
    """

    def __init__(self, cache_dir: Optional[str] = None, max_size_mb: int = 1024, ttl_seconds: int = 86400):
        """
        Initialize the code cache.
        
        Args:
            cache_dir: Directory to store cached code files. Defaults to /tmp/sandbox_code_cache
            max_size_mb: Maximum cache size in megabytes. Default 1GB
            ttl_seconds: Time-to-live for cache entries in seconds. Default 24 hours
        """
        self.cache_dir = Path(cache_dir or '/tmp/sandbox_code_cache')
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.ttl_seconds = ttl_seconds
        self._ensure_cache_dir()

    def _ensure_cache_dir(self):
        """Ensure the cache directory exists."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _compute_hash(self, code: str, language: str, suffix: str = '') -> str:
        """
        Compute a unique hash for the code and language combination.
        
        Args:
            code: The code content
            language: The programming language
            suffix: Optional file suffix (e.g., '.py', '.java')
            
        Returns:
            A hex digest hash string
        """
        # Combine code, language, and suffix to create a unique identifier
        content = f"{language}:{suffix}:{code}"
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def get_cached_file(self, code: str, language: str, suffix: str = '') -> Optional[str]:
        """
        Retrieve a cached code file if it exists and is valid.
        
        Args:
            code: The code content
            language: The programming language
            suffix: File suffix (e.g., '.py', '.cpp')
            
        Returns:
            Path to the cached file if found and valid, None otherwise
        """
        code_hash = self._compute_hash(code, language, suffix)
        cache_file = self.cache_dir / f"{code_hash}{suffix}"
        
        if cache_file.exists():
            # Check if cache entry is still valid (not expired)
            file_age = time.time() - cache_file.stat().st_mtime
            if file_age < self.ttl_seconds:
                # Update access time
                cache_file.touch()
                logger.debug(f"Cache hit for {language} code (hash: {code_hash[:8]}...)")
                return str(cache_file)
            else:
                # Remove expired entry
                logger.debug(f"Cache entry expired for {code_hash[:8]}...")
                cache_file.unlink()
        
        return None

    def cache_code_file(self, code: str, language: str, suffix: str = '') -> str:
        """
        Cache a code file and return its path.
        
        Args:
            code: The code content
            language: The programming language
            suffix: File suffix (e.g., '.py', '.cpp')
            
        Returns:
            Path to the cached file
        """
        code_hash = self._compute_hash(code, language, suffix)
        cache_file = self.cache_dir / f"{code_hash}{suffix}"
        
        if not cache_file.exists():
            # Write code to cache
            cache_file.write_text(code)
            logger.debug(f"Cached {language} code file (hash: {code_hash[:8]}...)")
            
            # Cleanup if cache is too large
            self._cleanup_if_needed()
        else:
            # Update access time
            cache_file.touch()
        
        return str(cache_file)

    def get_or_cache(self, code: str, language: str, suffix: str = '') -> tuple[str, bool]:
        """
        Get cached file or cache it if not found.
        
        Args:
            code: The code content
            language: The programming language  
            suffix: File suffix (e.g., '.py', '.cpp')
            
        Returns:
            Tuple of (file_path, was_cached) where was_cached is True if file was in cache
        """
        cached_path = self.get_cached_file(code, language, suffix)
        if cached_path:
            return cached_path, True
        
        new_path = self.cache_code_file(code, language, suffix)
        return new_path, False

    def _get_cache_size(self) -> int:
        """Get total size of cache directory in bytes."""
        total_size = 0
        for entry in self.cache_dir.iterdir():
            if entry.is_file():
                total_size += entry.stat().st_size
        return total_size

    def _cleanup_if_needed(self):
        """Remove oldest cache entries if cache exceeds max size."""
        current_size = self._get_cache_size()
        
        if current_size > self.max_size_bytes:
            logger.info(f"Cache size ({current_size / 1024 / 1024:.2f} MB) exceeds limit, cleaning up...")
            
            # Get all cache files sorted by access time (oldest first)
            cache_files = []
            for entry in self.cache_dir.iterdir():
                if entry.is_file():
                    cache_files.append((entry.stat().st_atime, entry))
            
            cache_files.sort()
            
            # Remove oldest files until we're under the limit
            target_size = self.max_size_bytes * 0.8  # Clean to 80% of max
            for _, file_path in cache_files:
                if current_size <= target_size:
                    break
                file_size = file_path.stat().st_size
                file_path.unlink()
                current_size -= file_size
                logger.debug(f"Removed cache entry: {file_path.name}")

    def clear(self):
        """Clear all cache entries."""
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
            self._ensure_cache_dir()
            logger.info("Code cache cleared")

    def get_stats(self) -> Dict[str, any]:
        """Get cache statistics."""
        if not self.cache_dir.exists():
            return {
                'num_files': 0,
                'total_size_mb': 0,
                'max_size_mb': self.max_size_bytes / 1024 / 1024
            }
        
        num_files = sum(1 for _ in self.cache_dir.iterdir() if _.is_file())
        total_size = self._get_cache_size()
        
        return {
            'num_files': num_files,
            'total_size_mb': total_size / 1024 / 1024,
            'max_size_mb': self.max_size_bytes / 1024 / 1024,
            'cache_dir': str(self.cache_dir)
        }


# Global singleton instance
_global_cache: Optional[CodeCache] = None


def get_code_cache(enabled: bool = True, **kwargs) -> Optional[CodeCache]:
    """
    Get or create the global code cache instance.
    
    Args:
        enabled: Whether caching is enabled
        **kwargs: Additional arguments to pass to CodeCache constructor
        
    Returns:
        CodeCache instance if enabled, None otherwise
    """
    if not enabled:
        return None
    
    global _global_cache
    if _global_cache is None:
        _global_cache = CodeCache(**kwargs)
    return _global_cache

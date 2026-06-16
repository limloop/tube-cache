"""
Storage management for video files.

Handles:
- Storage monitoring and cleanup
- File integrity checks
- Video file discovery with auto-migration
- Storage statistics
"""

import os
import asyncio
import time
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from app.config import settings
from app.database import db
from app.file_utils import find_video_file, get_all_video_files
from app.utils import check_video_file_integrity
from app import logger


class StorageManager:
    """
    Storage management for video files.
    
    Features:
    - Storage usage monitoring
    - Automatic cleanup of old videos when storage is full
    - Video file integrity checking
    - Old log file cleanup
    """
    
    # Supported video formats for file discovery
    VIDEO_EXTENSIONS = ['.mp4', '.webm', '.mkv', '.avi', '.mov', '.flv', '.wmv']
    
    def __init__(self):
        """Initialize storage manager."""
        self.videos_path = Path(settings.storage.videos_path)
        self.max_size_bytes = settings.storage.max_size_gb * (1024 ** 3)
        
        # Configuration
        self.monitoring_interval = settings.storage.monitoring_interval
        self.storage_cleanup_threshold = settings.storage.cleanup_threshold
        self.target_free_space = settings.storage.target_free_space
        self.log_retention_days = settings.storage.log_retention_days
        self.log_check_interval = settings.storage.log_check_interval
        self.integrity_check_interval = settings.storage.integrity_check_interval
        
        # State
        self._last_log_cleanup = 0
        self._last_integrity_check = 0
        self._integrity_check_running = False
        self._cleanup_running = False
        self._monitor_task = None
        self._is_monitoring = False
    
    async def start_monitoring(self):
        """Start background storage monitoring."""
        if self._is_monitoring:
            return
            
        self._is_monitoring = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Storage monitoring started")
    
    async def stop_monitoring(self):
        """Stop background storage monitoring."""
        self._is_monitoring = False
        
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            
        logger.info("Storage monitoring stopped")
    
    async def _monitor_loop(self):
        """Main monitoring loop."""
        logger.debug("Storage monitoring loop started")
        
        try:
            while self._is_monitoring:
                await asyncio.sleep(self.monitoring_interval)
                await self._perform_monitoring_checks()
                
        except asyncio.CancelledError:
            logger.debug("Storage monitoring loop stopped")
        except Exception as e:
            logger.error(f"Storage monitoring error: {e}")
        finally:
            self._is_monitoring = False
    
    async def _perform_monitoring_checks(self):
        """Perform all monitoring checks."""
        current_time = time.time()
        
        # 1. Check storage usage
        storage_info = await self.get_storage_info()
        if (storage_info['used_percent'] >= self.storage_cleanup_threshold and 
            not self._cleanup_running):
            logger.warning(
                f"Storage {storage_info['used_percent']:.1f}% full, "
                "triggering cleanup"
            )
            asyncio.create_task(self._safe_cleanup_old_videos())
        
        # 2. Check logs
        if current_time - self._last_log_cleanup > self.log_check_interval:
            await self.cleanup_old_logs()
            self._last_log_cleanup = current_time
        
        # 3. Check integrity
        if (current_time - self._last_integrity_check > self.integrity_check_interval and 
            not self._integrity_check_running):
            asyncio.create_task(self._safe_check_video_integrity())
    
    async def _safe_cleanup_old_videos(self):
        """Safely cleanup old videos (prevents concurrent runs)."""
        if self._cleanup_running:
            logger.debug("Cleanup already running, skipping")
            return
            
        self._cleanup_running = True
        try:
            await self.cleanup_old_videos()
        finally:
            self._cleanup_running = False
    
    async def _safe_check_video_integrity(self):
        """Safely check integrity (prevents concurrent runs)."""
        if self._integrity_check_running:
            logger.debug("Integrity check already running, skipping")
            return
            
        self._integrity_check_running = True
        try:
            await self.check_all_video_integrity()
            self._last_integrity_check = time.time()
        finally:
            self._integrity_check_running = False

    async def cleanup_old_videos(self) -> List[str]:
        """
        Remove oldest videos to free up storage space.
        
        Returns:
            List of deleted video hashes
        """
        if self._cleanup_running:
            return []
            
        self._cleanup_running = True
        deleted_hashes = []
        
        try:
            # Get all ready videos sorted by last access (oldest first)
            videos = await db.get_all_ready_videos()
            videos.sort(key=lambda x: x.get('last_accessed', 0) or 0)
            
            current_size = sum(v.get('file_size', 0) for v in videos)
            target_size = self.max_size_bytes * (1 - self.target_free_space / 100)
            
            # Check if cleanup is needed
            if current_size <= target_size:
                return deleted_hashes
            
            # Delete oldest videos until target is reached
            for video in videos:
                if current_size <= target_size:
                    break
                    
                video_hash = video['hash']
                file_size = video.get('file_size', 0)
                
                if not file_size:
                    continue
                
                # Find file using file_utils (handles both old and new structure)
                file_path = await self.find_video_path(video_hash)
                
                if file_path and file_path.exists():
                    try:
                        file_path.unlink()
                        await db.mark_video_deleted(video_hash)
                        
                        current_size -= file_size
                        deleted_hashes.append(video_hash)
                        
                        logger.info(f"Deleted old video: {video_hash[:12]} ({file_size:,} bytes)")
                        
                    except Exception as e:
                        logger.error(f"Failed to delete video {video_hash[:12]}: {e}")
            
            if deleted_hashes:
                logger.info(f"Cleanup complete: removed {len(deleted_hashes)} videos")
                
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
        finally:
            self._cleanup_running = False
        
        return deleted_hashes
    
    async def cleanup_old_logs(self) -> List[str]:
        """
        Delete log files older than retention period.
        
        Returns:
            List of deleted file names
        """
        deleted_files = []
        logs_dir = Path(settings.storage.logs_path)
        
        if not logs_dir.exists():
            return deleted_files
        
        cutoff_date = datetime.now().timestamp() - (self.log_retention_days * 86400)
        
        for log_file in logs_dir.glob("*.log"):
            if not log_file.is_file():
                continue
            
            try:
                file_date = self._get_file_date(log_file)
                
                if file_date.timestamp() < cutoff_date:
                    log_file.unlink(missing_ok=True)
                    deleted_files.append(log_file.name)
                    
            except Exception as e:
                logger.warning(f"Failed to process {log_file.name}: {e}")
        
        if deleted_files:
            logger.info(f"Cleaned {len(deleted_files)} log files")
        
        return deleted_files
    
    def _get_file_date(self, file_path: Path) -> datetime:
        """
        Determine file date from name or modification time.
        
        Tries to extract date from filename (format: name_YYYY-MM-DD.log)
        Falls back to modification time.
        """
        # Try to extract date from filename
        date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', file_path.stem)
        if date_match:
            year, month, day = map(int, date_match.groups())
            return datetime(year, month, day)
        
        # Fallback to modification time
        return datetime.fromtimestamp(file_path.stat().st_mtime)
    
    async def check_all_video_integrity(self) -> List[str]:
        """
        Check integrity of all video files.
        
        Returns:
            List of corrupted video hashes
        """
        damaged_files = []
        start_time = time.time()
        
        try:
            videos = await db.get_all_ready_videos()
            total_videos = len(videos)
            
            if total_videos == 0:
                logger.debug("No videos to check")
                return damaged_files
            
            logger.info(f"Starting integrity check: {total_videos} videos...")
            
            for index, video in enumerate(videos, 1):
                if not self._is_monitoring:
                    logger.info("Integrity check interrupted")
                    break
                    
                video_hash = video['hash']
                file_path = await self.find_video_path(video_hash)
                
                if file_path and file_path.exists():
                    try:
                        if not check_video_file_integrity(file_path):
                            damaged_files.append(video_hash)
                            logger.warning(f"Corrupted file: {video_hash[:12]}")
                            
                            # Auto-delete corrupted file
                            file_path.unlink(missing_ok=True)
                            await db.mark_video_deleted(video_hash)
                            
                    except Exception as e:
                        logger.error(f"Failed to check {video_hash[:12]}: {e}")
                
                # Log progress
                if index % max(10, total_videos // 10) == 0:
                    progress = (index / total_videos) * 100
                    logger.debug(f"Progress: {progress:.0f}% ({index}/{total_videos})")
            
            elapsed = time.time() - start_time
            logger.info(
                f"Integrity check complete: {total_videos} checked, "
                f"{len(damaged_files)} corrupted, {elapsed:.1f}s"
            )
            
        except Exception as e:
            logger.error(f"Integrity check error: {e}")
        
        return damaged_files
    
    async def find_video_path(self, video_hash: str) -> Optional[Path]:
        """
        Find a video file by hash using file_utils.
        
        Handles both old (root) and new (subdir) structures.
        Auto-migrates old files to new structure.
        
        Args:
            video_hash: 64-character hash
            
        Returns:
            Path to video file if found, None otherwise
        """
        return find_video_file(video_hash)
    
    async def get_storage_info(self) -> Dict[str, Any]:
        """
        Get current storage statistics.
        
        Returns:
            Dict with storage usage information
        """
        try:
            stats = await db.get_storage_stats()
            used_bytes = stats.get('total_size', 0)
            
            used_percent = 0
            if self.max_size_bytes > 0:
                used_percent = (used_bytes / self.max_size_bytes) * 100
            
            return {
                'total_size_bytes': used_bytes,
                'max_size_bytes': self.max_size_bytes,
                'video_count': stats.get('video_count', 0),
                'used_percent': round(used_percent, 1),
                'free_bytes': max(0, self.max_size_bytes - used_bytes),
                'free_percent': round(max(0, 100 - used_percent), 1)
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage info: {e}")
            return self._get_empty_storage_info()
    
    def _get_empty_storage_info(self) -> Dict[str, Any]:
        """Return empty storage info on error."""
        return {
            'total_size_bytes': 0,
            'max_size_bytes': self.max_size_bytes,
            'video_count': 0,
            'used_percent': 0,
            'free_bytes': self.max_size_bytes,
            'free_percent': 100
        }
    
    def is_monitoring_active(self) -> bool:
        """Check if monitoring is active."""
        return self._is_monitoring


# Global instance
storage = StorageManager()
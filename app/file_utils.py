"""
File utilities for video storage management.

Handles:
- Finding video files in new (subdir) and old (root) formats
- Auto-migration of old files to new structure
- Path management with 4-character prefix subdirectories
"""

from pathlib import Path
from typing import Optional, List
import shutil

from app.config import settings
from app import logger


# Supported video extensions
VIDEO_EXTENSIONS = ['.mp4', '.webm', '.mkv', '.avi', '.mov', '.flv', '.wmv']


def get_video_subdir(video_hash: str) -> str:
    """
    Get subdirectory name for a video hash.
    
    Args:
        video_hash: 64-character hash
        
    Returns:
        First 4 characters of hash
    """
    return video_hash[:4]


def get_video_path_in_subdir(video_hash: str, ext: str) -> Path:
    """
    Get the expected path for a video in the new subdirectory structure.
    
    Args:
        video_hash: 64-character hash
        ext: File extension (with dot, e.g., '.mp4')
        
    Returns:
        Path object: data/videos/{hash[:4]}/{hash}{ext}
    """
    subdir = get_video_subdir(video_hash)
    videos_dir = Path(settings.storage.videos_path)
    return videos_dir / subdir / f"{video_hash}{ext}"


def get_video_path_in_root(video_hash: str, ext: str) -> Path:
    """
    Get the expected path for a video in the old root structure.
    
    Args:
        video_hash: 64-character hash
        ext: File extension (with dot, e.g., '.mp4')
        
    Returns:
        Path object: data/videos/{hash}{ext}
    """
    videos_dir = Path(settings.storage.videos_path)
    return videos_dir / f"{video_hash}{ext}"


def find_video_file(video_hash: str) -> Optional[Path]:
    """
    Find a video file by hash.
    
    Search order:
    1. New subdirectory structure: data/videos/{hash[:4]}/{hash}.{ext}
    2. Old root structure: data/videos/{hash}.{ext}
    3. If found in root, auto-migrate to subdirectory
    
    Args:
        video_hash: 64-character hash
        
    Returns:
        Path to video file if found, None otherwise
    """
    videos_dir = Path(settings.storage.videos_path)
    
    if not videos_dir.exists():
        return None
    
    # 1. Search in subdirectory (new format)
    subdir = get_video_subdir(video_hash)
    subdir_path = videos_dir / subdir
    
    if subdir_path.exists():
        for ext in VIDEO_EXTENSIONS:
            file_path = subdir_path / f"{video_hash}{ext}"
            if file_path.exists():
                return file_path
    
    # 2. Search in root (old format)
    for ext in VIDEO_EXTENSIONS:
        file_path = videos_dir / f"{video_hash}{ext}"
        if file_path.exists():
            # Found in root - auto-migrate to subdirectory
            logger.info(f"Found video in root, migrating: {video_hash[:12]}...")
            new_path = _migrate_video_to_subdir(file_path, video_hash)
            if new_path:
                return new_path
            else:
                # Migration failed, return old path
                return file_path
    
    return None


def _migrate_video_to_subdir(file_path: Path, video_hash: str) -> Optional[Path]:
    """
    Migrate a video file from root to subdirectory structure.
    
    Args:
        file_path: Current file path (in root)
        video_hash: 64-character hash
        
    Returns:
        New file path if successful, None otherwise
    """
    try:
        # Get extension from current file
        ext = file_path.suffix
        if not ext:
            return None
        
        # Build new path
        subdir = get_video_subdir(video_hash)
        videos_dir = Path(settings.storage.videos_path)
        new_dir = videos_dir / subdir
        new_path = new_dir / f"{video_hash}{ext}"
        
        # Create subdirectory
        new_dir.mkdir(parents=True, exist_ok=True)
        
        # Move file
        shutil.move(str(file_path), str(new_path))
        logger.info(f"Migrated video: {file_path.name} -> {new_path}")
        
        return new_path
        
    except Exception as e:
        logger.error(f"Failed to migrate video {video_hash[:12]}...: {e}")
        return None


def ensure_video_subdir(video_hash: str) -> Path:
    """
    Ensure the subdirectory for a video exists.
    
    Args:
        video_hash: 64-character hash
        
    Returns:
        Path to the subdirectory
    """
    subdir = get_video_subdir(video_hash)
    videos_dir = Path(settings.storage.videos_path)
    subdir_path = videos_dir / subdir
    subdir_path.mkdir(parents=True, exist_ok=True)
    return subdir_path


def get_all_video_files() -> List[Path]:
    """
    Get all video files in the storage (both old and new structure).
    
    Returns:
        List of all video file paths
    """
    videos_dir = Path(settings.storage.videos_path)
    if not videos_dir.exists():
        return []
    
    files = []
    
    # Search in root (old format)
    for ext in VIDEO_EXTENSIONS:
        files.extend(videos_dir.glob(f"*{ext}"))
    
    # Search in subdirectories (new format)
    for subdir in videos_dir.iterdir():
        if subdir.is_dir():
            for ext in VIDEO_EXTENSIONS:
                files.extend(subdir.glob(f"*{ext}"))
    
    return files
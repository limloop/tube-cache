"""
Utility functions with precompiled regular expressions
"""

import re
import hashlib
import subprocess
import asyncio
import shutil
import json
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
from typing import Tuple, Dict, Any, Optional

from app.config import settings
from app import logger


# Precompiled regular expressions for performance
_UNSAFE_CHARS_PATTERN = re.compile(r'[<>:"/\\|?*\x00-\x1F\x7F]')
_MULTIPLE_SPACES_PATTERN = re.compile(r'\s+')
_WHITESPACE_PATTERN = re.compile(r'^\s+|\s+$')


# ============================================
# Title & URL Processing
# ============================================

def normalize_title(title: str) -> str:
    """
    Clean video title from dangerous characters only.
    Preserves emoji, non-latin characters and original format.
    
    Args:
        title: Original title
        
    Returns:
        Cleaned title for safe database storage
    """
    if not title or not isinstance(title, str):
        return ""
    
    # Remove dangerous filesystem characters only
    title = _UNSAFE_CHARS_PATTERN.sub('', title)
    
    # Replace multiple spaces with single
    title = _MULTIPLE_SPACES_PATTERN.sub(' ', title)
    
    # Strip whitespace from edges
    title = _WHITESPACE_PATTERN.sub('', title)
    
    return title


def extract_domain(url: str) -> str:
    """
    Extract clean domain from URL.
    
    Args:
        url: Video URL
        
    Returns:
        Domain without www (e.g., 'youtube.com')
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Remove www, port and lowercase
        domain = domain.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        
        # Remove port if present
        if ':' in domain:
            domain = domain.split(':')[0]
            
        return domain
    except Exception:
        return "unknown"


def generate_video_hash(url: str, quality_params: str = "") -> str:
    """
    Generate unique 64-character SHA256 hash for video.
    
    Args:
        url: Video URL
        quality_params: Quality parameters from config
        
    Returns:
        64-character hex hash
    """
    combined = f"{url}|{quality_params}"
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()


def get_download_config_for_url(url: str) -> Tuple[str, bool]:
    """
    Get download configuration for specific URL.
    
    Args:
        url: Video URL
        
    Returns:
        Tuple of (format, extract_audio)
    """
    domain = extract_domain(url)
    
    # Look for domain-specific config
    if domain in settings.sources:
        source_config = settings.sources[domain]
    else:
        # Use default config
        source_config = settings.sources.get("default")
    
    return source_config.format, source_config.extract_audio


def format_file_size(size_bytes: Optional[int]) -> str:
    """
    Format file size to human readable.
    
    Args:
        size_bytes: Size in bytes or None
        
    Returns:
        Formatted string
    """
    if size_bytes is None:
        return "Unknown"
    
    units = ['B', 'KB', 'MB', 'GB']
    size = float(size_bytes)
    
    for unit in units:
        if size < 1024.0 or unit == 'GB':
            if unit == 'B':
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    
    return f"{size:.1f} TB"


def normalize_youtube_url(url: str) -> str:
    """
    Normalize YouTube URL to standard format.
    
    Extracts only video_id, removes all other parameters.
    
    Returns: https://www.youtube.com/watch?v=VIDEO_ID
    
    Args:
        url: Any YouTube URL
        
    Returns:
        Normalized URL with only video_id
    """
    try:
        # Add scheme if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Normalize domain
        if domain in ['youtu.be', 'www.youtu.be']:
            # Short youtu.be links
            video_id = parsed.path.strip('/').split('?')[0]
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"
        
        elif 'youtube.com' in domain:
            # All youtube.com variants
            query_params = parse_qs(parsed.query)
            video_id = query_params.get('v', [None])[0]
            
            # Check other formats if v param missing
            if not video_id:
                # Check /embed/VIDEO_ID format
                if '/embed/' in parsed.path:
                    video_id = parsed.path.split('/embed/')[1].split('?')[0].split('/')[0]
                # Check /v/VIDEO_ID format
                elif '/v/' in parsed.path:
                    video_id = parsed.path.split('/v/')[1].split('?')[0].split('/')[0]
            
            if video_id:
                # Clean video_id from extra params
                video_id = video_id.split('&')[0].split('?')[0].split('#')[0]
                
                # Create normalized URL with ONLY v parameter
                normalized_query = urlencode({'v': video_id})
                normalized_url = urlunparse((
                    'https',
                    'www.youtube.com',
                    '/watch',
                    '',
                    normalized_query,
                    ''
                ))
                return normalized_url
        
        # Not recognized as YouTube
        return url
        
    except Exception:
        return url


def clean_and_validate_url(url: str) -> Optional[str]:
    """
    Clean URL from quotes and extra characters, validate.
    
    Args:
        url: Raw URL (may have quotes, spaces, etc.)
        
    Returns:
        Cleaned valid URL or None if invalid
    """
    if not url or not isinstance(url, str):
        return None
    
    # Strip whitespace
    url = url.strip()
    
    # Remove quotes of all types
    quotes = ['"', "'", '`', '```', '````', '«', '»', '“', '”']
    for quote in quotes:
        if url.startswith(quote) and url.endswith(quote):
            url = url[1:-1].strip()
    
    # Remove angle brackets
    if url.startswith('<') and url.endswith('>'):
        url = url[1:-1].strip()
    
    # Must contain a dot (domain) and slash or question mark
    if '.' not in url:
        return None
    
    # Minimum length check
    if len(url) < 10:
        return None
    
    # Add scheme if missing
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Validate via urlparse
    try:
        parsed = urlparse(url)
        
        # Must have domain
        if not parsed.netloc or '.' not in parsed.netloc:
            return None
        
        # Domain must be at least 3 chars
        if len(parsed.netloc) < 3:
            return None
        
        return url
        
    except Exception:
        return None


def normalize_video_url(url: str) -> Optional[str]:
    """
    Recognize and normalize video URL.
    
    Args:
        url: Video URL (may have quotes, spaces, etc.)
        
    Returns:
        Normalized URL or None if invalid
    """
    # Clean URL first
    cleaned_url = clean_and_validate_url(url)
    
    if not cleaned_url:
        return None
    
    try:
        parsed = urlparse(cleaned_url)
        domain = parsed.netloc.lower()
        
        # Remove www.
        if domain.startswith('www.'):
            domain = domain[4:]
        
        # Normalize by domain
        if domain in ['youtube.com', 'youtu.be']:
            return normalize_youtube_url(cleaned_url)
        
        # Default: return cleaned URL
        return cleaned_url
        
    except Exception:
        return cleaned_url


# ============================================
# File Integrity Checking
# ============================================

def check_video_file_integrity(file_path: Path) -> bool:
    """
    Simple file integrity check.
    
    Args:
        file_path: Path to video file
        
    Returns:
        True if file appears valid
    """
    try:
        if not file_path.exists():
            return False
        
        file_size = file_path.stat().st_size
        if file_size == 0:
            return False
        
        # Minimum size for video/audio
        if file_size < 1024 * 10:  # 10KB
            return False
        
        # Try to read file header
        with open(file_path, 'rb') as f:
            header = f.read(1024)
            if len(header) == 0:
                return False
            
            # Check file is not all zeros
            if all(b == 0 for b in header[:100]):
                return False
        
        return True
        
    except Exception:
        return False


async def check_video_file_integrity_extended(
    file_path: Path, 
    expected_size: Optional[int] = None
) -> Dict[str, Any]:
    """
    Extended file integrity check with ffprobe support.
    
    Args:
        file_path: Path to file
        expected_size: Expected file size in bytes (optional)
    
    Returns:
        Dict with validation results
    """
    try:
        # 1. Check file exists
        if not file_path.exists():
            return {
                'valid': False,
                'reason': 'File does not exist',
                'file_size': 0,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
        # 2. Check file size
        file_size = file_path.stat().st_size
        
        if file_size == 0:
            return {
                'valid': False,
                'reason': 'File is empty (0 bytes)',
                'file_size': 0,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
        # 3. Minimum size check
        MIN_SIZE = 1024 * 10  # 10KB
        if file_size < MIN_SIZE:
            return {
                'valid': False,
                'reason': f'File too small ({file_size} bytes)',
                'file_size': file_size,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
        # 4. Expected size check (if provided)
        if expected_size and abs(file_size - expected_size) > (expected_size * 0.1):
            return {
                'valid': False,
                'reason': f'Size mismatch: {file_size} != {expected_size}',
                'file_size': file_size,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
        # 5. Quick read test
        try:
            with open(file_path, 'rb') as f:
                header = f.read(1024)
                if len(header) < 100:
                    return {
                        'valid': False,
                        'reason': 'Cannot read file header',
                        'file_size': file_size,
                        'has_video_stream': False,
                        'has_audio_stream': False,
                        'duration': None
                    }
                
                # Check for all-zero file
                if all(b == 0 for b in header[:100]):
                    return {
                        'valid': False,
                        'reason': 'File contains only zeros',
                        'file_size': file_size,
                        'has_video_stream': False,
                        'has_audio_stream': False,
                        'duration': None
                    }
                
        except Exception as e:
            return {
                'valid': False,
                'reason': f'Read error: {str(e)}',
                'file_size': file_size,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
        # 6. ffprobe check (if available)
        ffprobe_result = await _check_with_ffprobe(file_path)
        
        if ffprobe_result['valid']:
            return {
                'valid': True,
                'reason': None,
                'file_size': file_size,
                'has_video_stream': ffprobe_result.get('has_video_stream', False),
                'has_audio_stream': ffprobe_result.get('has_audio_stream', False),
                'duration': ffprobe_result.get('duration')
            }
        
        # 7. Basic format checks without ffprobe
        extension = file_path.suffix.lower()
        
        # MP4 files need 'ftyp' atom
        if extension in ['.mp4', '.m4a']:
            if b'ftyp' not in header:
                return {
                    'valid': False,
                    'reason': 'MP4 file missing ftyp atom',
                    'file_size': file_size,
                    'has_video_stream': False,
                    'has_audio_stream': False,
                    'duration': None
                }
        
        # File passes basic checks
        return {
            'valid': True,
            'reason': None,
            'file_size': file_size,
            'has_video_stream': True,
            'has_audio_stream': True,
            'duration': None
        }
        
    except Exception as e:
        logger.error(f"Integrity check error for {file_path}: {e}")
        return {
            'valid': False,
            'reason': f'Check error: {str(e)}',
            'file_size': 0,
            'has_video_stream': False,
            'has_audio_stream': False,
            'duration': None
        }


async def _check_with_ffprobe(file_path: Path) -> Dict[str, Any]:
    """
    Check file with ffprobe for detailed validation.
    """
    try:
        # Check if ffprobe is available
        if not await check_tool('ffprobe', '-version'):
            logger.debug("ffprobe not available")
            return {'valid': True}  # Assume valid if ffprobe missing
        
        # Run ffprobe
        process = await asyncio.create_subprocess_exec(
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration,size',
            '-show_entries', 'stream=codec_type',
            '-of', 'json',
            str(file_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode('utf-8', errors='ignore').strip()
            
            # Ignore non-critical errors
            if "moov atom not found" in error_msg:
                logger.warning(f"ffprobe warning for {file_path}: moov atom not found")
                return {'valid': True, 'has_video_stream': True, 'has_audio_stream': True}
            
            logger.error(f"ffprobe error for {file_path}: {error_msg}")
            return {'valid': False}
        
        # Parse JSON output
        try:
            probe_data = json.loads(stdout.decode('utf-8', errors='ignore'))
            
            has_video_stream = False
            has_audio_stream = False
            duration = None
            
            # Check streams
            if 'streams' in probe_data:
                for stream in probe_data['streams']:
                    if stream.get('codec_type') == 'video':
                        has_video_stream = True
                    elif stream.get('codec_type') == 'audio':
                        has_audio_stream = True
            
            # Get duration
            if 'format' in probe_data and 'duration' in probe_data['format']:
                try:
                    duration = float(probe_data['format']['duration'])
                except (ValueError, TypeError):
                    pass
            
            # Audio files need audio stream
            if file_path.suffix.lower() in ['.mp3', '.m4a', '.aac', '.flac', '.wav']:
                if has_audio_stream:
                    return {
                        'valid': True,
                        'has_video_stream': has_video_stream,
                        'has_audio_stream': has_audio_stream,
                        'duration': duration
                    }
                else:
                    return {'valid': False}
            
            # Video files need at least one stream
            if has_video_stream or has_audio_stream:
                return {
                    'valid': True,
                    'has_video_stream': has_video_stream,
                    'has_audio_stream': has_audio_stream,
                    'duration': duration
                }
            else:
                return {'valid': False}
                
        except json.JSONDecodeError as e:
            logger.error(f"ffprobe JSON parse error: {e}")
            return {'valid': True}  # Assume valid on parse error
            
    except Exception as e:
        logger.error(f"ffprobe execution error: {e}")
        return {'valid': True}  # Assume valid on error


# ============================================
# Date & Sorting Utilities
# ============================================

def get_date_sort_key(item: dict, key: str):
    """
    Get sortable timestamp from dict item with date field.
    
    Args:
        item: Dictionary containing date field
        key: Key name for date field
        
    Returns:
        Unix timestamp or 0 if invalid
    """
    val = item.get(key)
    if not val:
        return 0
    
    if isinstance(val, (int, float)):
        return val
    
    if isinstance(val, str):
        try:
            # Try different date formats
            formats = [
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(val, fmt)
                    return dt.timestamp()
                except ValueError:
                    continue
            return 0
        except:
            return 0
    
    return 0


# ============================================
# Dependency Checking
# ============================================

async def check_tool(command: str, *args) -> bool:
    """
    Check if a system tool is available in PATH.
    
    Args:
        command: Command name (e.g., 'ffmpeg')
        *args: Arguments to pass (e.g., '--version')
    
    Returns:
        True if tool exists and returns exit code 0, False otherwise
    """
    # First check if command exists in PATH
    if not shutil.which(command):
        logger.debug(f"Tool '{command}' not found in PATH")
        return False
    
    try:
        # Try to run the command with given args
        cmd = [command] + list(args)
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await process.communicate()
        
        return process.returncode == 0
        
    except Exception as e:
        logger.debug(f"Failed to run '{command}': {e}")
        return False
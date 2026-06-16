"""
Utility functions with precompiled regular expressions
"""

import re
import hashlib
import subprocess
import asyncio
import shutil
import json
import aiohttp
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
from typing import Tuple, Dict, Any, Optional, List

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
    """
    if not title or not isinstance(title, str):
        return ""
    
    title = _UNSAFE_CHARS_PATTERN.sub('', title)
    title = _MULTIPLE_SPACES_PATTERN.sub(' ', title)
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
        domain = parsed.netloc.lower()
        
        if domain.startswith('www.'):
            domain = domain[4:]
        
        if ':' in domain:
            domain = domain.split(':')[0]
            
        return domain
    except Exception:
        return "unknown"


def generate_video_hash(url: str, quality_params: str = "") -> str:
    """Generate unique 64-character SHA256 hash for video."""
    combined = f"{url}|{quality_params}"
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()


def get_download_config_for_url(url: str) -> Tuple[str, bool]:
    """Get download configuration for specific URL."""
    domain = extract_domain(url)
    
    if domain in settings.sources:
        source_config = settings.sources[domain]
    else:
        source_config = settings.sources.get("default")
    
    return source_config.format, source_config.extract_audio


def format_file_size(size_bytes: Optional[int]) -> str:
    """Format file size to human readable."""
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


# ============================================
# URL Normalization (config-driven)
# ============================================

def normalize_url_by_rules(url: str) -> str:
    """
    Normalize URL using rules from config.
    
    Uses normalization_rules from url_filter config.
    Supports:
    - keep_params: comma-separated list of params to keep
    - normalize_to: template with {video_id} placeholder
    - video_id_pattern: regex to extract video_id
    """
    try:
        domain = extract_domain(url)
        rules = settings.url_filter.normalization_rules
        
        for rule_domain, rule in rules.items():
            if domain == rule_domain or domain == f"www.{rule_domain}":
                pattern = rule.get('video_id_pattern')
                if pattern:
                    match = re.search(pattern, url)
                    if match:
                        video_id = match.group(1)
                        template = rule.get('normalize_to')
                        
                        if template and '{video_id}' in template:
                            normalized = template.format(video_id=video_id)
                            
                            # Handle keep_params if present
                            keep_params = rule.get('keep_params')
                            if keep_params:
                                # Parse original URL params
                                parsed = urlparse(url)
                                query_params = parse_qs(parsed.query)
                                
                                # Keep only specified params
                                keep_list = [p.strip() for p in keep_params.split(',') if p.strip()]
                                if keep_list and query_params:
                                    filtered_params = {}
                                    for param in keep_list:
                                        if param in query_params:
                                            filtered_params[param] = query_params[param][0]
                                    
                                    if filtered_params:
                                        # Parse normalized URL and add filtered params
                                        norm_parsed = urlparse(normalized)
                                        existing_params = parse_qs(norm_parsed.query)
                                        
                                        # Merge with existing params (keep_params override)
                                        for key, value in filtered_params.items():
                                            existing_params[key] = [value]
                                        
                                        new_query = urlencode(existing_params, doseq=True)
                                        normalized = urlunparse((
                                            norm_parsed.scheme,
                                            norm_parsed.netloc,
                                            norm_parsed.path,
                                            norm_parsed.params,
                                            new_query,
                                            norm_parsed.fragment
                                        ))
                            
                            return normalized
        
        return url
        
    except Exception as e:
        logger.warning(f"Normalization error for {url}: {e}")
        return url


def clean_and_validate_url(url: str) -> Optional[str]:
    """
    Clean URL from quotes and extra characters, validate basic format.
    """
    if not url or not isinstance(url, str):
        return None
    
    url = url.strip()
    
    quotes = ['"', "'", '`', '```', '````', '«', '»', '“', '”']
    for quote in quotes:
        if url.startswith(quote) and url.endswith(quote):
            url = url[1:-1].strip()
    
    if url.startswith('<') and url.endswith('>'):
        url = url[1:-1].strip()
    
    if '.' not in url:
        return None
    
    if len(url) < 10:
        return None
    
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    try:
        parsed = urlparse(url)
        if not parsed.netloc or '.' not in parsed.netloc:
            return None
        if len(parsed.netloc) < 3:
            return None
        return url
    except Exception:
        return None


def normalize_video_url(url: str) -> Optional[str]:
    """
    Full URL normalization pipeline:
    1. Clean and validate basic format
    2. Apply config-driven normalization rules
    """
    cleaned = clean_and_validate_url(url)
    if not cleaned:
        return None
    
    return normalize_url_by_rules(cleaned)


# ============================================
# URL Filtering (config-driven)
# ============================================

def is_domain_allowed(url: str) -> bool:
    """
    Check if URL domain is in allowed list.
    
    Returns True if:
    - allowed_domains is empty (all domains allowed)
    - domain matches an entry in allowed_domains
    """
    if not settings.url_filter.allowed_domains:
        return True
    
    try:
        domain = extract_domain(url)
        for allowed in settings.url_filter.allowed_domains:
            if domain == allowed or domain == f"www.{allowed}":
                return True
        
        logger.debug(f"Domain '{domain}' not in allowed list")
        return False
        
    except Exception as e:
        logger.warning(f"Failed to check domain for {url}: {e}")
        return False


def is_url_allowed(url: str) -> bool:
    """
    Check if URL passes all filters.
    
    Currently checks:
    - Domain whitelist (is_domain_allowed)
    """
    return is_domain_allowed(url)


# ============================================
# URL Validation (HEAD request)
# ============================================

async def validate_url(url: str) -> tuple[bool, Optional[str]]:
    """
    Validate URL by making a HEAD request.
    
    Returns:
        (is_valid, error_message)
    """
    if not settings.url_filter.validation:
        return True, None
    
    try:
        timeout = aiohttp.ClientTimeout(total=settings.url_filter.validation_timeout)
        headers = {
            'User-Agent': settings.url_filter.validation_user_agent,
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.head(url, headers=headers, allow_redirects=True) as response:
                    if response.status < 400:
                        return True, None
                    
                    # Some servers block HEAD, try GET with range
                    if response.status in [403, 405, 501]:
                        headers['Range'] = 'bytes=0-0'
                        async with session.get(url, headers=headers, allow_redirects=True) as get_response:
                            if get_response.status < 400:
                                return True, None
                    
                    return False, f"HTTP {response.status}"
                    
            except aiohttp.ClientError as e:
                return False, f"Connection error: {str(e)}"
            except asyncio.TimeoutError:
                return False, "Connection timeout"
                
    except Exception as e:
        logger.warning(f"URL validation error for {url}: {e}")
        return False, f"Validation error: {str(e)}"


async def process_url(url: str) -> tuple[Optional[str], Optional[str]]:
    """
    Full URL processing pipeline:
    1. Normalize
    2. Filter (allowed domains)
    3. Validate (HEAD request)
    
    Returns:
        (processed_url, error_message)
    """
    # 1. Normalize
    normalized = normalize_video_url(url)
    if not normalized:
        return None, "Invalid URL format"
    
    # 2. Filter
    if not is_url_allowed(normalized):
        allowed_domains = settings.url_filter.allowed_domains
        if allowed_domains:
            return None, f"Domain not allowed. Allowed: {', '.join(allowed_domains)}"
        return None, "Domain not allowed"
    
    # 3. Validate
    valid, error = await validate_url(normalized)
    if not valid:
        return None, f"URL validation failed: {error}"
    
    return normalized, None


# ============================================
# File Integrity Checking
# ============================================

def check_video_file_integrity(file_path: Path) -> bool:
    """Simple file integrity check."""
    try:
        if not file_path.exists():
            return False
        
        file_size = file_path.stat().st_size
        if file_size == 0:
            return False
        
        if file_size < 1024 * 10:  # 10KB
            return False
        
        with open(file_path, 'rb') as f:
            header = f.read(1024)
            if len(header) == 0:
                return False
            if all(b == 0 for b in header[:100]):
                return False
        
        return True
    except Exception:
        return False


async def check_video_file_integrity_extended(
    file_path: Path, 
    expected_size: Optional[int] = None
) -> Dict[str, Any]:
    """Extended file integrity check with ffprobe support."""
    try:
        if not file_path.exists():
            return {
                'valid': False,
                'reason': 'File does not exist',
                'file_size': 0,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
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
        
        MIN_SIZE = 1024 * 10
        if file_size < MIN_SIZE:
            return {
                'valid': False,
                'reason': f'File too small ({file_size} bytes)',
                'file_size': file_size,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
        if expected_size and abs(file_size - expected_size) > (expected_size * 0.1):
            return {
                'valid': False,
                'reason': f'Size mismatch: {file_size} != {expected_size}',
                'file_size': file_size,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
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
        
        extension = file_path.suffix.lower()
        
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
    """Check file with ffprobe for detailed validation."""
    try:
        if not await check_tool('ffprobe', '-version'):
            return {'valid': True}
        
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
            if "moov atom not found" in error_msg:
                return {'valid': True, 'has_video_stream': True, 'has_audio_stream': True}
            return {'valid': False}
        
        try:
            probe_data = json.loads(stdout.decode('utf-8', errors='ignore'))
            
            has_video_stream = False
            has_audio_stream = False
            duration = None
            
            if 'streams' in probe_data:
                for stream in probe_data['streams']:
                    if stream.get('codec_type') == 'video':
                        has_video_stream = True
                    elif stream.get('codec_type') == 'audio':
                        has_audio_stream = True
            
            if 'format' in probe_data and 'duration' in probe_data['format']:
                try:
                    duration = float(probe_data['format']['duration'])
                except (ValueError, TypeError):
                    pass
            
            if file_path.suffix.lower() in ['.mp3', '.m4a', '.aac', '.flac', '.wav']:
                if has_audio_stream:
                    return {
                        'valid': True,
                        'has_video_stream': has_video_stream,
                        'has_audio_stream': has_audio_stream,
                        'duration': duration
                    }
                return {'valid': False}
            
            if has_video_stream or has_audio_stream:
                return {
                    'valid': True,
                    'has_video_stream': has_video_stream,
                    'has_audio_stream': has_audio_stream,
                    'duration': duration
                }
            return {'valid': False}
            
        except json.JSONDecodeError:
            return {'valid': True}
            
    except Exception as e:
        logger.error(f"ffprobe execution error: {e}")
        return {'valid': True}


# ============================================
# Date & Sorting Utilities
# ============================================

def get_date_sort_key(item: dict, key: str):
    """Get sortable timestamp from dict item with date field."""
    val = item.get(key)
    if not val:
        return 0
    
    if isinstance(val, (int, float)):
        return val
    
    if isinstance(val, str):
        try:
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
    """Check if a system tool is available in PATH."""
    if not shutil.which(command):
        return False
    
    try:
        cmd = [command] + list(args)
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await process.communicate()
        return process.returncode == 0
    except Exception:
        return False


# ============================================
# Exports
# ============================================

__all__ = [
    'normalize_title',
    'extract_domain',
    'generate_video_hash',
    'get_download_config_for_url',
    'format_file_size',
    'normalize_video_url',
    'normalize_url_by_rules',
    'clean_and_validate_url',
    'is_domain_allowed',
    'is_url_allowed',
    'validate_url',
    'process_url',
    'check_video_file_integrity',
    'check_video_file_integrity_extended',
    'get_date_sort_key',
    'check_tool',
]
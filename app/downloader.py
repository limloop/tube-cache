"""
Video downloader using yt-dlp.

Handles downloading videos from supported sources.
Saves files to subdirectory structure: data/videos/{hash[:4]}/{hash}.{ext}
"""

import asyncio
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional

import yt_dlp

from app.config import settings
from app.file_utils import ensure_video_subdir
from app.utils import get_download_config_for_url, normalize_title
from app import logger


class DownloadError(Exception):
    """Download error exception."""
    pass


class VideoDownloader:
    """Video downloader using yt-dlp."""
    
    def __init__(self):
        self.videos_path = Path(settings.storage.videos_path)
        self.temp_path = Path(settings.storage.temp_path)
        self.opts = settings.download.yt_dlp
        
        # Create directories
        self.videos_path.mkdir(parents=True, exist_ok=True)
        self.temp_path.mkdir(parents=True, exist_ok=True)
    
    async def download(self, url: str, video_hash: str) -> Dict[str, Any]:
        """
        Download a video.
        
        Args:
            url: Video URL
            video_hash: 64-character hash
            
        Returns:
            Dict with download results
            
        Raises:
            DownloadError: On download failure
        """
        # Clean all temporary files before starting
        await self._cleanup_all_temp_files(video_hash)
        
        loop = asyncio.get_event_loop()
        
        try:
            # Get format configuration
            format_spec, extract_audio = get_download_config_for_url(url)
            
            # Build yt-dlp options
            ydl_opts = self._build_ydl_opts(format_spec, extract_audio, video_hash)
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Extract video info
                info = await loop.run_in_executor(None, ydl.extract_info, url, False)
                
                if not info:
                    raise DownloadError("Failed to get video info")
                
                # Determine extension
                ext = 'mp3' if extract_audio else info.get('ext', 'mp4')
                
                # Download video
                logger.info(f"Downloading {video_hash[:12]}...")
                await loop.run_in_executor(None, ydl.download, [url])
                
                # Wait for file to be written
                await asyncio.sleep(2)
                
                # Find downloaded file
                downloaded_file = await self._find_downloaded_file(video_hash, ext)
                if not downloaded_file:
                    raise DownloadError("File not found after download")
                
                # Validate file
                await self._validate_file(downloaded_file, extract_audio)
                
                # Ensure subdirectory exists
                subdir_path = ensure_video_subdir(video_hash)
                final_path = subdir_path / f"{video_hash}.{ext}"
                
                # Remove old file if exists
                final_path.unlink(missing_ok=True)
                
                # Move to final location
                shutil.move(str(downloaded_file), str(final_path))
                
                if not final_path.exists():
                    raise DownloadError("File not moved to final location")
                
                # Quick check after move
                if not await self._quick_check_file(final_path):
                    raise DownloadError("File corrupted after move")
                
                # Return result
                return {
                    'file_path': str(final_path),
                    'title': normalize_title(info.get('title', '')),
                    'duration': info.get('duration'),
                    'uploader': info.get('uploader'),
                    'file_size': final_path.stat().st_size,
                    'file_ext': ext,
                    'download_success': True
                }
                
        except yt_dlp.utils.DownloadError as e:
            await self._cleanup_all_temp_files(video_hash)
            
            error_msg = str(e).lower()
            
            # Handle specific errors
            if "http error 403" in error_msg or "forbidden" in error_msg:
                raise DownloadError("403 Forbidden: Access denied. Video may be private or blocked.")
            elif "http error 416" in error_msg or "range not satisfiable" in error_msg:
                raise DownloadError(f"HTTP 416: Range error - {e}")
            elif "http error 404" in error_msg or "not found" in error_msg:
                raise DownloadError("404 Not Found: Video was removed or doesn't exist.")
            elif "http error 429" in error_msg or "too many requests" in error_msg:
                raise DownloadError("429 Too Many Requests: Please try again later.")
            elif "unavailable" in error_msg:
                raise DownloadError(f"Video unavailable: {e}")
            else:
                raise DownloadError(f"Download error: {e}")

        except Exception as e:
            await self._cleanup_all_temp_files(video_hash)
            raise DownloadError(f"Download error: {e}")
    
    async def _cleanup_all_temp_files(self, video_hash: str):
        """Clean all temporary files for this video."""
        try:
            for file in self.temp_path.glob(f"*{video_hash}*"):
                try:
                    if file.exists():
                        file.unlink()
                        logger.debug(f"Cleaned: {file.name}")
                except Exception as e:
                    logger.debug(f"Failed to delete {file}: {e}")
        except Exception as e:
            logger.warning(f"Cleanup error for {video_hash}: {e}")
    
    def _build_ydl_opts(
        self, 
        format_spec: str, 
        extract_audio: bool, 
        video_hash: str
    ) -> Dict[str, Any]:
        """Build yt-dlp options."""
        
        temp_filename = f"{video_hash}_temp.%(ext)s"
        output_template = str(self.temp_path / temp_filename)
        
        # Default options
        default_opts = {
            'format': format_spec,
            'outtmpl': output_template,
            'continuedl': True,
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'retries': 2,
            'fragment_retries': 3,
            'socket_timeout': 30,
            'concurrent_fragment_downloads': 2,
            'skip_unavailable_fragments': True,
            'fixup': 'detect_or_warn'
        }
        
        # Merge with user options
        if self.opts:
            user_opts = self.opts.copy()
            
            # Merge http_headers instead of overwriting
            if 'http_headers' in user_opts and 'http_headers' in default_opts:
                default_opts['http_headers'].update(user_opts.pop('http_headers'))
            
            default_opts.update(user_opts)
        
        # Audio-specific options
        if extract_audio:
            default_opts.update({
                'format': 'bestaudio/best'
            })
            
            if self.opts and 'postprocessors' in self.opts:
                default_opts['postprocessors'] = self.opts['postprocessors']
        else:
            if 'merge_output_format' not in default_opts:
                default_opts['merge_output_format'] = 'mp4'
        
        return default_opts
    
    async def _find_downloaded_file(self, video_hash: str, expected_ext: str) -> Optional[Path]:
        """Find downloaded file."""
        # Search by pattern
        for file in self.temp_path.glob(f"{video_hash}_temp*.{expected_ext}"):
            if file.exists() and file.stat().st_size > 0 and not file.name.endswith('.part'):
                return file
        
        # Search any file with this hash
        for file in self.temp_path.glob(f"*{video_hash}*"):
            if (file.exists() and file.stat().st_size > 0 and 
                not file.name.endswith('.part') and 
                file.suffix.lstrip('.') == expected_ext):
                return file
        
        return None
    
    async def _validate_file(self, file_path: Path, is_audio: bool):
        """Validate downloaded file."""
        # Check size
        if file_path.stat().st_size < 1024 * 10:  # 10KB minimum
            raise DownloadError("File too small")
        
        # Check with file command if available
        if await self._has_file_command():
            if not await self._check_with_file(file_path, is_audio):
                raise DownloadError("Invalid file format")
    
    async def _quick_check_file(self, file_path: Path) -> bool:
        """Quick file check."""
        try:
            if not file_path.exists():
                return False
            
            if file_path.stat().st_size == 0:
                return False
            
            # Try to read beginning and end
            with open(file_path, 'rb') as f:
                f.seek(0)
                header = f.read(100)
                if len(header) == 0:
                    return False
                
                # Check end for small files
                if file_path.stat().st_size < 10 * 1024 * 1024:  # 10MB
                    f.seek(-100, 2)
                    footer = f.read(100)
                    if len(footer) == 0:
                        return False
            
            return True
            
        except Exception:
            return False
    
    async def _has_file_command(self) -> bool:
        """Check if 'file' command is available."""
        try:
            result = await asyncio.create_subprocess_exec(
                'file', '--version',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            await result.communicate()
            return result.returncode == 0
        except:
            return False
    
    async def _check_with_file(self, file_path: Path, is_audio: bool) -> bool:
        """Check file with 'file' command."""
        try:
            result = await asyncio.create_subprocess_exec(
                'file', '-b', '--mime-type', str(file_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            stdout, _ = await result.communicate()
            mime_type = stdout.decode().strip().lower()
            
            if is_audio:
                return 'audio' in mime_type or 'mpeg' in mime_type
            else:
                return 'video' in mime_type or 'mp4' in mime_type
                
        except Exception:
            return True  # Assume valid if file command fails
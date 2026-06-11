"""
Video Server API - Main Application

Provides endpoints for video management, streaming, and thumbnail generation.
"""

from fastapi import FastAPI, HTTPException, Query, Request, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from typing import Optional, Dict, Any
from pathlib import Path

from app.config import settings
from app.database import db
from app.queue import queue
from app.storage import storage
from app.utils import (
    generate_video_hash, 
    get_download_config_for_url, 
    normalize_title, 
    normalize_video_url, 
    check_video_file_integrity, 
    check_video_file_integrity_extended, 
    get_date_sort_key
)
from app.models import VideoStatus, TaskStatus, VideoMetadata
from app.webui import router as webui_router
from app.i18n import I18nMiddleware
from app import logger


# ============================================
# Application Setup
# ============================================

app = FastAPI(
    title="Video Server API",
    description="Self-hosted video cache and streaming server",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add i18n middleware (reads language from cookie)
app.add_middleware(I18nMiddleware)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
static_path = Path(__file__).parent.parent / "app" / "static"
static_path.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# Include web interface router
app.include_router(webui_router)


# ============================================
# Startup & Shutdown Events
# ============================================

@app.on_event("startup")
async def startup_event():
    """
    Initialize application on startup:
    - Connect to database
    - Start download queue
    - Start storage monitoring
    """
    try:
        # Connect to database
        await db.connect()
        
        # Start download queue
        await queue.start()

        # Start storage monitoring
        await storage.start_monitoring()
        
        # Check dependencies
        await _check_dependencies()
        
        logger.info("✅ Video Server started successfully")
        logger.info(f"📁 Storage path: {settings.storage.base_path}")
        logger.info(f"🌐 Server: http://{settings.server.host}:{settings.server.port}")
        
    except Exception as e:
        logger.error(f"❌ Startup error: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """
    Cleanup on shutdown:
    - Stop download queue
    - Close database connection
    """
    try:
        await queue.stop()
        await db.close()
        await storage.stop_monitoring()
        logger.info("🛑 Video Server stopped")
    except Exception as e:
        logger.error(f"Shutdown error: {e}")


async def _check_dependencies():
    """Check required system dependencies"""
    from app.utils import check_tool
    
    dependencies = {
        'ffmpeg': ['ffmpeg', '-version'],
        'yt-dlp': ['yt-dlp', '--version'],
    }
    
    missing = []
    for name, cmd in dependencies.items():
        if not await check_tool(cmd[0], cmd[1:]):
            missing.append(name)
            logger.warning(f"⚠️ {name} not found in PATH")
    
    if missing:
        logger.warning(f"Missing tools: {', '.join(missing)}")
        if 'ffmpeg' in missing:
            logger.warning("Thumbnail generation will be limited")


# ============================================
# API Endpoints
# ============================================

@app.get("/", include_in_schema=False)
async def root():
    """Root endpoint - API information"""
    return {
        "message": "Video Server API",
        "version": "2.0.0",
        "endpoints": {
            "docs": "/docs",
            "video": "/video?url=...",
            "stream": "/stream/{hash}",
            "info": "/info/{hash}",
            "queue": "/queue/info",
            "storage": "/storage/info"
        }
    }


@app.get("/video", response_model=TaskStatus)
async def request_video(url: str = Query(..., description="Video URL")):
    """
    Request video download.
    
    Returns task status immediately. If video is already cached,
    returns stream URL. If not, adds to download queue.
    """
    try:
        # Normalize URL
        normalized_url = normalize_video_url(url)
        if not normalized_url:
            raise HTTPException(status_code=400, detail="Invalid URL")
        
        url = normalized_url
        
        # Get format config and generate hash
        format_spec, _ = get_download_config_for_url(url)
        video_hash = generate_video_hash(url, format_spec)
        
        # Check database
        video = await db.get_video(video_hash)
        
        if not video:
            # New video - create record and add to queue
            await db.create_video(video_hash, url)
            await _cleanup_temp_files(video_hash)
            
            added = await queue.add_task(video_hash, url)
            
            if not added:
                position = await queue.get_queue_position(video_hash)
                message = "Video already in queue"
                if position is not None:
                    message += f", position: {position + 1}"
                
                return TaskStatus(
                    hash=video_hash,
                    status=VideoStatus.PENDING,
                    message=message
                )
            
            return TaskStatus(
                hash=video_hash,
                status=VideoStatus.PENDING,
                message="Video added to download queue"
            )
        
        # Existing video - check status
        status = VideoStatus(video['status'])
        
        if status == VideoStatus.READY:
            # Check if file exists
            file_path = await storage.find_video_path(video_hash)
            
            if not file_path or not file_path.exists():
                logger.error(f"File missing for ready video: {video_hash}")
                await db.mark_video_deleted(video_hash)
                
                # Recreate task
                await db.update_status(video_hash, VideoStatus.PENDING)
                await _cleanup_temp_files(video_hash)
                await queue.add_task(video_hash, video['source_url'])
                
                return TaskStatus(
                    hash=video_hash,
                    status=VideoStatus.PENDING,
                    message="File lost, restarting download"
                )
            
            # Check integrity
            integrity_result = await check_video_file_integrity_extended(
                file_path, video.get('file_size')
            )
            
            if not integrity_result['valid']:
                logger.error(f"Corrupted file: {video_hash}")
                
                try:
                    file_path.unlink()
                except:
                    pass
                
                await db.update_status(video_hash, VideoStatus.FAILED)
                
                retry_count = video.get('retry_count', 0)
                if retry_count < 3:
                    await db.update_status(video_hash, VideoStatus.PENDING)
                    await _cleanup_temp_files(video_hash)
                    await queue.add_task(video_hash, video['source_url'])
                    
                    return TaskStatus(
                        hash=video_hash,
                        status=VideoStatus.PENDING,
                        message=f"File corrupted, retry ({retry_count + 1}/3)"
                    )
                else:
                    return TaskStatus(
                        hash=video_hash,
                        status=VideoStatus.FAILED,
                        message="Video corrupted and cannot be recovered"
                    )
            
            # All good
            return TaskStatus(
                hash=video_hash,
                status=VideoStatus.READY,
                stream_url=f"/stream/{video_hash}",
                message="Video ready to watch",
                file_size=video.get('file_size'),
                duration=video.get('duration')
            )
        
        elif status == VideoStatus.FAILED:
            retry_count = video.get('retry_count', 0)
            if retry_count < 3:
                await db.update_status(video_hash, VideoStatus.PENDING)
                await _cleanup_temp_files(video_hash)
                await queue.add_task(video_hash, video['source_url'])
                
                return TaskStatus(
                    hash=video_hash,
                    status=VideoStatus.PENDING,
                    message=f"Restarting failed download ({retry_count + 1}/3)"
                )
            else:
                return TaskStatus(
                    hash=video_hash,
                    status=VideoStatus.FAILED,
                    message="Download failed after 3 attempts"
                )
        
        elif status == VideoStatus.PENDING:
            position = await queue.get_queue_position(video_hash)
            
            if position is None:
                logger.warning(f"Task lost, recreating: {video_hash[:12]}")
                await _cleanup_temp_files(video_hash)
                await queue.add_task(video_hash, video['source_url'])
                position = await queue.get_queue_position(video_hash)
            
            message = "Video in download queue"
            if position is not None:
                message += f", position: {position + 1}"
            
            return TaskStatus(
                hash=video_hash,
                status=VideoStatus.PENDING,
                message=message
            )
        
        elif status == VideoStatus.DOWNLOADING:
            # Check if task is actually active
            queue_info = await queue.get_queue_info()
            is_active = any(
                task['hash'].startswith(video_hash[:12]) 
                for task in queue_info.get('active_tasks_list', [])
            )
            
            if not is_active:
                logger.warning(f"Task DOWNLOADING but not active: {video_hash[:12]}")
                await db.update_status(video_hash, VideoStatus.PENDING)
                await _cleanup_temp_files(video_hash)
                await queue.add_task(video_hash, video['source_url'])
                
                return TaskStatus(
                    hash=video_hash,
                    status=VideoStatus.PENDING,
                    message="Task restarted"
                )
            
            return TaskStatus(
                hash=video_hash,
                status=VideoStatus.DOWNLOADING,
                message="Video is downloading"
            )
        
        else:
            # Unknown status - recover
            logger.warning(f"Unknown status {status} for {video_hash}")
            await db.update_status(video_hash, VideoStatus.PENDING)
            await _cleanup_temp_files(video_hash)
            await queue.add_task(video_hash, video['source_url'])
            
            return TaskStatus(
                hash=video_hash,
                status=VideoStatus.PENDING,
                message="Unknown status, restarting"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing video request {url}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


async def _cleanup_temp_files(video_hash: str):
    """Clean temporary files for a video"""
    try:
        temp_dir = Path(settings.storage.temp_path)
        for file in temp_dir.glob(f"*{video_hash}*"):
            try:
                file.unlink()
                logger.debug(f"Cleaned temp file: {file.name}")
            except:
                pass
    except Exception as e:
        logger.warning(f"Failed to cleanup temp files for {video_hash}: {e}")


@app.get("/stream/{video_hash}")
async def stream_video(video_hash: str, request: Request):
    """
    Stream video file with HTTP Range support.
    
    Supports seeking and partial content requests.
    """
    # Check video exists
    video = await db.get_video(video_hash)
    
    if not video or VideoStatus(video['status']) != VideoStatus.READY:
        raise HTTPException(status_code=404, detail="Video not found")
    
    # Update access statistics
    await db.update_access(video_hash)
    
    # Find file
    file_path = await storage.find_video_path(video_hash)
    
    if not file_path:
        await db.update_status(video_hash, VideoStatus.DELETED)
        raise HTTPException(status_code=404, detail="Video file not found")
    
    # Check integrity
    if not check_video_file_integrity(file_path):
        logger.error(f"Corrupted file during streaming: {video_hash[:12]}")
        
        try:
            file_path.unlink()
        except:
            pass
        
        await db.mark_video_deleted(video_hash)
        raise HTTPException(status_code=410, detail="File corrupted, re-download required")
    
    # Determine MIME type
    mime_type = "video/mp4"
    suffix = file_path.suffix.lower()
    if suffix == '.webm':
        mime_type = "video/webm"
    elif suffix == '.mkv':
        mime_type = "video/x-matroska"
    elif suffix == '.mp4':
        mime_type = "video/mp4"
    
    # Get filename for download
    filename = f"{normalize_title(video.get('title', video_hash))}{suffix}"
    
    # Return file with Range support
    return FileResponse(
        path=file_path,
        media_type=mime_type,
        filename=filename,
        content_disposition_type="inline"
    )


@app.get("/info/{video_hash}", response_model=VideoMetadata)
async def get_video_info(video_hash: str):
    """
    Get video metadata from database.
    """
    video = await db.get_video(video_hash)
    
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")
    
    return VideoMetadata(**video)


@app.get("/health")
async def health_check():
    """
    Health check endpoint for monitoring.
    """
    try:
        await db.get_storage_stats()
        return {"status": "healthy", "service": "video-server", "version": "2.0.0"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")


@app.get("/queue/info")
async def get_queue_info():
    """
    Get detailed queue information.
    """
    info = await queue.get_queue_info()
    return info


@app.get("/storage/info")
async def get_storage_info_detailed():
    """
    Get detailed storage information with video statistics.
    """
    info = await storage.get_storage_info()
    
    # Add additional info
    videos = await db.get_all_ready_videos()
    
    # Sort by last accessed
    def sort_key(item):
        return get_date_sort_key(item, 'last_accessed')
    
    videos_sorted = sorted(videos, key=sort_key, reverse=True)
    
    # Top 10 oldest videos (candidates for cleanup)
    oldest_videos = videos_sorted[:10] if len(videos_sorted) > 10 else videos_sorted
    
    return {
        **info,
        "total_videos": len(videos),
        "oldest_videos": [{
            "hash": v['hash'][:12] + '...',
            "title": v.get('title', 'Untitled'),
            "last_accessed": v.get('last_accessed'),
            "access_count": v.get('access_count', 0),
            "file_size_mb": round(v.get('file_size', 0) / 1024**2, 2) if v.get('file_size') else 0,
        } for v in oldest_videos]
    }


@app.post("/cleanup")
async def trigger_cleanup():
    """
    Manually trigger storage cleanup.
    """
    deleted = await storage.cleanup_old_videos()
    
    info = await storage.get_storage_info()
    
    return {
        "message": f"Cleanup completed, removed {len(deleted)} videos",
        "deleted_count": len(deleted),
        "deleted_hashes": deleted,
        "storage_info": info
    }


# ============================================
# Thumbnail Endpoint (placeholder for Stage 5-6)
# ============================================

@app.get("/thumbnail/{video_hash}")
async def get_thumbnail(
    video_hash: str,
    size: Optional[int] = Query(None, ge=64, le=1920, description="Thumbnail width in pixels")
):
    """
    Get video thumbnail.
    
    Current implementation returns SVG placeholder.
    Full implementation with FFmpeg will be added in Stage 5-6.
    """
    import hashlib
    
    # Generate colored SVG placeholder based on hash
    color = "#" + hashlib.md5(video_hash.encode()).hexdigest()[:6]
    target_size = size or 320
    
    svg = f'''<svg width="{target_size}" height="{target_size}" xmlns="http://www.w3.org/2000/svg">
        <rect width="100%" height="100%" fill="{color}"/>
        <circle cx="50%" cy="50%" r="25%" fill="rgba(255,255,255,0.2)"/>
        <polygon points="40%,35% 40%,65% 65%,50%" fill="rgba(255,255,255,0.5)"/>
        <text x="50%" y="75%" text-anchor="middle" fill="rgba(255,255,255,0.7)" font-size="12" font-family="monospace">No thumbnail</text>
    </svg>'''
    
    return Response(
        content=svg,
        media_type="image/svg+xml",
        headers={
            "Cache-Control": "public, max-age=3600",
            "X-Thumbnail-Status": "placeholder"
        }
    )
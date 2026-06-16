"""
Web interface for Video Server
"""
from fastapi import APIRouter, Request, Query, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional, List
import math
import random
from pathlib import Path
from datetime import datetime

from app.database import db
from app.queue import queue
from app.storage import storage
from app.utils import process_url, get_date_sort_key
from app.models import VideoStatus
from app.i18n import get_language_switcher_context, get_language_from_request
from app import logger

router = APIRouter()

# Setup templates
TEMPLATES_DIR = Path(__file__).parent.parent / "app" / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# ============================================
# Custom Template Context with Language Support
# ============================================

def get_base_context(request: Request) -> dict:
    """Get common context variables for all templates with translation function"""
    current_lang = get_language_from_request(request)
    
    # Create a translation function that captures the current language
    def _translate(key: str, **kwargs) -> str:
        from app.i18n import translate
        return translate(key, current_lang, **kwargs)
    
    return {
        'request': request,
        '_': _translate,  # Pass language-aware translate function
        **get_language_switcher_context(request)
    }


# ============================================
# Template Filters
# ============================================

def timestamp_to_time(timestamp):
    """Convert timestamp to time string"""
    if not timestamp:
        return "--:--:--"
    try:
        if isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
        elif isinstance(timestamp, str):
            dt = datetime.fromisoformat(timestamp.replace(' ', 'T'))
            return dt.strftime('%H:%M:%S')
        return "--:--:--"
    except:
        return "--:--:--"


def format_duration(seconds: Optional[float]) -> str:
    """Format duration in seconds to HH:MM:SS or MM:SS"""
    if not seconds:
        return "Unknown"
    
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


# Note: get_status_text and get_status_badge_class need translation
# They will be defined as functions that take language parameter
# For now, keep simple or move to template

# Register filters
templates.env.filters["timestamp_to_time"] = timestamp_to_time
templates.env.filters["format_duration"] = format_duration


# ============================================
# Helper Functions
# ============================================

def format_file_size(size_bytes: Optional[int]) -> str:
    """Format file size to human readable"""
    if not size_bytes:
        return "Unknown"
    
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.2f} GB"
    elif size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.2f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    return f"{size_bytes} B"


# ============================================
# Page Routes
# ============================================

@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home page - video download form"""
    queue_info = await queue.get_queue_info() if hasattr(queue, 'get_queue_info') else {}
    
    # Get active tasks for display
    active_tasks = []
    if queue_info:
        active_tasks = [
            {
                'hash': task['hash'][:12] + '...',
                'url': task['url'][:50] + '...' if len(task['url']) > 50 else task['url'],
                'position': i + 1
            }
            for i, task in enumerate(queue_info.get('queue', [])[:5])
        ]
    
    return templates.TemplateResponse("index.html", {
        **get_base_context(request),
        "active_tasks": active_tasks,
        "queue_info": queue_info
    })


@router.post("/download", response_class=HTMLResponse)
async def download_video(request: Request, url: str = Form(...)):
    """Handle video download form submission."""
    processed_url, error = await process_url(url)
    
    if not processed_url:
        context = get_base_context(request)
        return templates.TemplateResponse("error.html", {
            **context,
            "error": error or context['_']('invalid_url')
        })
    
    return RedirectResponse(
        url=f"/video?url={processed_url}",
        status_code=303
    )


@router.get("/videos", response_class=HTMLResponse)
async def list_videos(
    request: Request,
    page: int = Query(1, ge=1),
    search: str = Query(""),
    status_filter: str = Query("ready", alias="status")
):
    """
    List all videos with pagination and search.
    Auto-checks READY videos for file existence.
    """
    # Get videos based on filter
    all_videos = []
    
    if status_filter == "ready":
        all_videos = await db.get_all_ready_videos() or []
        
        # Quick check: verify file exists for READY videos
        for video in all_videos:
            file_path = await storage.find_video_path(video['hash'])
            if not file_path or not file_path.exists():
                logger.debug(f"Marking as DELETED (file missing): {video['hash'][:12]}")
                await db.mark_video_deleted(video['hash'])
                video['status'] = 'deleted'
        
        # Re-filter after updates (remove deleted)
        all_videos = [v for v in all_videos if v.get('status') != 'deleted']
        
    elif status_filter == "pending":
        all_videos = await db.get_videos_by_status(VideoStatus.PENDING) or []
        
    elif status_filter == "downloading":
        all_videos = await db.get_videos_by_status(VideoStatus.DOWNLOADING) or []
        
    elif status_filter == "failed":
        all_videos = await db.get_videos_by_status(VideoStatus.FAILED) or []
        
    elif status_filter == "deleted":
        all_videos = await db.get_videos_by_status(VideoStatus.DELETED) or []
        
    else:  # "all" - get ALL videos regardless of status
        all_videos = await db.get_all_videos() or []
        
        # Check READY videos for file existence (but keep them even if missing)
        for video in all_videos:
            if video.get('status') == 'ready':
                file_path = await storage.find_video_path(video['hash'])
                if not file_path or not file_path.exists():
                    logger.debug(f"File missing for READY video: {video['hash'][:12]}")
                    # Mark as DELETED but keep in list (user will see status changed)
                    await db.mark_video_deleted(video['hash'])
                    video['status'] = 'deleted'
        
        # Don't filter - show all videos including deleted
        # all_videos stays as is
    
    # Filter by search - supports title, uploader, and hash
    if search and all_videos:
        search_lower = search.lower()
        all_videos = [
            v for v in all_videos
            if (search_lower in ((v.get('title') or '').lower()) or
                search_lower in ((v.get('uploader') or '').lower()) or
                search_lower in (v.get('hash', '').lower()))
        ]
    
    # Sort by creation date (newest first)
    if all_videos:
        all_videos.sort(key=lambda x: get_date_sort_key(x, 'created_at'), reverse=True)
    
    # Pagination
    total_videos = len(all_videos)
    total_pages = max(1, math.ceil(total_videos / 20) if total_videos > 0 else 1)
    page = max(1, min(page, total_pages))
    
    start_idx = (page - 1) * 20
    end_idx = start_idx + 20
    videos = all_videos[start_idx:end_idx] if all_videos else []
    
    # Get translate function
    context = get_base_context(request)
    _ = context['_']
    
    # Format videos for template
    formatted_videos = []
    for video in videos:
        status = video.get('status', 'unknown')
        status_icons = {
            'ready': '✅',
            'downloading': '📥',
            'pending': '⏳',
            'failed': '❌',
            'deleted': '🗑️'
        }
        status_texts = {
            'ready': _('status_ready'),
            'downloading': _('status_downloading'),
            'pending': _('status_pending'),
            'failed': _('status_failed'),
            'deleted': _('status_deleted')
        }
        status_badge_classes = {
            'ready': 'bg-success',
            'downloading': 'bg-info',
            'pending': 'bg-warning text-dark',
            'failed': 'bg-danger',
            'deleted': 'bg-secondary'
        }
        
        formatted_videos.append({
            'hash': video['hash'],
            'short_hash': video['hash'][:12] + '...',
            'title': video.get('title') or 'Untitled',
            'uploader': video.get('uploader') or 'Unknown',
            'duration': video.get('duration'),
            'duration_str': format_duration(video.get('duration')),
            'file_size': format_file_size(video.get('file_size')),
            'status': status,
            'status_text': f"{status_icons.get(status, '❓')} {status_texts.get(status, _('status_unknown'))}",
            'status_badge_class': status_badge_classes.get(status, 'bg-dark'),
            'last_accessed': video.get('last_accessed'),
            'access_count': video.get('access_count', 0),
            'created_at': video.get('created_at'),
            'source_url': video.get('source_url', '#'),
        })
    
    return templates.TemplateResponse("videos.html", {
        **context,
        "videos": formatted_videos,
        "page": page,
        "total_pages": total_pages,
        "search": search,
        "status_filter": status_filter,
        "total_videos": total_videos,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "prev_page": page - 1,
        "next_page": page + 1,
    })


@router.get("/video/{video_hash}", response_class=HTMLResponse)
async def video_detail(request: Request, video_hash: str):
    """
    Video detail page with player and metadata.
    Pure display - does NOT modify video state.
    Shows appropriate UI for missing/corrupted videos.
    """
    # Get video from DB
    video = await db.get_video(video_hash)
    context = get_base_context(request)
    _ = context['_']
    
    if not video:
        return templates.TemplateResponse("video_not_found.html", {
            **context,
            "video_hash": video_hash,
            "error_code": 404
        })
    
    # Check if file exists for READY videos
    file_exists = False
    status = video.get('status', 'unknown')
    
    if status == 'ready':
        file_path = await storage.find_video_path(video_hash)
        file_exists = file_path is not None and file_path.exists()
        
        # If file missing but status is READY - show warning page
        if not file_exists:
            return templates.TemplateResponse("video_missing.html", {
                **context,
                "video": video,
                "video_hash": video_hash,
                "title": video.get('title', 'Untitled'),
                "source_url": video.get('source_url', '#'),
            })
    
    # If status is FAILED or DELETED - show appropriate page
    if status in ['failed', 'deleted']:
        return templates.TemplateResponse("video_unavailable.html", {
            **context,
            "video": video,
            "video_hash": video_hash,
            "title": video.get('title', 'Untitled'),
            "status": status,
            "status_text": _('status_failed') if status == 'failed' else _('status_deleted'),
            "source_url": video.get('source_url', '#'),
        })
    
    # If PENDING or DOWNLOADING - show loading state
    if status in ['pending', 'downloading']:
        return templates.TemplateResponse("video_loading.html", {
            **context,
            "video": video,
            "video_hash": video_hash,
            "title": video.get('title', 'Untitled'),
            "status": status,
            "status_text": _('status_pending') if status == 'pending' else _('status_downloading'),
        })
    
    # Get ready videos for navigation (only for valid videos)
    ready_videos = await db.get_all_ready_videos()
    ready_videos.sort(key=lambda v: v.get('created_at', ''), reverse=False)
    
    # Find current video index
    current_index = None
    for i, v in enumerate(ready_videos):
        if v['hash'] == video_hash:
            current_index = i
            break
    
    # Get neighbors
    prev_video = None
    next_video = None
    
    if current_index is not None:
        if current_index - 1 >= 0:
            prev_video = ready_videos[current_index - 1]
        if current_index + 1 < len(ready_videos):
            next_video = ready_videos[current_index + 1]
    
    # Get random recommendations
    random_videos = []
    other_ready = [v for v in ready_videos if v['hash'] != video_hash]
    if other_ready:
        random_videos = random.sample(other_ready, min(5, len(other_ready)))
    
    # Format current video
    status_icons = {
        'ready': '✅',
        'downloading': '📥',
        'pending': '⏳',
        'failed': '❌',
        'deleted': '🗑️'
    }
    status_texts = {
        'ready': _('status_ready'),
        'downloading': _('status_downloading'),
        'pending': _('status_pending'),
        'failed': _('status_failed'),
        'deleted': _('status_deleted')
    }
    status_badge_classes = {
        'ready': 'bg-success',
        'downloading': 'bg-info',
        'pending': 'bg-warning text-dark',
        'failed': 'bg-danger',
        'deleted': 'bg-secondary'
    }
    
    formatted_video = {
        'hash': video_hash,
        'short_hash': video_hash[:12] + '...',
        'title': video.get('title', 'Untitled'),
        'uploader': video.get('uploader', 'Unknown'),
        'duration': video.get('duration'),
        'duration_str': format_duration(video.get('duration')),
        'file_size': format_file_size(video.get('file_size')),
        'status': status,
        'status_text': f"{status_icons.get(status, '❓')} {status_texts.get(status, _('status_unknown'))}",
        'status_badge_class': status_badge_classes.get(status, 'bg-dark'),
        'last_accessed': video.get('last_accessed'),
        'access_count': video.get('access_count', 0),
        'created_at': video.get('created_at'),
        'source_url': video.get('source_url', '#'),
        'file_ext': video.get('file_ext', 'mp4'),
    }
    
    # MIME types for video
    mime_types = {
        'mp4': 'video/mp4',
        'webm': 'video/webm',
        'mkv': 'video/x-matroska',
        'avi': 'video/x-msvideo',
    }
    formatted_video['mime_type'] = mime_types.get(
        formatted_video['file_ext'].lower().lstrip('.'),
        'video/mp4'
    )
    
    # Format neighbor function
    def format_neighbor(v):
        if not v:
            return None
        return {
            'hash': v['hash'],
            'title': v.get('title', 'Untitled'),
            'uploader': v.get('uploader', 'Unknown'),
            'duration_str': format_duration(v.get('duration')),
        }
    
    return templates.TemplateResponse("video.html", {
        **context,
        "video": formatted_video,
        "prev_video": format_neighbor(prev_video),
        "next_video": format_neighbor(next_video),
        "random_videos": [format_neighbor(v) for v in random_videos],
        "file_exists": file_exists,
    })


@router.get("/queue", response_class=HTMLResponse)
async def queue_status(request: Request):
    """Queue status page"""
    context = get_base_context(request)
    
    try:
        queue_info = await queue.get_queue_info()
        
        # Format timestamps
        if queue_info:
            for task in queue_info.get('queue', []):
                task['added_time'] = timestamp_to_time(task.get('added_at'))
                task['worker_name'] = None
            
            for task in queue_info.get('active_tasks_list', []):
                task['started_time'] = timestamp_to_time(task.get('started_at'))
                task['worker_name'] = f"Worker {task.get('worker_id', '?')}"
        
        logger.debug(f"Queue: {queue_info.get('active_tasks', 0)} active, "
                    f"{queue_info.get('queued_tasks', 0)} queued")
        
    except Exception as e:
        logger.error(f"Error getting queue info: {e}")
        queue_info = {}
    
    return templates.TemplateResponse("queue.html", {
        **context,
        "queue_info": queue_info
    })


@router.get("/storage", response_class=HTMLResponse)
async def storage_info(request: Request):
    """Storage information page"""
    storage_info_data = await storage.get_storage_info()
    context = get_base_context(request)
    
    return templates.TemplateResponse("storage.html", {
        **context,
        "storage_info": storage_info_data
    })
"""
Web интерфейс для Video Server
"""
from fastapi import APIRouter, Request, Query, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional, List
import math
from pathlib import Path
import os
import time
from datetime import datetime
from app.database import db
from app.queue import queue
from app.utils import normalize_video_url, get_date_sort_key
from app.models import VideoStatus
from app import logger

router = APIRouter()

# Настраиваем шаблоны
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "app" / "templates"))


# Фильтр для timestamp
def timestamp_to_time(timestamp):
    if not timestamp:
        return "--:--:--"
    try:
        return datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
    except:
        return "--:--:--"

templates.env.filters["timestamp_to_time"] = timestamp_to_time

@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """
    Главная страница - форма для загрузки видео
    """
    # Получаем информацию об очереди для отображения
    queue_info = await queue.get_queue_info() if hasattr(queue, 'get_queue_info') else {}
    
    # Получаем активные задачи
    active_tasks = []
    if queue_info:
        active_tasks = [
            {
                'hash': task['hash'][:12] + '...',
                'url': task['url'][:50] + '...' if len(task['url']) > 50 else task['url'],
                'position': i
            }
            for i, task in enumerate(queue_info.get('queue', [])[:5])  # Первые 5 задач
        ]
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "active_tasks": active_tasks,
        "queue_info": queue_info
    })

@router.post("/download", response_class=HTMLResponse)
async def download_video(request: Request, url: str = Form(...)):
    """
    Обработка формы загрузки видео
    """
    # Нормализуем URL
    normalized_url = normalize_video_url(url)
    
    if not normalized_url:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Некорректный URL. Пожалуйста, введите валидную ссылку на видео."
        })
    
    # Перенаправляем на API endpoint
    return RedirectResponse(
        url=f"/video?url={normalized_url}",
        status_code=303  # See Other
    )

@router.get("/videos", response_class=HTMLResponse)
async def list_videos(
    request: Request,
    page: int = Query(1, ge=1),
    search: str = Query(""),
    status: str = Query("ready")
):
    """
    Список всех видео с пагинацией и поиском
    """
    # Получаем все видео из БД
    if status == "ready":
        all_videos = await db.get_all_ready_videos()
    elif status == "pending":
        all_videos = await db.get_videos_by_status(VideoStatus.PENDING)
    elif status == "downloading":
        all_videos = await db.get_videos_by_status(VideoStatus.DOWNLOADING)
    elif status == "failed":
        all_videos = await db.get_videos_by_status(VideoStatus.FAILED)
    elif status == "deleted":
        all_videos = await db.get_videos_by_status(VideoStatus.DELETED)
    else:
        all_videos = await db.get_all_videos()
    
    # Фильтрация по поиску
    if search:
        search_lower = search.lower()
        all_videos = [
            v for v in all_videos
            if search_lower in (v.get('title', '').lower() or '') or
               search_lower in (v.get('uploader', '').lower() or '')
        ]

    # Сортировка
    def sort_key(item):
        return get_date_sort_key(item, 'created_at')

    all_videos = sorted(all_videos, key=sort_key, reverse=True)
    
    # Пагинация
    page_size = 20
    total_videos = len(all_videos)
    total_pages = math.ceil(total_videos / page_size) if total_videos > 0 else 1
    
    # Корректируем номер страницы
    if page > total_pages:
        page = total_pages
    
    # Получаем видео для текущей страницы
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    videos = all_videos[start_idx:end_idx]
    
    # Форматируем данные для шаблона
    formatted_videos = []
    for video in videos:
        file_size = video.get('file_size')
        if file_size:
            if file_size >= 1024**3:  # GB
                size_str = f"{file_size / 1024**3:.1f} GB"
            elif file_size >= 1024**2:  # MB
                size_str = f"{file_size / 1024**2:.1f} MB"
            elif file_size >= 1024:  # KB
                size_str = f"{file_size / 1024:.1f} KB"
            else:
                size_str = f"{file_size} B"
        else:
            size_str = "Неизвестно"
        
        formatted_videos.append({
            'hash': video['hash'],
            'short_hash': video['hash'][:12] + '...',
            'title': video.get('title', 'Без названия'),
            'uploader': video.get('uploader', 'Неизвестно'),
            'duration': video.get('duration'),
            'duration_str': format_duration(video.get('duration')),
            'file_size': size_str,
            'status': video.get('status', 'unknown'),
            'status_text': get_status_text(video.get('status')),
            'last_accessed': video.get('last_accessed'),
            'access_count': video.get('access_count', 0),
            'created_at': video.get('created_at'),
            'source_url': video.get('source_url', ''),
        })
    
    return templates.TemplateResponse("videos.html", {
        "request": request,
        "videos": formatted_videos,
        "page": page,
        "total_pages": total_pages,
        "search": search,
        "status": status,
        "total_videos": total_videos,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "prev_page": page - 1 if page > 1 else 1,
        "next_page": page + 1 if page < total_pages else total_pages,
    })

@router.get("/video/{video_hash}", response_class=HTMLResponse)
async def video_detail(request: Request, video_hash: str):
    video = await db.get_video(video_hash)
    
    if not video:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Видео не найдено"
        })
    
    # Получаем только готовые видео
    ready_videos = await db.get_all_ready_videos()
    
    # Сортируем по дате добавления
    sorted_videos = sorted(
        ready_videos,
        key=lambda v: v.get('created_at', ''),
        reverse=False
    )
    
    # Находим текущее видео
    current_index = None
    for i, v in enumerate(sorted_videos):
        if v['hash'] == video_hash:
            current_index = i
            break
    
    # Соседние видео
    prev_video = None
    next_video = None
    
    if current_index is not None:
        if current_index - 1 >= 0:
            prev_video = sorted_videos[current_index - 1]
        if current_index + 1 < len(sorted_videos):
            next_video = sorted_videos[current_index + 1]
    
    # Случайные рекомендации
    import random
    random_videos = []
    other_ready = [v for v in ready_videos if v['hash'] != video_hash]
    if other_ready:
        random_videos = random.sample(other_ready, min(5, len(other_ready)))
    
    # Форматируем видео (как было в оригинале)
    file_size = video.get('file_size')
    if file_size:
        if file_size >= 1024**3:
            size_str = f"{file_size / 1024**3:.2f} GB"
        elif file_size >= 1024**2:
            size_str = f"{file_size / 1024**2:.2f} MB"
        else:
            size_str = f"{file_size / 1024:.2f} KB"
    else:
        size_str = "Неизвестно"
    
    formatted_video = {
        'hash': video_hash,
        'short_hash': video_hash[:12] + '...',
        'title': video.get('title', 'Без названия'),
        'uploader': video.get('uploader', 'Неизвестно'),
        'duration': video.get('duration'),
        'duration_str': format_duration(video.get('duration')),
        'file_size': size_str,
        'status': video.get('status'),
        'status_text': get_status_text(video.get('status')),
        'last_accessed': video.get('last_accessed'),
        'access_count': video.get('access_count', 0),
        'created_at': video.get('created_at'),
        'source_url': video.get('source_url', '#'),
        'file_ext': video.get('file_ext', 'mp4'),
    }
    
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
    
    # Форматируем соседние видео
    def format_neighbor(v):
        if not v:
            return None
        return {
            'hash': v['hash'],
            'title': v.get('title', 'Без названия'),
            'uploader': v.get('uploader', 'Неизвестно'),
            'duration_str': format_duration(v.get('duration')),
        }
    
    return templates.TemplateResponse("video.html", {
        "request": request,
        "video": formatted_video,
        "prev_video": format_neighbor(prev_video),
        "next_video": format_neighbor(next_video),
        "random_videos": [format_neighbor(v) for v in random_videos],
    })

@router.get("/queue", response_class=HTMLResponse)
async def queue_status(request: Request):
    """Страница очереди"""
    try:
        queue_info = await queue.get_queue_info()
        
        # Форматируем время
        if queue_info:
            for task in queue_info.get('queue', []):
                task['added_time'] = timestamp_to_time(task.get('added_at'))
            
            for task in queue_info.get('active_tasks_list', []):
                task['started_time'] = timestamp_to_time(task.get('started_at'))
                task['worker_name'] = f"Воркер {task.get('worker_id', '?')}"
        
        logger.debug(f"Queue: {queue_info.get('active_tasks', 0)} active, "
                    f"{queue_info.get('queued_tasks', 0)} queued, "
                    f"{queue_info.get('working_workers', 0)}/{queue_info.get('total_workers', 0)} workers")
        
    except Exception as e:
        logger.error(f"Error getting queue info: {e}")
        queue_info = {}
    
    return templates.TemplateResponse("queue.html", {
        "request": request,
        "queue_info": queue_info
    })

@router.get("/storage", response_class=HTMLResponse)
async def storage_info(request: Request):
    """
    Информация о хранилище
    """
    from app.storage import storage
    storage_info = await storage.get_storage_info() if hasattr(storage, 'get_storage_info') else {}
    
    return templates.TemplateResponse("storage.html", {
        "request": request,
        "storage_info": storage_info
    })

# Вспомогательные функции
def format_duration(seconds: Optional[float]) -> str:
    """Форматирует длительность в читаемый вид"""
    if not seconds:
        return "Неизвестно"
    
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes}:{secs:02d}"

def get_status_text(status: Optional[str]) -> str:
    """Возвращает человеко-читаемый текст статуса"""
    status_map = {
        'pending': '⏳ В очереди',
        'downloading': '📥 Загружается',
        'ready': '✅ Готово',
        'failed': '❌ Ошибка',
        'deleted': '🗑️ Удалено',
    }
    return status_map.get(status, '❓ Неизвестно')
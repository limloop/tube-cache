"""
Оптимизированные API эндпоинты
"""
from fastapi import FastAPI, HTTPException, Query, Request, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from datetime import datetime
from pathlib import Path
from app.config import settings
from app.database import db
from app.queue import queue
from app.storage import storage
from app.utils import generate_video_hash, get_download_config_for_url, normalize_title, normalize_video_url, check_video_file_integrity, check_video_file_integrity_extended, get_date_sort_key
from app.models import VideoStatus, VideoRequest, TaskStatus, VideoMetadata, StorageInfo
from app.webui import router as webui_router
from app import logger


# Создаем приложение FastAPI
app = FastAPI(
    title="Video Server API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Настраиваем CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(webui_router)

@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    try:
        # Подключаем базу данных
        await db.connect()
        
        # Запускаем очередь
        await queue.start()

        # Запускаем мониторинг хранилища
        await storage.start_monitoring()
        
        logger.info("✅ Сервер запущен")
        logger.info(f"📁 Хранилище: {settings.storage.base_path}")
        logger.info(f"🌐 Сервер: http://{settings.server.host}:{settings.server.port}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска приложения: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при завершении"""
    try:
        await queue.stop()
        await db.close()
        logger.info("🛑 Сервер остановлен")
    except Exception as e:
        logger.error(f"Ошибка остановки приложения: {e}")

@app.get("/", include_in_schema=False)
async def root():
    """Корневой эндпоинт"""
    return {"message": "Video Server API", "version": "1.0.0"}

@app.get("/video", response_model=TaskStatus)
async def request_video(url: str = Query(..., description="URL видео")):
    """
    Запрашивает скачивание видео
    """
    try:
        # Нормализуем URL
        normalized_url = normalize_video_url(url)
        if not normalized_url:
            raise HTTPException(status_code=400, detail="Некорректный URL")
        
        url = normalized_url
        
        # Генерируем хеш
        format_spec, _ = get_download_config_for_url(url)
        video_hash = generate_video_hash(url, format_spec)
        
        # Проверяем в БД
        video = await db.get_video(video_hash)
        
        if not video:
            # Новая задача - создаём запись и добавляем в очередь
            await db.create_video(video_hash, url)
            await cleanup_temp_files(video_hash)
            
            added = await queue.add_task(video_hash, url)
            
            if not added:
                position = await queue.get_queue_position(video_hash)
                message = f"Видео уже в очереди"
                if position is not None:
                    message += f", позиция: {position + 1}"
                
                return TaskStatus(
                    hash=video_hash,
                    status=VideoStatus.PENDING,
                    message=message
                )
            
            return TaskStatus(
                hash=video_hash,
                status=VideoStatus.PENDING,
                message="Видео добавлено в очередь на загрузку"
            )
        
        # Проверяем статус
        status = VideoStatus(video['status'])
        
        if status == VideoStatus.READY:
            # Проверяем целостность файла
            file_path = await storage.find_video_path(video_hash)
            
            if not file_path or not file_path.exists():
                logger.error(f"Файл отсутствует для готового видео: {video_hash}")
                await db.mark_video_deleted(video_hash)
                
                # Пересоздаём задачу
                await db.update_status(video_hash, VideoStatus.PENDING)
                await cleanup_temp_files(video_hash)
                await queue.add_task(video_hash, video['source_url'])
                
                return TaskStatus(
                    hash=video_hash,
                    status=VideoStatus.PENDING,
                    message="Файл потерян, перезапуск загрузки"
                )
            
            # Проверяем целостность
            integrity_result = await check_video_file_integrity_extended(file_path, video.get('file_size'))
            
            if not integrity_result['valid']:
                logger.error(f"Файл повреждён: {video_hash}")
                
                try:
                    file_path.unlink()
                except:
                    pass
                
                await db.update_status(video_hash, VideoStatus.FAILED)
                
                # Если у видео были попытки, считаем их
                retry_count = video.get('retry_count', 0)
                if retry_count < 3:
                    await db.update_status(video_hash, VideoStatus.PENDING)
                    await cleanup_temp_files(video_hash)
                    await queue.add_task(video_hash, video['source_url'])
                    
                    return TaskStatus(
                        hash=video_hash,
                        status=VideoStatus.PENDING,
                        message=f"Файл повреждён, повторная попытка ({retry_count + 1}/3)"
                    )
                else:
                    return TaskStatus(
                        hash=video_hash,
                        status=VideoStatus.FAILED,
                        message="Видео повреждено и не может быть восстановлено"
                    )
            
            # Всё в порядке
            return TaskStatus(
                hash=video_hash,
                status=VideoStatus.READY,
                stream_url=f"/stream/{video_hash}",
                message="Видео готово к просмотру",
                file_size=video.get('file_size'),
                duration=video.get('duration')
            )
        
        elif status == VideoStatus.FAILED:
            # Проверяем, можно ли перезапустить
            retry_count = video.get('retry_count', 0)
            if retry_count < 3:
                await db.update_status(video_hash, VideoStatus.PENDING)
                await cleanup_temp_files(video_hash)
                await queue.add_task(video_hash, video['source_url'])
                
                return TaskStatus(
                    hash=video_hash,
                    status=VideoStatus.PENDING,
                    message=f"Перезапуск проваленной загрузки ({retry_count + 1}/3)"
                )
            else:
                return TaskStatus(
                    hash=video_hash,
                    status=VideoStatus.FAILED,
                    message="Загрузка провалилась после 3 попыток"
                )
        
        elif status == VideoStatus.PENDING:
            # Проверяем, есть ли задача в очереди
            position = await queue.get_queue_position(video_hash)
            
            if position is None:
                # Задача потерялась, пересоздаём
                logger.warning(f"Задача потеряна, пересоздаём: {video_hash[:12]}")
                await cleanup_temp_files(video_hash)
                await queue.add_task(video_hash, video['source_url'])
                position = await queue.get_queue_position(video_hash)
            
            message = "Видео в очереди на загрузку"
            if position is not None:
                message += f", позиция: {position + 1}"
            
            return TaskStatus(
                hash=video_hash,
                status=VideoStatus.PENDING,
                message=message
            )
        
        elif status == VideoStatus.DOWNLOADING:
            # Проверяем, активна ли задача в очереди
            queue_info = await queue.get_queue_info()
            is_active = any(task['hash'].startswith(video_hash[:12]) 
                          for task in queue_info.get('active_tasks_list', []))
            
            if not is_active:
                # Задача не активна, пересоздаём
                logger.warning(f"Задача DOWNLOADING но не активна: {video_hash[:12]}")
                await db.update_status(video_hash, VideoStatus.PENDING)
                await cleanup_temp_files(video_hash)
                await queue.add_task(video_hash, video['source_url'])
                
                return TaskStatus(
                    hash=video_hash,
                    status=VideoStatus.PENDING,
                    message="Задача перезапущена"
                )
            
            return TaskStatus(
                hash=video_hash,
                status=VideoStatus.DOWNLOADING,
                message="Видео загружается"
            )
        
        else:
            # Неизвестный статус
            logger.warning(f"Неизвестный статус {status} для видео {video_hash}")
            await db.update_status(video_hash, VideoStatus.PENDING)
            await cleanup_temp_files(video_hash)
            await queue.add_task(video_hash, video['source_url'])
            
            return TaskStatus(
                hash=video_hash,
                status=VideoStatus.PENDING,
                message="Неизвестный статус, перезапуск"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса видео {url}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Внутренняя ошибка сервера"
        )

async def cleanup_temp_files(video_hash: str):
    """Очищает только временные файлы"""
    try:
        temp_dir = Path(settings.storage.temp_path)
        for file in temp_dir.glob(f"*{video_hash}*"):
            try:
                file.unlink()
            except:
                pass
    except Exception as e:
        logger.warning(f"Ошибка очистки временных файлов {video_hash}: {e}")

@app.get("/stream/{video_hash}")
async def stream_video(video_hash: str, request: Request):
    """
    Стриминг видео по хешу
    
    Поддерживает HTTP Range запросы для перемотки
    """
    # Проверяем наличие видео
    video = await db.get_video(video_hash)
    
    if not video or VideoStatus(video['status']) != VideoStatus.READY:
        raise HTTPException(status_code=404, detail="Видео не найдено")
    
    # Обновляем статистику просмотров
    await db.update_access(video_hash)
    
    # Ищем файл
    file_path = await storage.find_video_path(video_hash)
    
    if not file_path:
        # Файл не найден, помечаем как удаленный
        await db.update_status(video_hash, VideoStatus.DELETED)
        raise HTTPException(status_code=404, detail="Файл видео не найден")
    
    if not check_video_file_integrity(file_path):
        logger.error(f"Поврежденный файл при стриминге: {video_hash[:12]}...")
        
        # Удаляем файл и обновляем статус
        try:
            file_path.unlink()
        except:
            pass
        
        await db.mark_video_deleted(video_hash)
        raise HTTPException(status_code=410, detail="Файл поврежден, требуется повторная загрузка")


    # Определяем MIME type
    mime_type = "video/mp4"
    if file_path.suffix == '.webm':
        mime_type = "video/webm"
    elif file_path.suffix == '.mkv':
        mime_type = "video/x-matroska"
    
    # Получаем очищенное название для скачивания
    filename = f"{normalize_title(video.get('title', video_hash))}{file_path.suffix}"
    
    # Возвращаем файл с поддержкой Range запросов
    return FileResponse(
        path=file_path,
        media_type=mime_type,
        filename=filename,
        content_disposition_type="inline"
    )

@app.get("/info/{video_hash}", response_model=VideoMetadata)
async def get_video_info(video_hash: str):
    """Получает метаданные видео"""
    video = await db.get_video(video_hash)
    
    if not video:
        raise HTTPException(status_code=404, detail="Видео не найдено")
    
    return VideoMetadata(**video)

@app.get("/health")
async def health_check():
    """Проверка здоровья приложения"""
    try:
        # Проверяем соединение с БД
        await db.get_storage_stats()
        
        return {"status": "healthy", "service": "video-server"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")

@app.get("/queue/info")
async def get_queue_info():
    """Получает информацию об очереди"""
    info = await queue.get_queue_info()
    return info

@app.get("/storage/info")
async def get_storage_info_detailed():
    """
    Детальная информация о хранилище
    """
    info = await storage.get_storage_info()
    
    # Добавляем дополнительную информацию
    videos = await db.get_all_ready_videos()
    
    # Сортируем по дате последнего доступа
    def sort_key(item):
        return get_date_sort_key(item, 'last_accessed')

    videos_sorted = sorted(videos, key=sort_key, reverse=True)
    
    # Топ 10 самых старых видео (кандидаты на удаление)
    oldest_videos = videos_sorted[:10] if len(videos_sorted) > 10 else videos_sorted
    
    return {
        **info,
        "total_videos": len(videos),
        "oldest_videos": [{
            "hash": v['hash'][:12] + '...',
            "title": v.get('title', 'Без названия'),
            "last_accessed": v.get('last_accessed'),
            "access_count": v.get('access_count', 0),
            "file_size_mb": round(v.get('file_size', 0) / 1024**2, 2) if v.get('file_size') else 0,
        } for v in oldest_videos]
    }

@app.post("/cleanup")
async def trigger_cleanup(aggressive: bool = False):
    """
    Ручная очистка хранилища
    
    Args:
        aggressive: Если True, удаляет больше видео для создания запаса места
    """
    deleted = await storage.cleanup_old_videos(aggressive=aggressive)
    
    # Получаем статистику после очистки
    info = await storage.get_storage_info()
    
    return {
        "message": f"Очистка завершена, удалено {len(deleted)} видео",
        "deleted_count": len(deleted),
        "deleted_hashes": deleted[:10],  # Первые 10 хешей
        "storage_info": info,
        "aggressive_mode": aggressive
    }
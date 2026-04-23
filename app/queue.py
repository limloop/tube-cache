"""
Очередь с фиксированным количеством воркеров
"""
import asyncio
import time
import logging
from typing import Dict, Any, Optional, Deque, List
from collections import deque
from dataclasses import dataclass, field

from app.config import settings
from app.downloader import VideoDownloader
from app.database import db
from app.models import VideoStatus
from app.storage import storage
from app import logger

@dataclass
class DownloadTask:
    """Задача на загрузку"""
    video_hash: str
    url: str
    added_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    retry_count: int = 0
    worker_id: Optional[int] = None

class TaskQueue:
    """Очередь с фиксированным количеством воркеров"""
    
    def __init__(self):
        self.downloader = VideoDownloader()
        
        # Основные структуры данных
        self._queue: Deque[DownloadTask] = deque()           # Очередь ожидания
        self._active_tasks: Dict[str, DownloadTask] = {}     # Активные задачи по хешу
        self._task_futures: Dict[str, asyncio.Future] = {}   # Futures для ожидания
        
        # Состояние системы
        self._is_running = False
        self._lock = asyncio.Lock()
        self._max_concurrent = settings.download.max_concurrent
        self._download_timeout = settings.download.timeout_seconds
        self._max_retries = settings.download.retry_attempts

        # Фиксированные воркеры
        self._workers: List[asyncio.Task] = []
        
        # Мониторинг
        self._cleanup_monitor_task: Optional[asyncio.Task] = None
        
        # Статистика
        self._stats = {
            'added': 0,
            'completed': 0,
            'failed': 0,
            'retried': 0
        }
        
        logger.info(f"Очередь создана (воркеров: {self._max_concurrent})")
    
    async def start(self):
        """Запускает очередь"""
        if self._is_running:
            return
        
        logger.info(f"Запуск очереди с {self._max_concurrent} воркерами...")
        self._is_running = True
        
        # Восстанавливаем задачи из БД
        await self._restore_tasks()
        
        # Создаём фиксированное количество воркеров
        for i in range(self._max_concurrent):
            worker = asyncio.create_task(
                self._worker_loop(i),
                name=f"queue-worker-{i}"
            )
            self._workers.append(worker)
        
        # Запускаем мониторинг зависших задач
        self._cleanup_monitor_task = asyncio.create_task(self._cleanup_monitor_loop())
        
        logger.info(f"✅ Очередь запущена")
        logger.info(f"   Воркеров: {len(self._workers)}")
        logger.info(f"   Задач в очереди: {len(self._queue)}")
        logger.info(f"   Активных задач: {len(self._active_tasks)}")
    
    async def stop(self):
        """Останавливает очередь"""
        logger.info("Остановка очереди...")
        self._is_running = False
        
        # Отменяем все futures
        for future in self._task_futures.values():
            if not future.done():
                future.cancel()
        
        # Останавливаем всех воркеров
        for worker in self._workers:
            if not worker.done():
                worker.cancel()
                try:
                    await worker
                except asyncio.CancelledError:
                    pass
        
        # Останавливаем монитор
        if self._cleanup_monitor_task and not self._cleanup_monitor_task.done():
            self._cleanup_monitor_task.cancel()
            try:
                await self._cleanup_monitor_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ Очередь остановлена")
    
    async def add_task(self, video_hash: str, url: str) -> bool:
        """Добавляет задачу в очередь"""
        async with self._lock:
            # Проверяем, не обрабатывается ли уже эта задача
            if video_hash in self._active_tasks:
                logger.debug(f"Задача уже активна: {video_hash[:12]}")
                return False
            
            # Проверяем, нет ли в очереди
            for task in self._queue:
                if task.video_hash == video_hash:
                    logger.debug(f"Задача уже в очереди: {video_hash[:12]}")
                    return False
            
            # Создаём новую задачу
            task = DownloadTask(video_hash=video_hash, url=url)
            self._queue.append(task)
            self._task_futures[video_hash] = asyncio.Future()
            
            self._stats['added'] += 1
            
            logger.info(f"✅ Задача добавлена: {video_hash[:12]}")
            logger.debug(f"   Позиция в очереди: {len(self._queue)}")
            
            return True
    
    async def get_queue_position(self, video_hash: str) -> Optional[int]:
        """Получает позицию задачи в очереди"""
        async with self._lock:
            for i, task in enumerate(self._queue):
                if task.video_hash == video_hash:
                    return i
            return None
    
    async def get_queue_info(self) -> Dict[str, Any]:
        """Получает информацию об очереди"""
        async with self._lock:
            # Задачи в очереди
            queue_tasks = []
            for i, task in enumerate(list(self._queue)[:20]):
                queue_tasks.append({
                    'hash': task.video_hash[:12] + '...',
                    'url': task.url[:80] + '...' if len(task.url) > 80 else task.url,
                    'retry_count': task.retry_count,
                    'added_at': task.added_at,
                    'position': i + 1
                })
            
            # Активные задачи
            active_tasks_list = []
            current_time = time.time()
            for task in self._active_tasks.values():
                active_tasks_list.append({
                    'hash': task.video_hash[:12] + '...',
                    'url': task.url[:80] + '...' if len(task.url) > 80 else task.url,
                    'retry_count': task.retry_count,
                    'started_at': task.started_at,
                    'worker_id': task.worker_id,
                    'task_age': int(current_time - task.started_at) if task.started_at else 0
                })
            
            # Работающие воркеры
            working_workers = sum(1 for w in self._workers if not w.done())
            
            return {
                'is_running': self._is_running,
                'queued_tasks': len(self._queue),
                'active_tasks': len(self._active_tasks),
                'max_concurrent': self._max_concurrent,
                'working_workers': working_workers,
                'total_workers': len(self._workers),
                'stats': self._stats.copy(),
                'queue': queue_tasks,
                'active_tasks_list': active_tasks_list,
                'has_cleanup_monitor': self._cleanup_monitor_task is not None
            }
    
    async def wait_for_task(self, video_hash: str, timeout: float = 30.0) -> bool:
        """Ожидает завершения задачи"""
        if video_hash not in self._task_futures:
            return False
        
        try:
            await asyncio.wait_for(self._task_futures[video_hash], timeout)
            return True
        except (asyncio.TimeoutError, asyncio.CancelledError):
            return False
    
    async def _restore_tasks(self):
        """Восстанавливает незавершённые задачи из БД"""
        try:
            logger.info("Восстановление задач из БД...")
            
            # Получаем задачи со статусом PENDING
            pending_videos = await db.get_pending_videos()
            
            restored = 0
            for video in pending_videos:
                video_hash = video['hash']
                url = video['source_url']
                
                # Добавляем в очередь
                if await self.add_task(video_hash, url):
                    restored += 1
            
            logger.info(f"Восстановлено задач: {restored}")
            return restored
            
        except Exception as e:
            logger.error(f"Ошибка восстановления задач: {e}")
            return 0
    
    async def _worker_loop(self, worker_id: int):
        """Цикл одного воркера с таймаутом между загрузками"""
        logger.debug(f"Воркер {worker_id} запущен")
        
        while self._is_running:
            try:
                # Берём задачу из очереди
                task = None
                async with self._lock:
                    if self._queue:
                        task = self._queue.popleft()
                        self._active_tasks[task.video_hash] = task
                
                if task:
                    # Обрабатываем задачу
                    task.worker_id = worker_id
                    await self._process_task(task)
                    
                    # ЖДЁМ таймаут между загрузками
                    if self._download_timeout > 0:
                        logger.debug(f"Воркер {worker_id} ждёт {self._download_timeout}с перед следующей задачей")
                        await asyncio.sleep(self._download_timeout)
                else:
                    # Если нет задач, ждём
                    await asyncio.sleep(5)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в воркере {worker_id}: {e}")
                # Ждём перед повторной попыткой даже при ошибке
                if self._download_timeout > 0:
                    await asyncio.sleep(self._download_timeout)
                else:
                    await asyncio.sleep(2)
        
        logger.debug(f"Воркер {worker_id} остановлен")
    
    async def _process_task(self, task: DownloadTask):
        """Обрабатывает одну задачу"""
        task.started_at = time.time()
        
        try:
            logger.info(f"▶️  Воркер {task.worker_id} начинает загрузку: {task.video_hash[:12]}")
            
            # Обновляем статус в БД
            await db.update_status(task.video_hash, VideoStatus.DOWNLOADING)
            
            # Загружаем видео
            result = await self.downloader.download(task.url, task.video_hash)
            
            # Проверяем место в хранилище
            await self._check_storage_space()
            
            # Обновляем БД с результатами
            success = await db.update_video_on_download(
                video_hash=task.video_hash,
                title=result['title'],
                file_size=result['file_size'],
                duration=result['duration'],
                uploader=result['uploader'],
                file_ext=result['file_ext']
            )
            
            if not success:
                raise Exception("Не удалось обновить запись в БД")
            
            # Успешное завершение
            logger.info(f"✅ Воркер {task.worker_id} завершил: {task.video_hash[:12]}")
            self._stats['completed'] += 1
            
            # Помечаем future как выполненное
            if task.video_hash in self._task_futures:
                self._task_futures[task.video_hash].set_result(True)
                
        except Exception as e:
            logger.error(f"❌ Воркер {task.worker_id} ошибка: {task.video_hash[:12]} - {e}")
            await self._handle_task_error(task, e)
            
        finally:
            # Очищаем задачу
            await self._cleanup_task(task)
    
    async def _handle_task_error(self, task: DownloadTask, error: Exception):
        """Обрабатывает ошибку задачи"""
        task.retry_count += 1
        
        if task.retry_count < self._max_retries:
            logger.info(f"🔄 Повтор задачи {task.video_hash[:12]} ({task.retry_count}/{self._max_retries})")
            self._stats['retried'] += 1
            
            # Возвращаем задачу в очередь для повторной попытки
            async with self._lock:
                self._queue.append(task)
            
            # Обновляем статус в БД на PENDING для повторной попытки
            await db.update_status(task.video_hash, VideoStatus.PENDING)
            
        else:
            logger.error(f"🚫 Превышен лимит повторов: {task.video_hash[:12]}")
            self._stats['failed'] += 1
            
            # Обновляем статус в БД на FAILED - окончательная ошибка
            await db.update_status(task.video_hash, VideoStatus.FAILED)
            
            # Помечаем future как завершённое с ошибкой
            if task.video_hash in self._task_futures:
                self._task_futures[task.video_hash].set_exception(error)
    
    async def _cleanup_task(self, task: DownloadTask):
        """Очищает задачу после завершения"""
        async with self._lock:
            # Удаляем из активных задач
            self._active_tasks.pop(task.video_hash, None)
            
            # Удаляем future если он есть
            if task.video_hash in self._task_futures:
                # Не удаляем future если он ещё не завершён (ждёт повторной попытки)
                if self._task_futures[task.video_hash].done():
                    del self._task_futures[task.video_hash]
    
    async def _cleanup_monitor_loop(self):
        """Мониторит и очищает зависшие задачи"""
        logger.debug("Запущен мониторинг зависших задач")
        
        while self._is_running:
            try:
                await asyncio.sleep(60)  # Проверяем каждую минуту
                
                current_time = time.time()
                stale_tasks = []
                
                async with self._lock:
                    # Ищем задачи, которые выполняются слишком долго (> 10 минут)
                    for task in self._active_tasks.values():
                        if task.started_at and (current_time - task.started_at) > 600:
                            stale_tasks.append(task)
                
                # Обрабатываем зависшие задачи
                for task in stale_tasks:
                    logger.warning(f"Обнаружена зависшая задача: {task.video_hash[:12]}")
                    
                    # Обновляем статус в БД на FAILED
                    await db.update_status(task.video_hash, VideoStatus.FAILED)
                    
                    # Удаляем задачу
                    async with self._lock:
                        self._active_tasks.pop(task.video_hash, None)
                    
                    # Завершаем future с ошибкой
                    if task.video_hash in self._task_futures:
                        self._task_futures[task.video_hash].set_exception(
                            Exception("Задача зависла (timeout)")
                        )
                        del self._task_futures[task.video_hash]
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в мониторинге зависших задач: {e}")
    
    async def _check_storage_space(self):
        """Проверяет место в хранилище"""
        try:
            storage_info = await storage.get_storage_info()
            used_percent = storage_info['used_percent']
            
            if used_percent > 90:
                logger.warning(f"Хранилище заполнено на {used_percent:.1f}%, запуск очистки...")
                deleted = await storage.cleanup_old_videos()
                if deleted:
                    logger.info(f"Очищено видео: {len(deleted)}")
            
        except Exception as e:
            logger.error(f"Ошибка проверки хранилища: {e}")

# Глобальный экземпляр
queue = TaskQueue()
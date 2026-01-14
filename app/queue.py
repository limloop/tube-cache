"""
Автономная система очередей на основе asyncio и SQLite
"""
import asyncio
import time
import logging
from typing import Dict, Any, Optional, List, Set
from collections import deque
from dataclasses import dataclass, field

from app.config import settings
from app.downloader import VideoDownloader
from app.database import db
from app.models import VideoStatus

logger = logging.getLogger(__name__)

@dataclass
class DownloadTask:
    """Задача на загрузку"""
    video_hash: str
    url: str
    added_at: float = field(default_factory=time.time)
    retry_count: int = 0
    max_retries: int = 3

class TaskQueue:
    """Автономная очередь задач на основе asyncio"""
    
    def __init__(self):
        self.downloader = VideoDownloader()
        
        # Структуры данных для очереди
        self._queue: deque[DownloadTask] = deque()           # Основная очередь (FIFO)
        self._active_tasks: Set[str] = set()                 # Хеши активных задач
        self._task_cache: Dict[str, DownloadTask] = {}       # Кэш задач по хешу
        self._task_futures: Dict[str, asyncio.Future] = {}   # Futures для ожидания задач
        
        # Состояние очереди
        self._worker_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._lock = asyncio.Lock()
        self._max_concurrent = settings.download.max_concurrent
        
        # Статистика
        self._stats = {
            'total_added': 0,
            'total_processed': 0,
            'total_failed': 0,
            'total_retried': 0,
            'total_restored': 0,
        }
        
        logger.debug(f"Очередь инициализирована (макс. параллельных: {self._max_concurrent})")
    
    async def start(self):
        """Запускает очередь"""
        if self._is_running:
            logger.warning("Очередь уже запущена")
            return
        
        logger.info("Запуск очереди загрузок...")
        self._is_running = True
        
        try:
            # Восстанавливаем незавершенные задачи из БД
            restored = await self._restore_pending_tasks()
            logger.info(f"Восстановлено задач из БД: {restored}")
            
            # Запускаем фоновый воркер
            self._worker_task = asyncio.create_task(self._worker_loop())
            
            # Запускаем мониторинг очереди
            asyncio.create_task(self._monitor_loop())
            
            logger.info(f"✅ Очередь успешно запущена")
            logger.info(f"   Активных задач: {len(self._active_tasks)}")
            logger.info(f"   Задач в очереди: {len(self._queue)}")
            logger.info(f"   Макс. параллельных: {self._max_concurrent}")
            
        except Exception as e:
            self._is_running = False
            logger.error(f"❌ Ошибка запуска очереди: {e}", exc_info=True)
            raise
    
    async def stop(self):
        """Останавливает очередь"""
        logger.info("Остановка очереди...")
        self._is_running = False
        
        # Отменяем все ожидающие futures
        for future in self._task_futures.values():
            if not future.done():
                future.cancel()
        
        # Ждем завершения воркера
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ Очередь остановлена")
    
    async def add_task(self, video_hash: str, url: str) -> bool:
        """
        Добавляет задачу в очередь
        
        Returns:
            True если задача добавлена
        """
        async with self._lock:
            # Проверяем, нет ли уже такой задачи
            if video_hash in self._task_cache:
                logger.debug(f"Задача уже в очереди: {video_hash[:12]}...")
                return False
            
            # Создаем задачу
            task = DownloadTask(video_hash=video_hash, url=url)
            
            # Добавляем в структуры данных
            self._queue.append(task)
            self._task_cache[video_hash] = task
            
            # Создаем future для ожидания завершения
            self._task_futures[video_hash] = asyncio.Future()
            
            # Обновляем статистику
            self._stats['total_added'] += 1
            
            logger.info(f"✅ Задача добавлена: {video_hash[:12]}...")
            logger.debug(f"   Позиция в очереди: {len(self._queue)}")
            logger.debug(f"   Всего задач в кэше: {len(self._task_cache)}")
            
            return True
    
    async def is_processing(self, video_hash: str) -> bool:
        """Проверяет, обрабатывается ли видео"""
        return (video_hash in self._active_tasks or 
                video_hash in self._task_cache)
    
    async def get_queue_position(self, video_hash: str) -> Optional[int]:
        """Получает позицию в очереди"""
        async with self._lock:
            for i, task in enumerate(self._queue):
                if task.video_hash == video_hash:
                    return i
            return None
    
    async def get_queue_info(self) -> Dict[str, Any]:
        """Получает информацию об очереди"""
        async with self._lock:
            return {
                'is_running': self._is_running,
                'queued_tasks': len(self._queue),
                'active_tasks': len(self._active_tasks),
                'cached_tasks': len(self._task_cache),
                'max_concurrent': self._max_concurrent,
                'stats': self._stats.copy(),
                'queue': [{
                    'hash': task.video_hash[:12] + '...',
                    'url': task.url[:50] + '...' if len(task.url) > 50 else task.url,
                    'retry_count': task.retry_count,
                    'added_at': task.added_at
                } for task in list(self._queue)[:10]]  # Только первые 10 задач для отладки
            }
    
    async def wait_for_task(self, video_hash: str, timeout: float = 30.0) -> bool:
        """
        Ожидает завершения задачи
        
        Args:
            video_hash: Хеш задачи
            timeout: Таймаут ожидания в секундах
            
        Returns:
            True если задача завершена успешно
        """
        if video_hash not in self._task_futures:
            return False
        
        try:
            await asyncio.wait_for(self._task_futures[video_hash], timeout)
            return True
        except (asyncio.TimeoutError, asyncio.CancelledError):
            return False
    
    async def _restore_pending_tasks(self) -> int:
        """Восстанавливает незавершенные задачи из БД"""
        try:
            logger.info("Восстановление незавершенных задач из БД...")
            
            # Получаем задачи со статусом pending или downloading
            pending_videos = await db.get_pending_videos()
            
            restored_count = 0
            for video in pending_videos:
                video_hash = video['hash']
                url = video['source_url']
                
                # Сбрасываем статус на pending для повторной попытки
                success = await db.update_status(video_hash, VideoStatus.PENDING)
                
                if success:
                    # Добавляем в очередь
                    added = await self.add_task(video_hash, url)
                    if added:
                        restored_count += 1
                        logger.debug(f"   Восстановлена: {video_hash[:12]}...")
            
            self._stats['total_restored'] = restored_count
            logger.info(f"Восстановлено задач: {restored_count}")
            return restored_count
            
        except Exception as e:
            logger.error(f"Ошибка восстановления задач: {e}", exc_info=True)
            return 0
    
    async def _worker_loop(self):
        """Основной цикл обработки очереди"""
        logger.info("🚀 Фоновый воркер очереди запущен")
        
        try:
            while self._is_running:
                try:
                    # Проверяем, можем ли запустить новую задачу
                    active_count = len(self._active_tasks)
                    
                    if active_count < self._max_concurrent and self._queue:
                        # Берем следующую задачу
                        task = await self._get_next_task()
                        
                        if task:
                            logger.info(f"▶️  Запуск загрузки: {task.video_hash[:12]}...")
                            logger.debug(f"   Активных задач: {active_count + 1}/{self._max_concurrent}")
                            logger.debug(f"   Осталось в очереди: {len(self._queue)}")
                            
                            # Запускаем обработку задачи
                            asyncio.create_task(
                                self._process_task(task),
                                name=f"download-{task.video_hash[:12]}"
                            )
                    
                    # Короткая пауза между итерациями
                    await asyncio.sleep(0.1)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Ошибка в цикле воркера: {e}", exc_info=True)
                    await asyncio.sleep(1)
        
        except asyncio.CancelledError:
            logger.info("Воркер очереди остановлен по запросу")
        except Exception as e:
            logger.error(f"Критическая ошибка в воркере: {e}", exc_info=True)
        finally:
            logger.info("Фоновый воркер очереди завершен")
    
    async def _monitor_loop(self):
        """Цикл мониторинга состояния очереди"""
        logger.debug("Мониторинг очереди запущен")
        
        try:
            while self._is_running:
                # Логируем состояние каждые 30 секунд
                await asyncio.sleep(30)
                
                async with self._lock:
                    if self._queue or self._active_tasks:
                        logger.info(
                            f"📊 Состояние очереди: "
                            f"в очереди={len(self._queue)}, "
                            f"активных={len(self._active_tasks)}, "
                            f"всего обработано={self._stats['total_processed']}"
                        )
                
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Ошибка в мониторе: {e}")
    
    async def _get_next_task(self) -> Optional[DownloadTask]:
        """Получает следующую задачу из очереди"""
        async with self._lock:
            if not self._queue:
                return None
            
            task = self._queue.popleft()
            
            # Добавляем в активные задачи
            self._active_tasks.add(task.video_hash)
            
            return task
    
    async def _process_task(self, task: DownloadTask):
        """Обрабатывает одну задачу загрузки"""
        video_hash = task.video_hash
        
        try:
            # Обновляем статус в БД
            await db.update_status(video_hash, VideoStatus.DOWNLOADING)
            
            # Загружаем видео
            logger.info(f"📥 Начало загрузки: {video_hash[:12]}...")
            result = await self.downloader.download(task.url, video_hash)
            
            # ОБНОВЛЕНИЕ: Перед обновлением БД проверяем место
            await self._check_and_cleanup_storage()
            
            # Обновляем БД с результатами
            success = await db.update_video_on_download(
                video_hash=video_hash,
                title=result['title'],
                file_size=result['file_size'],
                duration=result['duration'],
                uploader=result['uploader'],
                file_ext=result['file_ext']
            )
            
            if success:
                logger.info(f"✅ Видео загружено: {video_hash[:12]}...")
                logger.debug(f"   Размер: {result.get('file_size', 0)} байт")
                
                self._stats['total_processed'] += 1
                
                # Помечаем future как выполненное
                if video_hash in self._task_futures:
                    self._task_futures[video_hash].set_result(True)
            
            else:
                raise Exception("Не удалось обновить запись в БД")
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки {video_hash[:12]}...: {e}")
            
            # Проверяем, нужно ли повторить
            task.retry_count += 1
            
            if task.retry_count < task.max_retries:
                logger.info(f"🔄 Повторная попытка ({task.retry_count}/{task.max_retries}): {video_hash[:12]}...")
                self._stats['total_retried'] += 1
                
                # Возвращаем задачу в очередь
                async with self._lock:
                    self._queue.append(task)
            else:
                # Превышено количество попыток
                logger.error(f"🚫 Превышено количество попыток для: {video_hash[:12]}...")
                await db.update_status(video_hash, VideoStatus.FAILED)
                self._stats['total_failed'] += 1
                
                # Помечаем future как завершенное с ошибкой
                if video_hash in self._task_futures:
                    self._task_futures[video_hash].set_exception(e)
        
        finally:
            # Удаляем из активных задач и кэша
            async with self._lock:
                self._active_tasks.discard(video_hash)
                
                if video_hash in self._task_cache:
                    del self._task_cache[video_hash]
                
                if video_hash in self._task_futures:
                    if not self._task_futures[video_hash].done():
                        self._task_futures[video_hash].cancel()
                    del self._task_futures[video_hash]

    async def _check_and_cleanup_storage(self):
        """
        Проверяет место в хранилище и очищает если нужно
        Вызывается перед сохранением нового файла
        """
        try:
            # Получаем текущую статистику хранилища
            storage_info = await storage.get_storage_info()
            
            used_bytes = storage_info['total_size_bytes']
            max_bytes = storage_info['max_size_bytes']
            used_percent = storage_info['used_percent']
            
            logger.debug(f"📊 Проверка хранилища: {used_percent:.1f}% использовано")
            
            # Если хранилище заполнено более чем на 90%, запускаем очистку
            if used_percent > 90:
                logger.warning(f"⚠️  Хранилище заполнено на {used_percent:.1f}%, запуск очистки...")
                
                deleted = await storage.cleanup_old_videos()
                
                if deleted:
                    logger.info(f"🧹 Очистка завершена, удалено {len(deleted)} видео")
                    
                    # Обновляем статистику после очистки
                    new_info = await storage.get_storage_info()
                    logger.info(f"📊 После очистки: {new_info['used_percent']:.1f}% использовано")
                else:
                    logger.info("🧹 Очистка не потребовалась или не удалась")
            
            # Если совсем нет места (< 1% свободно), очищаем более агрессивно
            elif used_percent > 99:
                logger.error(f"🚨 Критически мало места! {used_percent:.1f}% использовано")
                
                # Можно добавить дополнительную логику очистки
                deleted = await storage.cleanup_old_videos()
                
                if deleted:
                    logger.info(f"🚨 Экстренная очистка, удалено {len(deleted)} видео")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке/очистке хранилища: {e}")

# Глобальный экземпляр очереди
queue = TaskQueue()
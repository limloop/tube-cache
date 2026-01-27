import os
import asyncio
import time
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from app.config import settings
from app.database import db
from app.utils import check_video_file_integrity
from app import logger

class StorageManager:
    """
    Управление хранилищем видеофайлов и их обслуживанием.
    
    Основные функции:
    - Мониторинг заполненности хранилища
    - Автоматическая очистка старых видео при нехватке места
    - Проверка целостности видеофайлов
    - Очистка старых логов
    """
    
    # Поддерживаемые видеоформаты для поиска файлов
    VIDEO_EXTENSIONS = ['.mp4', '.webm', '.mkv', '.avi', '.mov', '.flv', '.wmv']
    
    def __init__(self):
        """Инициализация менеджера хранилища."""
        self.videos_path = Path(settings.storage.videos_path)
        self.max_size_bytes = settings.storage.max_size_gb * (1024 ** 3)
        
        # Настройки из конфигурации
        self.monitoring_interval = settings.storage.monitoring_interval  # секунды
        self.storage_cleanup_threshold = settings.storage.cleanup_threshold  # процент заполнения
        self.target_free_space = settings.storage.target_free_space  # процент свободного места после очистки
        self.log_retention_days = settings.storage.log_retention_days
        self.log_check_interval = settings.storage.log_check_interval
        self.integrity_check_interval = settings.storage.integrity_check_interval  # секунды
        
        # Время последних операций
        self._last_log_cleanup = 0
        self._last_integrity_check = 0
        
        # Флаги выполняющихся операций (защита от повторного запуска)
        self._integrity_check_running = False
        self._cleanup_running = False
        
        self._monitor_task = None
        self._is_monitoring = False
    
    async def start_monitoring(self):
        """Запуск фонового мониторинга хранилища."""
        if self._is_monitoring:
            return
            
        self._is_monitoring = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Мониторинг хранилища запущен")
    
    async def stop_monitoring(self):
        """Остановка фонового мониторинга."""
        self._is_monitoring = False
        
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            
        logger.info("Мониторинг хранилища остановлен")
    
    async def _monitor_loop(self):
        """
        Основной цикл мониторинга.
        
        Выполняет периодические проверки:
        1. Заполненность хранилища
        2. Очистку старых логов (раз в сутки)
        3. Проверку целостности файлов (раз в указанный интервал)
        """
        logger.debug("Запущен цикл мониторинга хранилища")
        
        try:
            while self._is_monitoring:
                await asyncio.sleep(self.monitoring_interval)
                await self._perform_monitoring_checks()
                
        except asyncio.CancelledError:
            logger.debug("Цикл мониторинга остановлен")
        except Exception as e:
            logger.error(f"Ошибка в цикле мониторинга: {e}")
        finally:
            self._is_monitoring = False
    
    async def _perform_monitoring_checks(self):
        """Выполнение всех проверок состояния хранилища."""
        current_time = time.time()
        
        # 1. Проверка заполненности хранилища
        storage_info = await self.get_storage_info()
        if (storage_info['used_percent'] >= self.storage_cleanup_threshold and not self._cleanup_running):
            logger.warning(f"Хранилище заполнено на {storage_info['used_percent']}%, запуск очистки")
            asyncio.create_task(self._safe_cleanup_old_videos())
        
        # 2. Проверка логов - раз в указанный интервал
        if current_time - self._last_log_cleanup > self.log_check_interval:
            await self.cleanup_old_logs()
            self._last_log_cleanup = current_time
        
        # 3. Проверка целостности - раз в указанный интервал
        if (current_time - self._last_integrity_check > self.integrity_check_interval and not self._integrity_check_running):
            asyncio.create_task(self._safe_check_video_integrity())
    
    async def _safe_cleanup_old_videos(self):
        """Безопасная очистка видео с защитой от повторного запуска."""
        if self._cleanup_running:
            logger.debug("Очистка видео уже выполняется, пропускаем")
            return
            
        self._cleanup_running = True
        try:
            await self.cleanup_old_videos()
        finally:
            self._cleanup_running = False
    
    async def _safe_check_video_integrity(self):
        """Безопасная проверка целостности с защитой от повторного запуска."""
        if self._integrity_check_running:
            logger.debug("Проверка целостности уже выполняется, пропускаем")
            return
            
        self._integrity_check_running = True
        try:
            await self.check_all_video_integrity()
            self._last_integrity_check = time.time()
        finally:
            self._integrity_check_running = False

    async def cleanup_old_videos(self) -> List[str]:
        """
        Очистка самых старых видеофайлов для освобождения места.
        
        Возвращает список хешей удаленных видео.
        """
        if self._cleanup_running:
            return []
            
        self._cleanup_running = True
        deleted_hashes = []
        
        try:
            # Получаем видео отсортированные по времени последнего доступа
            videos = await db.get_all_ready_videos()
            videos.sort(key=lambda x: x.get('last_accessed', 0) or 0)
            
            current_size = sum(v.get('file_size', 0) for v in videos)
            target_size = self.max_size_bytes * (1 - self.target_free_space / 100)
            
            # Если места достаточно - выходим
            if current_size <= target_size:
                return deleted_hashes
            
            # Удаляем старые видео пока не освободим достаточно места
            for video in videos:
                if current_size <= target_size:
                    break
                    
                video_hash = video['hash']
                file_size = video.get('file_size', 0)
                
                if not file_size:
                    continue
                
                file_path = self._find_video_file(video_hash)
                if file_path and file_path.exists():
                    try:
                        file_path.unlink()
                        await db.mark_video_deleted(video_hash)
                        
                        current_size -= file_size
                        deleted_hashes.append(video_hash)
                        
                        logger.info(f"Удалено старое видео: {video_hash[:12]} ({file_size:,} bytes)")
                        
                    except Exception as e:
                        logger.error(f"Ошибка удаления видео {video_hash[:12]}: {e}")
            
            if deleted_hashes:
                logger.info(f"Очистка завершена: удалено {len(deleted_hashes)} видео")
                
        except Exception as e:
            logger.error(f"Ошибка очистки видео: {e}")
        finally:
            self._cleanup_running = False
        
        return deleted_hashes
    
    async def cleanup_old_logs(self) -> List[str]:
        """
        Удаление лог-файлов старше указанного количества дней.
        
        Анализирует дату из имени файла (формат: имя_ГГГГ-ММ-ДД.log)
        или использует дату изменения файла.
        """
        deleted_files = []
        logs_dir = Path(settings.storage.logs_path)
        
        if not logs_dir.exists():
            return deleted_files
        
        cutoff_date = datetime.now().timestamp() - (self.log_retention_days * self.log_check_interval)
        
        for log_file in logs_dir.glob("*.log"):
            if not log_file.is_file():
                continue
            
            try:
                # Пытаемся определить дату файла
                file_date = self._get_file_date(log_file)
                
                if file_date.timestamp() < cutoff_date:
                    log_file.unlink(missing_ok=True)
                    deleted_files.append(log_file.name)
                    
            except Exception as e:
                logger.warning(f"Не удалось обработать файл {log_file.name}: {e}")
        
        if deleted_files:
            logger.info(f"Очищено логов: {len(deleted_files)} файлов")
        
        return deleted_files
    
    def _get_file_date(self, file_path: Path) -> datetime:
        """
        Определяет дату файла.
        
        Сначала пытается извлечь дату из имени файла,
        затем использует дату изменения файла.
        """
        # Извлечение даты из имени файла
        date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', file_path.stem)
        if date_match:
            year, month, day = map(int, date_match.groups())
            return datetime(year, month, day)
        
        # Использование даты изменения файла
        return datetime.fromtimestamp(file_path.stat().st_mtime)
    
    async def check_all_video_integrity(self) -> List[str]:
        """
        Проверка целостности всех видеофайлов в хранилище.
        
        Возвращает список хешей поврежденных файлов.
        """
        damaged_files = []
        start_time = time.time()
        
        try:
            videos = await db.get_all_ready_videos()
            total_videos = len(videos)
            
            if total_videos == 0:
                logger.debug("Нет видео для проверки целостности")
                return damaged_files
            
            logger.info(f"Начинаем проверку целостности {total_videos} видео...")
            
            for index, video in enumerate(videos, 1):
                if not self._is_monitoring:
                    logger.info("Проверка целостности прервана (остановлен мониторинг)")
                    break
                    
                video_hash = video['hash']
                file_path = self._find_video_file(video_hash)
                
                if file_path and file_path.exists():
                    try:
                        if not check_video_file_integrity(file_path):
                            damaged_files.append(video_hash)
                            logger.warning(f"Обнаружен поврежденный файл: {video_hash[:12]}")
                            
                            # Автоматическое удаление поврежденного файла
                            file_path.unlink(missing_ok=True)
                            await db.mark_video_deleted(video_hash)
                            
                    except Exception as e:
                        logger.error(f"Ошибка проверки файла {video_hash[:12]}: {e}")
                
                # Логируем прогресс каждые 10% или каждые 10 файлов
                if index % max(10, total_videos // 10) == 0:
                    progress = (index / total_videos) * 100
                    logger.debug(f"Прогресс проверки целостности: {progress:.0f}% ({index}/{total_videos})")
            
            elapsed_time = time.time() - start_time
            logger.info(
                f"Проверка целостности завершена: "
                f"{total_videos} проверено, {len(damaged_files)} повреждено, "
                f"время: {elapsed_time:.1f} сек"
            )
            
        except Exception as e:
            logger.error(f"Ошибка проверки целостности: {e}")
        
        return damaged_files
    
    def _find_video_file(self, video_hash: str) -> Optional[Path]:
        """
        Поиск видеофайла по хешу.
        
        Проверяет файлы с различными расширениями видео.
        """
        for ext in self.VIDEO_EXTENSIONS:
            file_path = self.videos_path / f"{video_hash}{ext}"
            if file_path.exists():
                return file_path
        
        return None
    
    async def get_storage_info(self) -> Dict[str, Any]:
        """
        Получение текущей статистики хранилища.
        
        Возвращает словарь с информацией о размере, заполнении
        и количестве файлов.
        """
        try:
            stats = await db.get_storage_stats()
            used_bytes = stats.get('total_size', 0)
            
            used_percent = 0
            if self.max_size_bytes > 0:
                used_percent = (used_bytes / self.max_size_bytes) * 100
            
            return {
                'total_size_bytes': used_bytes,
                'max_size_bytes': self.max_size_bytes,
                'video_count': stats.get('video_count', 0),
                'used_percent': round(used_percent, 1),
                'free_bytes': max(0, self.max_size_bytes - used_bytes),
                'free_percent': round(max(0, 100 - used_percent), 1)
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики хранилища: {e}")
            return self._get_empty_storage_info()
    
    def _get_empty_storage_info(self) -> Dict[str, Any]:
        """Возвращает пустую статистику при ошибке."""
        return {
            'total_size_bytes': 0,
            'max_size_bytes': self.max_size_bytes,
            'video_count': 0,
            'used_percent': 0,
            'free_bytes': self.max_size_bytes,
            'free_percent': 100
        }
    
    async def find_video_path(self, video_hash: str) -> Optional[Path]:
        """
        Поиск пути к видеофайлу с проверкой доступности.
        
        Возвращает Path если файл существует и доступен для чтения.
        """
        file_path = self._find_video_file(video_hash)
        
        if file_path and file_path.exists() and os.access(file_path, os.R_OK):
            return file_path
        
        return None
    
    def is_monitoring_active(self) -> bool:
        """Проверка активности мониторинга."""
        return self._is_monitoring

# Глобальный экземпляр менеджера хранилища
storage = StorageManager()
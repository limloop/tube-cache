"""
Оптимизированное управление хранилищем
"""
import os
import asyncio
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from app.config import settings
from app.database import db
from app.utils import check_video_file_integrity
from app import logger

class StorageManager:
    """Оптимизированный менеджер хранилища"""
    
    def __init__(self):
        self.videos_path = Path(settings.storage.videos_path)
        self.max_size_bytes = settings.storage.max_size_gb * (1024 ** 3)
        self._monitor_task = None
        self._is_monitoring = False
    
    async def start_monitoring(self):
        """Запускает периодический мониторинг хранилища"""
        if self._is_monitoring:
            return
        
        self._is_monitoring = True
        self._monitor_task = asyncio.create_task(self._monitor_storage())
        logger.info("📊 Мониторинг хранилища запущен")
    
    async def stop_monitoring(self):
        """Останавливает мониторинг хранилища"""
        self._is_monitoring = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("📊 Мониторинг хранилища остановлен")
    
    async def _monitor_storage(self):
        """Периодически проверяет состояние хранилища"""
        try:
            check_counter = 0

            while self._is_monitoring:
                # Проверяем каждые 5 минут
                await asyncio.sleep(3600)  # 5 минут
                check_counter += 1

                # Раз в неделю чистим логи
                if datetime.now().weekday() == 0:  # Каждый понедельник
                    await self.cleanup_old_logs(days_to_keep=7)
                
                # Каждые 12 проверок (1 час) делаем полную проверку файлов
                if check_counter % 12 == 0:
                    logger.info("🚨 Запуск полной проверки целостности файлов...")
                    await self._check_all_video_files()

                info = await self.get_storage_info()
                used_percent = info['used_percent']
                
                # Если хранилище заполнено более чем на 95%, запускаем очистку
                if used_percent > 95:
                    logger.warning(f"📊 Автоматическая проверка: хранилище заполнено на {used_percent:.1f}%")
                    
                    # Запускаем очистку в фоне (не блокируем мониторинг)
                    asyncio.create_task(self.cleanup_old_videos(aggressive=True))
                
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Ошибка в мониторе хранилища: {e}")

    async def _check_all_video_files(self) -> List[str]:
        """
        Проверяет все видеофайлы на целостность
        
        Returns:
            Список поврежденных файлов
        """
        damaged = []
        
        try:
            videos = await db.get_all_ready_videos()
            
            for video in videos:
                video_hash = video['hash']
                file_path = self._find_video_file(video_hash)
                
                if file_path and file_path.exists():
                    if not check_video_file_integrity(file_path):
                        logger.warning(f"Найден поврежденный файл: {video_hash[:12]}...")
                        damaged.append(video_hash)
                        
                        # Удаляем файл и обновляем БД
                        try:
                            file_path.unlink()
                            await db.mark_video_deleted(video_hash)
                        except Exception as e:
                            logger.error(f"Не удалось удалить поврежденный файл {video_hash}: {e}")
            
            if damaged:
                logger.warning(f"Обнаружено поврежденных файлов: {len(damaged)}")
            else:
                logger.info("Все файлы в порядке")
                
            return damaged
            
        except Exception as e:
            logger.error(f"Ошибка проверки файлов: {e}")
            return []

    async def cleanup_old_videos(self, aggressive: bool = False) -> List[str]:
        """
        Оптимизированная очистка старых видео
        
        Args:
            aggressive: Если True, удаляет больше видео для создания запаса места
            
        Returns:
            Список хешей удаленных видео
        """
        deleted_hashes = []
        
        try:
            # Получаем все готовые видео отсортированные по last_accessed
            videos = await db.get_all_ready_videos()
            
            if not videos:
                return deleted_hashes
            
            # Вычисляем общий размер
            total_size = sum(v.get('file_size', 0) for v in videos)
            max_size = self.max_size_bytes
            
            logger.info(f"🧹 Начало очистки: {len(videos)} видео, {total_size/1024**3:.2f} GB")
            logger.info(f"   Лимит: {max_size/1024**3:.2f} GB")
            
            # Определяем целевой размер (сколько хотим освободить)
            if aggressive:
                # Агрессивная очистка: оставляем 50% свободного места
                target_free_percent = 50
            else:
                # Обычная очистка: оставляем 20% свободного места
                target_free_percent = 20
            
            target_size = max_size * (1 - target_free_percent / 100)
            
            # Если уже ниже целевого размера - выходим
            if total_size <= target_size:
                logger.info(f"   Места достаточно, очистка не требуется")
                return deleted_hashes
            
            logger.info(f"   Целевой размер после очистки: {target_size/1024**3:.2f} GB")
            
            # Удаляем самые старые видео
            for video in videos:
                if total_size <= target_size:
                    break
                
                video_hash = video['hash']
                file_size = video.get('file_size', 0)
                
                # Пропускаем видео без размера
                if not file_size or file_size <= 0:
                    continue
                
                # Ищем файл
                file_path = self._find_video_file(video_hash)
                
                if file_path and file_path.exists():
                    try:
                        # ПРОВЕРКА: Если файл поврежден - удаляем в любом случае
                        if not check_video_file_integrity(file_path):
                            logger.warning(f"Обнаружен поврежденный файл: {video_hash[:12]}...")
                            # Помечаем как удаленный в БД
                            await db.mark_video_deleted(video_hash)
                            file_path.unlink()
                            total_size -= file_size
                            deleted_hashes.append(video_hash)
                            continue

                        # Проверяем, не является ли видео "популярным"
                        # Не удаляем видео, к которым недавно обращались
                        last_accessed = video.get('last_accessed')
                        access_count = video.get('access_count', 0)
                        
                        # Если видео смотрели много раз или недавно - пропускаем
                        if (access_count > 10) or (last_accessed and aggressive == False):
                            logger.debug(f"   Пропускаем популярное видео: {video_hash[:12]}...")
                            continue
                        
                        # Удаляем файл
                        file_path.unlink()
                        
                        # Обновляем БД
                        await db.mark_video_deleted(video_hash)
                        
                        # Обновляем счетчики
                        total_size -= file_size
                        deleted_hashes.append(video_hash)
                        
                        logger.info(f"   Удалено: {video_hash[:12]}... ({file_size/1024**2:.1f} MB)")
                        
                    except Exception as e:
                        logger.error(f"   Ошибка удаления файла {video_hash[:12]}...: {e}")
            
            logger.info(f"✅ Очистка завершена: удалено {len(deleted_hashes)} видео")
            logger.info(f"   Осталось места: {(max_size - total_size)/1024**3:.2f} GB")
            
            return deleted_hashes
            
        except Exception as e:
            logger.error(f"❌ Ошибка очистки хранилища: {e}")
            return deleted_hashes

    async def cleanup_old_logs(self, days_to_keep: int = 7) -> List[str]:
        """
        Очищает старые логи
        
        Args:
            days_to_keep: Сколько дней хранить логи
            
        Returns:
            Список удаленных файлов
        """
        deleted = []
        
        try:
            logs_dir = Path(settings.storage.logs_path)
            
            if not logs_dir.exists():
                return deleted
            
            current_time = time.time()
            cutoff_time = current_time - (days_to_keep * 86400)
            
            for log_file in logs_dir.glob("*.log"):
                if log_file.is_file():
                    # Проверяем возраст файла
                    file_age = current_time - log_file.stat().st_mtime
                    
                    if file_age > cutoff_time:
                        try:
                            log_file.unlink()
                            deleted.append(log_file.name)
                            logger.debug(f"Удален старый лог: {log_file.name}")
                        except Exception as e:
                            logger.warning(f"Не удалось удалить лог {log_file}: {e}")
            
            if deleted:
                logger.info(f"Очищено старых логов: {len(deleted)}")
            
            return deleted
            
        except Exception as e:
            logger.error(f"Ошибка очистки логов: {e}")
            return []

    def _find_video_file(self, video_hash: str) -> Optional[Path]:
        """
        Быстрый поиск файла видео по хешу
        """
        # Ищем файл с любым расширением
        for file_path in self.videos_path.glob(f"{video_hash}.*"):
            if file_path.is_file():
                return file_path
        return None
    
    async def get_storage_info(self) -> Dict[str, Any]:
        """
        Быстрое получение информации о хранилище
        """
        try:
            stats = await db.get_storage_stats()
            
            total_size = stats.get('total_size', 0)
            video_count = stats.get('video_count', 0)
            
            used_percent = 0
            if self.max_size_bytes > 0:
                used_percent = min(100, (total_size / self.max_size_bytes) * 100)
            
            return {
                'total_size_bytes': total_size,
                'max_size_bytes': self.max_size_bytes,
                'video_count': video_count,
                'used_percent': round(used_percent, 2)
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения информации о хранилище: {e}")
            return {
                'total_size_bytes': 0,
                'max_size_bytes': self.max_size_bytes,
                'video_count': 0,
                'used_percent': 0
            }
    
    async def find_video_path(self, video_hash: str) -> Optional[Path]:
        """
        Быстрый поиск пути к видеофайлу
        """
        file_path = self._find_video_file(video_hash)
        
        if file_path and file_path.exists():
            return file_path
        
        return None

# Глобальный экземпляр менеджера хранилища
storage = StorageManager()
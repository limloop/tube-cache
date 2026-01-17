"""
Оптимизированная работа с базой данных
"""
import aiosqlite
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from app.config import settings
from app.models import SQL_QUERIES, CREATE_VIDEOS_TABLE_SQL, CREATE_INDEXES_SQL, VideoStatus

logger = logging.getLogger(__name__)

class Database:
    """Класс для оптимизированной работы с SQLite"""
    
    def __init__(self):
        self.db_path = settings.storage.db_path
        self._connection_pool = None
    
    async def connect(self):
        """Устанавливает соединение с базой данных"""
        try:
            # Используем более строгие настройки для производительности
            self.conn = await aiosqlite.connect(
                self.db_path,
                isolation_level=None,  # Автоматические транзакции
                cached_statements=50  # Кэширование подготовленных запросов
            )
            
            # Включаем оптимизации SQLite
            await self.conn.execute("PRAGMA journal_mode = WAL")
            await self.conn.execute("PRAGMA synchronous = NORMAL")
            await self.conn.execute("PRAGMA cache_size = -2000")
            await self.conn.execute("PRAGMA foreign_keys = ON")
            await self.conn.execute("PRAGMA wal_autocheckpoint = 400")
            await self.conn.execute("PRAGMA mmap_size = 268435456")
            
            # Создаем таблицу
            await self.conn.execute(CREATE_VIDEOS_TABLE_SQL)

            # Создаем индексы
            for index_sql in CREATE_INDEXES_SQL:
                await self.conn.execute(index_sql)

            await self.conn.commit()
            
            logger.info(f"База данных подключена: {self.db_path}")
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise
    
    async def close(self):
        """Закрывает соединение с базой данных"""
        if hasattr(self, 'conn') and self.conn:
            await self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            await self.conn.close()
            logger.info("Соединение с БД закрыто")
    
    @asynccontextmanager
    async def transaction(self):
        """Контекстный менеджер для транзакций"""
        try:
            await self.conn.execute("BEGIN")
            yield
            await self.conn.execute("COMMIT")
        except Exception:
            await self.conn.execute("ROLLBACK")
            raise
    
    async def create_video(self, video_hash: str, source_url: str) -> bool:
        """
        Создает запись о видео (оптимизированная версия)
        
        Args:
            video_hash: 64-символьный хеш
            source_url: Исходный URL
            
        Returns:
            True если создано, False если уже существует
        """
        try:
            cursor = await self.conn.execute(
                SQL_QUERIES["insert_video"],
                (video_hash, source_url, VideoStatus.PENDING.value, datetime.now())
            )
            await cursor.close()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка создания видео {video_hash}: {e}")
            return False
    
    async def get_video(self, video_hash: str) -> Optional[Dict[str, Any]]:
        """
        Получает видео по хешу (оптимизированно)
        """
        try:
            cursor = await self.conn.execute(
                SQL_QUERIES["get_video"], 
                (video_hash,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            return None
        except Exception as e:
            logger.error(f"Ошибка получения видео {video_hash}: {e}")
            return None
    
    async def update_video_on_download(
        self,
        video_hash: str,
        title: Optional[str],
        file_size: Optional[int],
        duration: Optional[float],
        uploader: Optional[str],
        file_ext: Optional[str]
    ) -> bool:
        """Оптимизированное обновление после загрузки"""
        try:
            cursor = await self.conn.execute(
                SQL_QUERIES["update_on_download"],
                (title, file_size, VideoStatus.READY.value, 
                 duration, uploader, file_ext, video_hash)
            )
            await cursor.close()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка обновления видео {video_hash}: {e}")
            return False
    
    async def update_access(self, video_hash: str) -> bool:
        """Оптимизированное обновление доступа"""
        try:
            cursor = await self.conn.execute(
                SQL_QUERIES["update_access"], 
                (video_hash,)
            )
            await cursor.close()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка обновления доступа {video_hash}: {e}")
            return False
    
    async def get_videos_by_status(
        self, 
        status: VideoStatus,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Оптимизированное получение видео по статусу"""
        try:
            query = SQL_QUERIES["get_by_status"]
            if limit:
                query += f" LIMIT {limit}"
            
            cursor = await self.conn.execute(query, (status.value,))
            rows = await cursor.fetchall()
            await cursor.close()
            
            videos = []
            if rows:
                columns = [description[0] for description in cursor.description]
                videos = [dict(zip(columns, row)) for row in rows]
            
            return videos
        except Exception as e:
            logger.error(f"Ошибка получения видео по статусу {status}: {e}")
            return []
    
    async def get_all_ready_videos(self) -> List[Dict[str, Any]]:
        """Оптимизированное получение всех готовых видео"""
        try:
            cursor = await self.conn.execute(SQL_QUERIES["get_all_ready"])
            rows = await cursor.fetchall()
            await cursor.close()
            
            videos = []
            if rows:
                columns = [description[0] for description in cursor.description]
                videos = [dict(zip(columns, row)) for row in rows]
            
            return videos
        except Exception as e:
            logger.error(f"Ошибка получения всех готовых видео: {e}")
            return []
    
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Оптимизированное получение статистики"""
        try:
            cursor = await self.conn.execute(SQL_QUERIES["get_storage_stats"])
            row = await cursor.fetchone()
            await cursor.close()
            
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            
            return {'video_count': 0, 'total_size': 0}
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {'video_count': 0, 'total_size': 0}
    
    async def get_video_hash_by_url(self, url: str) -> Optional[str]:
        """Получает хеш видео по URL"""
        try:
            cursor = await self.conn.execute(
                SQL_QUERIES["get_video_by_url"], 
                (url,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            
            return row[0] if row else None
        except Exception as e:
            logger.error(f"Ошибка получения хеша по URL {url}: {e}")
            return None

    async def update_status(self, video_hash: str, status: VideoStatus) -> bool:
        """
        Обновляет статус видео
        
        Args:
            video_hash: Хеш видео
            status: Новый статус
            
        Returns:
            True если обновлено
        """
        try:
            cursor = await self.conn.execute(
                SQL_QUERIES["update_status"],
                (status.value, video_hash)
            )
            await cursor.close()
            
            updated = cursor.rowcount > 0
            if updated:
                logger.debug(f"Статус обновлен: {video_hash} -> {status.value}")
            
            return updated
        except Exception as e:
            logger.error(f"Ошибка обновления статуса видео {video_hash}: {e}")
            return False

    async def mark_video_deleted(self, video_hash: str) -> bool:
        """
        Помечает видео как удаленное
        
        Args:
            video_hash: Хеш видео
            
        Returns:
            True если обновлено
        """
        try:
            cursor = await self.conn.execute(
                SQL_QUERIES["mark_deleted"],
                (video_hash,)
            )
            await cursor.close()
            
            updated = cursor.rowcount > 0
            if updated:
                logger.debug(f"Видео помечено как удаленное: {video_hash}")
            
            return updated
        except Exception as e:
            logger.error(f"Ошибка пометки видео как удаленного {video_hash}: {e}")
            return False

    async def get_pending_videos(self) -> List[Dict[str, Any]]:
        """
        Получает все видео со статусом pending или downloading
        
        Returns:
            Список видео для восстановления очереди
        """
        try:
            cursor = await self.conn.execute("""
                SELECT hash, source_url, status FROM videos 
                WHERE status IN ('pending', 'downloading')
                ORDER BY created_at
            """)
            rows = await cursor.fetchall()
            await cursor.close()
            
            videos = []
            if rows:
                columns = [description[0] for description in cursor.description]
                videos = [dict(zip(columns, row)) for row in rows]
            
            logger.debug(f"Найдено pending/downloading видео: {len(videos)}")
            return videos
        except Exception as e:
            logger.error(f"Ошибка получения pending видео: {e}")
            return []

# Глобальный экземпляр
db = Database()
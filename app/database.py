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
from app import logger

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
    
    async def get_videos_paginated(
        self,
        status: Optional[VideoStatus] = None,
        search: str = "",
        limit: int = 20,
        offset: int = 0
    ) -> tuple[List[Dict[str, Any]], int]:
        """
        Возвращает список видео с пагинацией и общее количество.
        """
        conditions = []
        params = []

        if status:
            conditions.append("status = ?")
            params.append(status.value)

        if search:
            conditions.append("(title LIKE ? OR uploader LIKE ? OR hash LIKE ?)")
            search_pattern = f"%{search}%"
            params.extend([search_pattern, search_pattern, search_pattern])

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Подсчёт общего количества
        count_query = f"SELECT COUNT(*) FROM videos WHERE {where_clause}"
        cursor = await self.conn.execute(count_query, params)
        total = (await cursor.fetchone())[0]
        await cursor.close()

        # Основной запрос с пагинацией
        query = f"""
            SELECT * FROM videos
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """
        cursor = await self.conn.execute(query, params + [limit, offset])
        rows = await cursor.fetchall()
        await cursor.close()

        columns = [desc[0] for desc in cursor.description]
        videos = [dict(zip(columns, row)) for row in rows]
        return videos, total

    async def get_neighbor_videos(self, video_hash: str) -> tuple[Optional[Dict], Optional[Dict]]:
        """Возвращает предыдущее и следующее видео по дате создания."""
        video = await self.get_video(video_hash)
        if not video:
            return None, None
        created_at = video['created_at']

        # Предыдущее (старше)
        cursor = await self.conn.execute(
            "SELECT * FROM videos WHERE status = 'ready' AND created_at < ? ORDER BY created_at DESC LIMIT 1",
            (created_at,)
        )
        prev_row = await cursor.fetchone()
        await cursor.close()
        prev_video = None
        if prev_row:
            columns = [desc[0] for desc in cursor.description]
            prev_video = dict(zip(columns, prev_row))

        # Следующее (новее)
        cursor = await self.conn.execute(
            "SELECT * FROM videos WHERE status = 'ready' AND created_at > ? ORDER BY created_at ASC LIMIT 1",
            (created_at,)
        )
        next_row = await cursor.fetchone()
        await cursor.close()
        next_video = None
        if next_row:
            columns = [desc[0] for desc in cursor.description]
            next_video = dict(zip(columns, next_row))

        return prev_video, next_video

    async def get_random_ready_videos(self, limit: int = 5, exclude_hash: Optional[str] = None) -> List[Dict]:
        """Возвращает случайные READY видео."""
        query = "SELECT * FROM videos WHERE status = 'ready'"
        params = []
        if exclude_hash:
            query += " AND hash != ?"
            params.append(exclude_hash)
        query += " ORDER BY RANDOM() LIMIT ?"
        params.append(limit)

        cursor = await self.conn.execute(query, params)
        rows = await cursor.fetchall()
        await cursor.close()
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    async def get_oldest_videos(self, limit: int = 10) -> List[Dict]:
        """Возвращает самые старые видео по last_accessed или created_at."""
        query = """
            SELECT hash, title, last_accessed, access_count, file_size
            FROM videos
            WHERE status = 'ready'
            ORDER BY last_accessed ASC NULLS LAST, created_at ASC
            LIMIT ?
        """
        cursor = await self.conn.execute(query, (limit,))
        rows = await cursor.fetchall()
        await cursor.close()
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    async def get_total_videos_count(self) -> int:
        """Возвращает общее количество видео (все статусы)."""
        cursor = await self.conn.execute("SELECT COUNT(*) FROM videos")
        count = (await cursor.fetchone())[0]
        await cursor.close()
        return count

    async def get_count_videos(
        self,
        status: Optional[VideoStatus] = None,
        search: str = ""
    ) -> int:
        """Возвращает количество видео по статусу и поисковому запросу."""
        conditions = []
        params = []
        if status is not None:
            conditions.append("status = ?")
            params.append(status.value)
        if search:
            conditions.append("(title LIKE ? OR uploader LIKE ? OR hash LIKE ?)")
            search_pattern = f"%{search}%"
            params.extend([search_pattern, search_pattern, search_pattern])
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"SELECT COUNT(*) FROM videos WHERE {where_clause}"
        cursor = await self.conn.execute(query, params)
        count = (await cursor.fetchone())[0]
        await cursor.close()
        return count

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
        """
        Get videos by status.
        
        Returns:
            List of video dicts (empty list if none found)
        """
        try:
            query = SQL_QUERIES["get_by_status"]
            if limit:
                query += f" LIMIT {limit}"
            
            cursor = await self.conn.execute(query, (status.value,))
            rows = await cursor.fetchall()
            await cursor.close()
            
            if not rows:
                return []
            
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting videos by status {status}: {e}")
            return []


    async def get_all_ready_videos(self) -> List[Dict[str, Any]]:
        """
        Get all ready videos.
        
        Returns:
            List of video dicts (empty list if none found)
        """
        try:
            cursor = await self.conn.execute(SQL_QUERIES["get_all_ready"])
            rows = await cursor.fetchall()
            await cursor.close()
            
            if not rows:
                return []
            
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting all ready videos: {e}")
            return []


    async def get_all_videos(self) -> List[Dict[str, Any]]:
        """
        Get all videos.
        
        Returns:
            List of video dicts (empty list if none found)
        """
        try:
            cursor = await self.conn.execute(SQL_QUERIES["get_all"])
            rows = await cursor.fetchall()
            await cursor.close()
            
            if not rows:
                return []
            
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting all videos: {e}")
            return []

    
    async def get_pending_videos(self) -> List[Dict[str, Any]]:
        """
        Get all pending or downloading videos for queue recovery.
        
        Returns:
            List of video dicts (empty list if none found)
        """
        try:
            cursor = await self.conn.execute("""
                SELECT hash, source_url, status FROM videos 
                WHERE status IN ('pending', 'downloading')
                ORDER BY created_at
            """)
            rows = await cursor.fetchall()
            await cursor.close()
            
            if not rows:
                return []
            
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting pending videos: {e}")
            return []


    async def get_storage_stats(self) -> Dict[str, Any]:
        """
        Get storage statistics.
        
        Returns:
            Dict with video_count and total_size
        """
        try:
            cursor = await self.conn.execute(SQL_QUERIES["get_storage_stats"])
            row = await cursor.fetchone()
            await cursor.close()
            
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            
            return {'video_count': 0, 'total_size': 0}
            
        except Exception as e:
            logger.error(f"Error getting storage stats: {e}")
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

# Глобальный экземпляр
db = Database()
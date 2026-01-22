"""
Модели данных с оптимизированными SQL запросами
"""
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional
from enum import Enum

# --- Enums ---

class VideoStatus(str, Enum):
    """Статусы видео"""
    PENDING = "pending"
    DOWNLOADING = "downloading" 
    READY = "ready"
    FAILED = "failed"
    DELETED = "deleted"

# --- Pydantic модели (для API) ---

class VideoRequest(BaseModel):
    """Запрос на скачивание видео"""
    url: str = Field(..., description="URL видео для скачивания")

class TaskStatus(BaseModel):
    """Статус задачи"""
    hash: str = Field(..., description="Хеш видео")
    status: VideoStatus = Field(..., description="Статус")
    message: Optional[str] = Field(None, description="Дополнительное сообщение")
    stream_url: Optional[str] = Field(None, description="URL для стриминга")

class VideoMetadata(BaseModel):
    """Метаданные видео"""
    hash: str = Field(..., description="64-символьный хеш")
    source_url: str = Field(..., description="Исходный URL")
    title: Optional[str] = Field(None, description="Очищенное название")
    file_size: Optional[int] = Field(None, description="Размер в байтах")
    last_accessed: Optional[datetime] = Field(None, description="Дата последнего просмотра")
    access_count: int = Field(default=0, description="Количество просмотров")
    status: VideoStatus = Field(default=VideoStatus.PENDING, description="Статус")
    created_at: datetime = Field(default_factory=datetime.now, description="Дата создания")
    duration: Optional[float] = Field(None, description="Длительность в секундах")
    uploader: Optional[str] = Field(None, description="Автор/канал")
    file_ext: Optional[str] = Field(None, description="Расширение файла")
    
    model_config = ConfigDict(from_attributes=True)

class StorageInfo(BaseModel):
    """Информация о хранилище"""
    total_size_bytes: int = Field(..., description="Общий размер в байтах")
    max_size_bytes: int = Field(..., description="Максимальный размер в байтах")
    video_count: int = Field(..., description="Количество видео")
    used_percent: float = Field(..., description="Использовано в процентах")

# --- SQL запросы (оптимизированные) ---

# Создание таблицы с индексами
CREATE_VIDEOS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS videos (
    hash TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    title TEXT,
    file_size INTEGER,
    last_accessed TIMESTAMP,
    access_count INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    duration REAL,
    uploader TEXT,
    file_ext TEXT
)
"""

# Создание индексов отдельными командами
CREATE_INDEXES_SQL = [
    # Индекс для быстрого поиска по статусу и дате последнего доступа
    "CREATE INDEX IF NOT EXISTS idx_status_last_accessed ON videos (status, last_accessed)",
    
    # Индекс для поиска по хешу и статусу
    "CREATE INDEX IF NOT EXISTS idx_hash_status ON videos (hash, status)",
    
    # Индекс для поиска по URL источника
    "CREATE INDEX IF NOT EXISTS idx_source_url ON videos (source_url)",
    
    # Индекс для быстрой сортировки по дате создания
    "CREATE INDEX IF NOT EXISTS idx_created_at ON videos (created_at)",
    
    # Индекс для очистки хранилища (статус + размер файла)
    "CREATE INDEX IF NOT EXISTS idx_status_file_size ON videos (status, file_size)"
]


# Подготовленные SQL запросы
SQL_QUERIES = {
    "insert_video": """
        INSERT OR IGNORE INTO videos (hash, source_url, status, created_at)
        VALUES (?, ?, ?, ?)
    """,
    
    "get_video": """
        SELECT * FROM videos WHERE hash = ?
    """,
    
    "update_on_download": """
        UPDATE videos 
        SET title = ?, file_size = ?, status = ?, duration = ?, 
            uploader = ?, file_ext = ?
        WHERE hash = ?
    """,
    
    "update_access": """
        UPDATE videos 
        SET last_accessed = CURRENT_TIMESTAMP, access_count = access_count + 1
        WHERE hash = ?
    """,
    
    "update_status": """
        UPDATE videos SET status = ? WHERE hash = ?
    """,
    
    "get_by_status": """
        SELECT * FROM videos WHERE status = ? ORDER BY last_accessed
    """,
    
    "get_all_ready": """
        SELECT * FROM videos 
        WHERE status = 'ready' AND file_size IS NOT NULL 
        ORDER BY last_accessed
    """,

    "get_all": """
        SELECT * FROM videos 
        ORDER BY last_accessed
    """,
    
    "get_storage_stats": """
        SELECT 
            COUNT(*) as video_count,
            COALESCE(SUM(file_size), 0) as total_size
        FROM videos 
        WHERE status = 'ready' AND file_size IS NOT NULL
    """,
    
    "mark_deleted": """
        UPDATE videos 
        SET status = 'deleted', file_size = NULL, file_ext = NULL
        WHERE hash = ?
    """,
    
    "get_video_by_url": """
        SELECT hash FROM videos WHERE source_url = ? LIMIT 1
    """
}
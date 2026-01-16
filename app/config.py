"""
Загрузка и управление конфигурацией приложения
"""
from pydantic import BaseModel, Field
from typing import Dict, Optional
import yaml
import os
import argparse
import sys
from pathlib import Path

# --- Модели конфигурации ---

class StorageConfig(BaseModel):
    """Конфигурация хранилища"""
    base_path: str = Field(default="./data", description="Базовый путь для всех данных")
    max_size_gb: int = Field(default=50, ge=1, description="Максимальный размер в GB")
    
    @property
    def videos_path(self) -> str:
        """Путь к папке с видео"""
        return str(Path(self.base_path) / "videos")
    
    @property
    def temp_path(self) -> str:
        """Путь к папке с временными файлами"""
        return str(Path(self.base_path) / "temp")

    @property
    def db_path(self) -> str:
        """Путь к базе данных"""
        return str(Path(self.base_path) / "metadata.db")
    
    @property
    def logs_path(self) -> str:
        """Путь к логам"""
        return str(Path(self.base_path) / "logs")

class DownloadConfig(BaseModel):
    """Конфигурация загрузки"""
    max_concurrent: int = Field(default=2, ge=1, description="Максимум параллельных загрузок")
    timeout_seconds: int = Field(default=300, ge=60, description="Таймаут загрузки в секундах")
    retry_attempts: int = Field(default=2, ge=0, description="Количество попыток повтора")

class ServerConfig(BaseModel):
    """Конфигурация сервера"""
    host: str = Field(default="0.0.0.0", description="Хост для запуска сервера")
    port: int = Field(default=8000, ge=1, le=65535, description="Порт для запуска сервера")
    workers: int = Field(default=1, ge=1, description="Количество воркеров uvicorn")

class SourceConfig(BaseModel):
    """Конфигурация для источника видео"""
    format: str = Field(default="best", description="Формат видео (yt-dlp format)")
    extract_audio: bool = Field(default=False, description="Извлекать только аудио")

class AppConfig(BaseModel):
    """Основная конфигурация приложения"""
    storage: StorageConfig
    download: DownloadConfig
    server: ServerConfig
    sources: Dict[str, SourceConfig]
    
    class Config:
        arbitrary_types_allowed = True

# --- Загрузка конфигурации ---

def load_config(config_path: Optional[str] = None) -> AppConfig:
    """
    Загружает конфигурацию из YAML файла
    
    Args:
        config_path: Путь к конфигурационному файлу. Если None, ищет config.yaml в корне.
    
    Returns:
        AppConfig: Загруженная конфигурация
    
    Raises:
        FileNotFoundError: Если конфигурационный файл не найден
        yaml.YAMLError: Если ошибка парсинга YAML
    """
    if config_path is None:
        # Ищем config.yaml в корне проекта
        current_dir = Path(__file__).parent.parent
        config_path = current_dir / "config.yaml"
    
    if not Path(config_path).exists():
        raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)
    
    # Создаем объект конфигурации
    config = AppConfig(
        storage=StorageConfig(**config_data.get('storage', {})),
        download=DownloadConfig(**config_data.get('download', {})),
        server=ServerConfig(**config_data.get('server', {})),
        sources={}
    )
    
    # Обрабатываем источники
    sources_data = config_data.get('sources', {})
    for source_name, source_config in sources_data.items():
        config.sources[source_name] = SourceConfig(**source_config)
    
    # Создаем необходимые папки
    _create_required_dirs(config.storage)
    
    return config

def _create_required_dirs(storage_config: StorageConfig):
    """Создает необходимые директории"""
    dirs_to_create = [
        storage_config.base_path,
        storage_config.temp_path,
        storage_config.videos_path,
        storage_config.logs_path
    ]
    
    for directory in dirs_to_create:
        Path(directory).mkdir(parents=True, exist_ok=True)

# Функция для парсинга аргументов
def parse_args():
    parser = argparse.ArgumentParser(description="Video Server")
    parser.add_argument(
        "--config", 
        "-c",
        type=str, 
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    return parser.parse_args()

# Получаем аргументы
args = parse_args()
settings = load_config(args.config)
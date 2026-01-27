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
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime


# --- Модели конфигурации ---

class StorageConfig(BaseModel):
    """Конфигурация хранилища"""
    base_path: str = Field(
        default="./data",
        description="Базовый путь для всех данных"
    )
    max_size_gb: int = Field(
        default=50,
        ge=1,
        description="Максимальный размер в GB"
    )
    monitoring_interval: int = Field(
        default=360,
        ge=60,
        description="Интервал мониторинга хранилища в секундах (минимум 60)"
    )
    
    cleanup_threshold: int = Field(
        default=90,
        ge=50,
        le=100,
        description="Порог заполнения для запуска очистки в процентах (50-100%)"
    )
    
    target_free_space: int = Field(
        default=60,
        ge=10,
        le=90,
        description="Целевой процент свободного места после очистки (10-90%)"
    )
    
    log_retention_days: int = Field(
        default=7,
        ge=1,
        description="Количество дней хранения лог-файлов"
    )
    
    log_check_interval: int = Field(
        default=14400,
        ge=3600,
        description="Интервал проверки логов для очистки в секундах (минимум 3600)"
    )
    
    integrity_check_interval: int = Field(
        default=3600,
        ge=300,
        description="Интервал проверки целостности видеофайлов в секундах (минимум 300)"
    )
    
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

def setup_logging():
    """
    Настраивает логирование в файлы и консоль
    """
    logs_dir = Path(settings.storage.logs_path)
    
    # Создаем форматтер
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Файл логов по дате
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_file = logs_dir / f"server_{current_date}.log"
    
    # Хендлер для файла (ротация по 10MB, максимум 5 файлов)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    
    # Хендлер для консоли
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    # Настраиваем корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Очищаем существующие хендлеры
    root_logger.handlers.clear()
    
    # Добавляем хендлеры
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Уменьшаем логирование для некоторых библиотек
    logging.getLogger('yt_dlp').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Логирование настроено. Файл: {log_file}")
    
    return logger


# Получаем аргументы
args = parse_args()
settings = load_config(args.config)

# Инициализируем логирование
logger = setup_logging()
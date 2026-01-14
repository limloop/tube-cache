"""
Вспомогательные утилиты с предкомпилированными регулярными выражениями
"""
import re
import hashlib
from urllib.parse import urlparse
from typing import Tuple, Optional
from app.config import settings

# Предкомпилированные регулярные выражения для производительности
_UNSAFE_CHARS_PATTERN = re.compile(r'[<>:"/\\|?*\x00-\x1F\x7F]')
_MULTIPLE_SPACES_PATTERN = re.compile(r'\s+')
_WHITESPACE_PATTERN = re.compile(r'^\s+|\s+$')

def normalize_title(title: str) -> str:
    """
    Очищает название видео только от опасных символов
    Сохраняет эмодзи, нелатинские символы и оригинальный формат
    
    Args:
        title: Исходное название
        
    Returns:
        Очищенное название для безопасного хранения в БД
    """
    if not title or not isinstance(title, str):
        return ""
    
    # ТОЛЬКО удаляем опасные символы для файловой системы
    title = _UNSAFE_CHARS_PATTERN.sub('', title)
    
    # Заменяем множественные пробелы на один (опционально)
    title = _MULTIPLE_SPACES_PATTERN.sub(' ', title)
    
    # Убираем пробелы по краям
    title = _WHITESPACE_PATTERN.sub('', title)
    
    return title

def extract_domain(url: str) -> str:
    """
    Извлекает чистый домен из URL
    
    Args:
        url: URL видео
        
    Returns:
        Домен без www (например, 'youtube.com')
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Убираем www., порт и приводим к нижнему регистру
        domain = domain.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
            
        return domain
    except Exception:
        return "unknown"

def generate_video_hash(url: str, quality_params: str = "") -> str:
    """
    Генерирует уникальный 64-символьный SHA256 хеш для видео
    
    Args:
        url: URL видео
        quality_params: Параметры качества (из конфига)
        
    Returns:
        64-символьный hex хеш
    """
    # Комбинируем URL и параметры качества
    combined = f"{url}|{quality_params}"
    
    # Генерируем SHA256 хеш
    return hashlib.sha256(combined.encode('utf-8')).hexdigest()

def get_download_config_for_url(url: str) -> Tuple[str, bool]:
    """
    Получает конфигурацию загрузки для конкретного URL
    
    Args:
        url: URL видео
        
    Returns:
        Кортеж (format, extract_audio)
    """
    domain = extract_domain(url)
    
    # Ищем конфиг для конкретного домена
    if domain in settings.sources:
        source_config = settings.sources[domain]
    else:
        # Используем конфиг по умолчанию
        source_config = settings.sources.get("default")
    
    return source_config.format, source_config.extract_audio

def format_file_size(size_bytes: Optional[int]) -> str:
    """
    Форматирует размер файла в читаемый вид
    
    Args:
        size_bytes: Размер в байтах или None
        
    Returns:
        Отформатированная строка
    """
    if size_bytes is None:
        return "Unknown"
    
    units = ['B', 'KB', 'MB', 'GB']
    size = float(size_bytes)
    
    for unit in units:
        if size < 1024.0 or unit == 'GB':
            if unit == 'B':
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    
    return f"{size:.1f} TB"
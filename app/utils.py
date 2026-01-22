"""
Вспомогательные утилиты с предкомпилированными регулярными выражениями
"""
import re
import hashlib
import subprocess
import os
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
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

def normalize_youtube_url(url: str) -> str:
    """
    Нормализует YouTube URL к стандартному виду
    
    Извлекает только video_id, убирает все остальные параметры:
    - playlist, list, index, t, start, end, feature, etc.
    - Оставляет только v параметр
    
    Returns: https://www.youtube.com/watch?v=VIDEO_ID
    
    Args:
        url: Любой YouTube URL
        
    Returns:
        Нормализованный URL только с video_id
    """
    try:
        # Добавляем схему если ее нет
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Нормализуем домен
        if domain in ['youtu.be', 'www.youtu.be']:
            # Короткие ссылки youtu.be: https://youtu.be/VIDEO_ID?t=10
            # Извлекаем video_id из пути, игнорируем все параметры
            video_id = parsed.path.strip('/').split('?')[0]  # Убираем query из пути если есть
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"
        
        elif 'youtube.com' in domain:
            # Все варианты youtube.com
            query_params = parse_qs(parsed.query)
            video_id = query_params.get('v', [None])[0]
            
            # Если v параметра нет в query, проверяем другие форматы
            if not video_id:
                # Проверяем format /embed/VIDEO_ID
                if '/embed/' in parsed.path:
                    video_id = parsed.path.split('/embed/')[1].split('?')[0].split('/')[0]
                # Проверяем format /v/VIDEO_ID
                elif '/v/' in parsed.path:
                    video_id = parsed.path.split('/v/')[1].split('?')[0].split('/')[0]
                # Проверяем format /watch/v=ID (редкий)
                elif '/watch' in parsed.path:
                    path_parts = parsed.path.split('/')
                    for part in path_parts:
                        if part.startswith('v='):
                            video_id = part[2:]
                            break
            
            if video_id:
                # Убираем все лишние символы из video_id
                # Иногда video_id может содержать дополнительные параметры
                video_id = video_id.split('&')[0].split('?')[0].split('#')[0]
                
                # Создаем нормализованный URL ТОЛЬКО с v параметром
                normalized_query = urlencode({'v': video_id})
                normalized_url = urlunparse((
                    'https',
                    'www.youtube.com',
                    '/watch',
                    '',
                    normalized_query,
                    ''
                ))
                return normalized_url
        
        # Если не удалось распознать как YouTube - возвращаем как есть
        return url
        
    except Exception:
        return url


def clean_and_validate_url(url: str) -> Optional[str]:
    """
    Очищает URL от кавычек и лишних символов, проверяет валидность
    
    Args:
        url: Сырой URL (может быть в кавычках, с пробелами и т.д.)
        
    Returns:
        Очищенный валидный URL или None если URL невалиден
    """
    if not url or not isinstance(url, str):
        return None
    
    # Убираем начальные и конечные пробелы
    url = url.strip()
    
    # Убираем кавычки всех типов
    quotes = ['"', "'", '`', '```', '````', '«', '»', '“', '”']
    for quote in quotes:
        if url.startswith(quote) and url.endswith(quote):
            url = url[1:-1].strip()
    
    # Убираем угловые скобки (редко, но бывает)
    if url.startswith('<') and url.endswith('>'):
        url = url[1:-1].strip()
    
    # Проверяем, что это похоже на URL
    # Должен содержать точку (домен) и слеш или знак вопроса
    if '.' not in url:
        return None
    
    # Проверяем минимальную длину
    if len(url) < 10:  # http://a.b минимальная длина
        return None
    
    # Проверяем наличие схемы, добавляем если нет
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Дополнительная валидация через urlparse
    try:
        parsed = urlparse(url)
        
        # Должен быть домен
        if not parsed.netloc or '.' not in parsed.netloc:
            return None
        
        # Домен должен быть не короче 3 символов (a.b)
        if len(parsed.netloc) < 3:
            return None
        
        return url
        
    except Exception:
        return None

def normalize_video_url(url: str) -> Optional[str]:
    """
    Распознает и нормализует URL видео
    
    Args:
        url: URL видео (может быть с кавычками, пробелами и т.д.)
        
    Returns:
        Нормализованный URL или None если URL невалиден
    """
    # Сначала очищаем URL
    cleaned_url = clean_and_validate_url(url)
    
    if not cleaned_url:
        return None
    
    try:
        parsed = urlparse(cleaned_url)
        domain = parsed.netloc.lower()
        
        # Убираем www.
        if domain.startswith('www.'):
            domain = domain[4:]
        
        # Нормализация по доменам
        if domain in ['youtube.com', 'youtu.be']:
            return normalize_youtube_url(cleaned_url)
        
        # Для других источников можно добавить нормализацию
        
        # По умолчанию возвращаем очищенный URL
        return cleaned_url
        
    except Exception:
        return cleaned_url

def check_video_file_integrity(file_path: Path) -> bool:
    """
    Проверяет целостность видеофайла с помощью ffprobe
    
    Args:
        file_path: Путь к файлу
        
    Returns:
        True если файл валиден, False если поврежден
    """
    try:
        if not file_path.exists():
            return False
        
        # Быстрая проверка размера
        file_size = file_path.stat().st_size
        if file_size < 1024 * 100:  # Меньше 100KB - точно битый
            return False
        
        # Проверка через ffprobe
        cmd = [
            'ffprobe',
            '-v', 'error',
            '-select_streams', 'v:0',
            '-count_frames',
            '-show_entries', 'stream=codec_type',
            '-of', 'csv=p=0',
            str(file_path)
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=5  # Таймаут 5 секунд
        )
        
        # Если ffprobe завершился успешно и нашел видеопоток
        return result.returncode == 0 and 'video' in result.stdout
        
    except subprocess.TimeoutExpired:
        # Файл слишком сложный для быстрой проверки, считаем валидным
        return True
    except Exception:
        return False


def get_date_sort_key(item, key):
    val = item.get(key)
    if not val:
        return 0
    
    if isinstance(val, (int, float)):
        return val
    
    if isinstance(val, str):
        try:
            # Пробуем разные форматы даты
            formats = [
                # С пробелом вместо T
                "%Y-%m-%d %H:%M:%S.%f",          # 2026-01-15 01:31:48.301245
                "%Y-%m-%d %H:%M:%S",             # 2026-01-15 01:31:48
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(val, fmt)
                    return dt.timestamp()
                except ValueError:
                    continue
            
            # Если ни один формат не подошел
            return 0
        except:
            return 0
    
    return 0
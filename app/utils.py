"""
Вспомогательные утилиты с предкомпилированными регулярными выражениями
"""
import re
import hashlib
import subprocess
import asyncio
import os
import json
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
from typing import Tuple, Dict, Any, Optional
from app.config import settings
from app import logger

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
    Простая проверка целостности файла
    """
    try:
        if not file_path.exists():
            return False
        
        file_size = file_path.stat().st_size
        if file_size == 0:
            return False
        
        # Минимальный размер для видео/аудио
        if file_size < 1024 * 10:  # 10KB
            return False
        
        # Пробуем прочитать файл
        with open(file_path, 'rb') as f:
            header = f.read(1024)
            if len(header) == 0:
                return False
            
            # Проверяем что файл не полностью состоит из нулей
            if all(b == 0 for b in header[:100]):
                return False
        
        return True
        
    except Exception:
        return False

async def check_video_file_integrity_extended(
    file_path: Path, 
    expected_size: Optional[int] = None
) -> Dict[str, Any]:
    """
    Расширенная проверка целостности видеофайла
    
    Args:
        file_path: Путь к файлу
        expected_size: Ожидаемый размер файла в байтах (опционально)
    
    Returns:
        Dict с результатами проверки:
        {
            'valid': bool,
            'reason': str (если not valid),
            'file_size': int,
            'has_video_stream': bool,
            'has_audio_stream': bool,
            'duration': Optional[float]
        }
    """
    try:
        # 1. Проверка существования файла
        if not file_path.exists():
            return {
                'valid': False,
                'reason': 'Файл не существует',
                'file_size': 0,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
        # 2. Проверка размера файла
        file_size = file_path.stat().st_size
        
        if file_size == 0:
            return {
                'valid': False,
                'reason': 'Файл пустой (0 байт)',
                'file_size': 0,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
        # 3. Проверка минимального размера
        MIN_SIZE = 1024 * 10  # 10KB минимальный размер для видео/аудио
        if file_size < MIN_SIZE:
            return {
                'valid': False,
                'reason': f'Файл слишком мал ({file_size} байт)',
                'file_size': file_size,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
        # 4. Проверка ожидаемого размера (если указан)
        if expected_size and abs(file_size - expected_size) > (expected_size * 0.1):  # 10% допуск
            return {
                'valid': False,
                'reason': f'Размер файла не совпадает: {file_size} != {expected_size}',
                'file_size': file_size,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
        # 5. Быстрая проверка чтения файла
        try:
            with open(file_path, 'rb') as f:
                # Читаем начало файла
                header = f.read(1024)
                if len(header) < 100:
                    return {
                        'valid': False,
                        'reason': 'Не удалось прочитать начало файла',
                        'file_size': file_size,
                        'has_video_stream': False,
                        'has_audio_stream': False,
                        'duration': None
                    }
                
                # Проверяем что файл не полностью состоит из нулей
                if all(b == 0 for b in header[:100]):
                    return {
                        'valid': False,
                        'reason': 'Файл состоит только из нулей',
                        'file_size': file_size,
                        'has_video_stream': False,
                        'has_audio_stream': False,
                        'duration': None
                    }
                
                # Для маленьких файлов читаем весь файл
                if file_size < 1024 * 1024:  # < 1MB
                    f.seek(0)
                    entire_file = f.read()
                    if len(entire_file) != file_size:
                        return {
                            'valid': False,
                            'reason': 'Не удалось прочитать весь файл',
                            'file_size': file_size,
                            'has_video_stream': False,
                            'has_audio_stream': False,
                            'duration': None
                        }
        except Exception as e:
            return {
                'valid': False,
                'reason': f'Ошибка чтения файла: {str(e)}',
                'file_size': file_size,
                'has_video_stream': False,
                'has_audio_stream': False,
                'duration': None
            }
        
        # 6. Проверка через ffprobe (если доступен)
        ffprobe_result = await _check_with_ffprobe(file_path)
        
        if ffprobe_result['valid']:
            return {
                'valid': True,
                'reason': None,
                'file_size': file_size,
                'has_video_stream': ffprobe_result.get('has_video_stream', False),
                'has_audio_stream': ffprobe_result.get('has_audio_stream', False),
                'duration': ffprobe_result.get('duration')
            }
        
        # 7. Если ffprobe не доступен или не поддерживает формат, делаем базовые проверки
        extension = file_path.suffix.lower()
        
        # Для MP3 файлов проверяем заголовок
        if extension == '.mp3':
            if not header.startswith(b'ID3') and header[:2] != b'\xFF\xFB':
                # Некоторые MP3 могут не иметь стандартного заголовка, но это не обязательно ошибка
                logger.warning(f"MP3 файл не имеет стандартного заголовка: {file_path}")
                # Но всё равно считаем валидным, так как некоторые MP3 могут быть без ID3 тегов
        
        # Для MP4 файлов проверяем наличие 'ftyp' атома
        elif extension == '.mp4' or extension == '.m4a':
            if b'ftyp' not in header:
                return {
                    'valid': False,
                    'reason': 'MP4 файл не содержит ftyp атом',
                    'file_size': file_size,
                    'has_video_stream': False,
                    'has_audio_stream': False,
                    'duration': None
                }
        
        # 8. Если дошли сюда, файл считается валидным
        return {
            'valid': True,
            'reason': None,
            'file_size': file_size,
            'has_video_stream': True,  # Предполагаем что есть видео
            'has_audio_stream': True,  # Предполагаем что есть аудио
            'duration': None  # Не знаем длительность без ffprobe
        }
        
    except Exception as e:
        logger.error(f"Ошибка при проверке целостности файла {file_path}: {e}")
        return {
            'valid': False,
            'reason': f'Ошибка проверки: {str(e)}',
            'file_size': 0,
            'has_video_stream': False,
            'has_audio_stream': False,
            'duration': None
        }


async def _check_with_ffprobe(file_path: Path) -> Dict[str, Any]:
    """
    Проверяет файл через ffprobe
    """
    try:
        # Проверяем доступность ffprobe
        result = await asyncio.create_subprocess_exec(
            'ffprobe', '-version',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        await result.communicate()
        
        if result.returncode != 0:
            logger.debug("ffprobe не доступен")
            return {'valid': True}  # Если ffprobe нет, считаем файл валидным
        
        # Запускаем ffprobe для проверки файла
        process = await asyncio.create_subprocess_exec(
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration,size',
            '-show_entries', 'stream=codec_type',
            '-of', 'json',
            str(file_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode('utf-8', errors='ignore').strip()
            
            # Игнорируем некоторые не критичные ошибки
            if "moov atom not found" in error_msg:
                logger.warning(f"ffprobe предупреждение для {file_path}: moov atom not found")
                # Всё равно считаем валидным
                return {'valid': True, 'has_video_stream': True, 'has_audio_stream': True}
            
            logger.error(f"ffprobe ошибка для {file_path}: {error_msg}")
            return {'valid': False}
        
        # Парсим JSON вывод ffprobe
        try:
            probe_data = json.loads(stdout.decode('utf-8', errors='ignore'))
            
            has_video_stream = False
            has_audio_stream = False
            duration = None
            
            # Проверяем потоки
            if 'streams' in probe_data:
                for stream in probe_data['streams']:
                    if stream.get('codec_type') == 'video':
                        has_video_stream = True
                    elif stream.get('codec_type') == 'audio':
                        has_audio_stream = True
            
            # Получаем длительность
            if 'format' in probe_data and 'duration' in probe_data['format']:
                try:
                    duration = float(probe_data['format']['duration'])
                except (ValueError, TypeError):
                    pass
            
            # Для аудиофайлов достаточно только аудио потока
            if file_path.suffix.lower() in ['.mp3', '.m4a', '.aac', '.flac', '.wav']:
                if has_audio_stream:
                    return {
                        'valid': True,
                        'has_video_stream': has_video_stream,
                        'has_audio_stream': has_audio_stream,
                        'duration': duration
                    }
                else:
                    return {'valid': False}
            
            # Для видеофайлов должен быть хотя бы один поток (видео или аудио)
            if has_video_stream or has_audio_stream:
                return {
                    'valid': True,
                    'has_video_stream': has_video_stream,
                    'has_audio_stream': has_audio_stream,
                    'duration': duration
                }
            else:
                return {'valid': False}
                
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON от ffprobe: {e}")
            return {'valid': True}  # На всякий случай считаем валидным
            
    except Exception as e:
        logger.error(f"Ошибка при вызове ffprobe: {e}")
        return {'valid': True}  # Если ошибка, считаем файл валидным

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
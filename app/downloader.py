"""
Упрощённый и надёжный загрузчик
Отвечает ТОЛЬКО за загрузку и свои временные файлы
"""
import asyncio
import os
import yt_dlp
import time
import shutil
import subprocess
from typing import Dict, Any, Optional
from pathlib import Path

from app.config import settings
from app.utils import get_download_config_for_url, normalize_title
from app import logger

class DownloadError(Exception):
    pass

class VideoDownloader:
    """Загрузчик видео - отвечает только за загрузку"""
    
    def __init__(self):
        self.videos_path = Path(settings.storage.videos_path)
        self.temp_path = Path(settings.storage.temp_path)
        
        # Создаём директории
        self.videos_path.mkdir(parents=True, exist_ok=True)
        self.temp_path.mkdir(parents=True, exist_ok=True)
    
    async def download(self, url: str, video_hash: str) -> Dict[str, Any]:
        """
        Загружает одно видео
        Отвечает ТОЛЬКО за загрузку и свои временные файлы
        """
        # ОЧИСТКА: Удаляем ВСЕ временные файлы этого видео перед началом
        await self._cleanup_all_temp_files(video_hash)
        
        loop = asyncio.get_event_loop()
        
        try:
            # Получаем конфигурацию
            format_spec, extract_audio = get_download_config_for_url(url)
            
            # Создаём опции
            ydl_opts = self._build_ydl_opts(format_spec, extract_audio, video_hash)
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Получаем информацию
                info = await loop.run_in_executor(None, ydl.extract_info, url, False)
                
                if not info:
                    raise DownloadError("Не удалось получить информацию о видео")
                
                # Определяем расширение
                ext = 'mp3' if extract_audio else info.get('ext', 'mp4')
                
                # Загружаем
                logger.info(f"Загрузка {video_hash[:12]}...")
                await loop.run_in_executor(None, ydl.download, [url])
                
                # Даём время на завершение
                await asyncio.sleep(2)
                
                # Ищем загруженный файл
                downloaded_file = await self._find_downloaded_file(video_hash, ext)
                if not downloaded_file:
                    raise DownloadError("Файл не найден после загрузки")
                
                # Проверяем файл
                await self._validate_file(downloaded_file, extract_audio)
                
                # Путь к финальному файлу
                final_path = self.videos_path / f"{video_hash}.{ext}"
                
                # Удаляем старый финальный файл если есть
                final_path.unlink(missing_ok=True)
                
                # Перемещаем
                shutil.move(str(downloaded_file), str(final_path))
                
                if not final_path.exists():
                    raise DownloadError("Файл не перемещён в финальную папку")
                
                # Проверяем после перемещения
                if not await self._quick_check_file(final_path):
                    raise DownloadError("Файл повреждён после перемещения")
                
                # Возвращаем результат
                return {
                    'file_path': str(final_path),
                    'title': normalize_title(info.get('title', '')),
                    'duration': info.get('duration'),
                    'uploader': info.get('uploader'),
                    'file_size': final_path.stat().st_size,
                    'file_ext': ext,
                    'download_success': True
                }
                
        except yt_dlp.utils.DownloadError as e:
            # Всегда очищаем временные файлы при ошибке
            await self._cleanup_all_temp_files(video_hash)
            
            error_msg = str(e).lower()
            
            # Обработка 403 Forbidden
            if "http error 403" in error_msg or "forbidden" in error_msg:
                raise DownloadError(f"403 Forbidden: Доступ к видео запрещён. Возможно, видео приватное или заблокировано.")
            # Обработка 416 Range Not Satisfiable
            elif "http error 416" in error_msg or "range not satisfiable" in error_msg:
                raise DownloadError(f"HTTP 416: Ошибка диапазона - {e}")
            # Обработка 404 Not Found
            elif "http error 404" in error_msg or "not found" in error_msg:
                raise DownloadError(f"404 Not Found: Видео не найдено или было удалено.")
            # Обработка 429 Too Many Requests
            elif "http error 429" in error_msg or "too many requests" in error_msg:
                raise DownloadError(f"429 Too Many Requests: Слишком много запросов. Попробуйте позже.")
            # Обработка недоступного контента
            elif "unavailable" in error_msg:
                raise DownloadError(f"Видео недоступно: {e}")
            # Остальные ошибки
            else:
                raise DownloadError(f"Ошибка загрузки: {e}")

        except Exception as e:
            # Всегда очищаем временные файлы при ошибке
            await self._cleanup_all_temp_files(video_hash)
            raise DownloadError(f"Ошибка загрузки: {e}")
    
    async def _cleanup_all_temp_files(self, video_hash: str):
        """
        Очищает ВСЕ временные файлы для этого видео
        Вызывается ПЕРЕД началом загрузки и ПРИ ошибках
        """
        try:
            for file in self.temp_path.glob(f"*{video_hash}*"):
                try:
                    if file.exists():
                        file.unlink()
                        logger.debug(f"Очистка: удалён {file.name}")
                except Exception as e:
                    logger.debug(f"Не удалось удалить {file}: {e}")
        except Exception as e:
            logger.warning(f"Ошибка очистки временных файлов {video_hash}: {e}")
    
    def _build_ydl_opts(self, format_spec: str, extract_audio: bool, video_hash: str) -> Dict[str, Any]:
        """Создаёт опции для yt-dlp"""
        # Временный файл
        temp_filename = f"{video_hash}_temp.%(ext)s"
        output_template = str(self.temp_path / temp_filename)
        
        opts = {
            'format': format_spec,
            'outtmpl': output_template,
            'continuedl': True,
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'retries': 10,
            'fragment_retries': 10,
            'socket_timeout': 30,
            'concurrent_fragment_downloads': 2,
            'skip_unavailable_fragments': True,
            'fixup': 'detect_or_warn',
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            },
        }
        
        if extract_audio:
            opts.update({
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                }],
            })
        else:
            opts['merge_output_format'] = 'mp4'
        
        return opts
    
    async def _find_downloaded_file(self, video_hash: str, expected_ext: str) -> Optional[Path]:
        """Ищет загруженный файл"""
        # Ищем по шаблону
        for file in self.temp_path.glob(f"{video_hash}_temp*.{expected_ext}"):
            if file.exists() and file.stat().st_size > 0 and not file.name.endswith('.part'):
                return file
        
        # Ищем любой файл с этим хешем
        for file in self.temp_path.glob(f"*{video_hash}*"):
            if (file.exists() and file.stat().st_size > 0 and 
                not file.name.endswith('.part') and 
                file.suffix.lstrip('.') == expected_ext):
                return file
        
        return None
    
    async def _validate_file(self, file_path: Path, is_audio: bool):
        """Проверяет загруженный файл"""
        # Проверка размера
        if file_path.stat().st_size < 1024 * 10:  # Минимум 10KB
            raise DownloadError("Файл слишком мал")
        
        # Быстрая проверка через file команду если доступна
        if await self._has_file_command():
            if not await self._check_with_file(file_path, is_audio):
                raise DownloadError("Файл имеет неверный формат")
    
    async def _quick_check_file(self, file_path: Path) -> bool:
        """Быстрая проверка файла"""
        try:
            # Проверяем существование и размер
            if not file_path.exists():
                return False
            
            if file_path.stat().st_size == 0:
                return False
            
            # Пробуем прочитать начало и конец
            with open(file_path, 'rb') as f:
                # Начало
                f.seek(0)
                header = f.read(100)
                if len(header) == 0:
                    return False
                
                # Конец (если файл не слишком большой)
                if file_path.stat().st_size < 10 * 1024 * 1024:  # 10MB
                    f.seek(-100, 2)
                    footer = f.read(100)
                    if len(footer) == 0:
                        return False
            
            return True
            
        except Exception:
            return False
    
    async def _has_file_command(self) -> bool:
        """Проверяет наличие команды file"""
        try:
            result = await asyncio.create_subprocess_exec(
                'file', '--version',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            await result.communicate()
            return result.returncode == 0
        except:
            return False
    
    async def _check_with_file(self, file_path: Path, is_audio: bool) -> bool:
        """Проверяет файл с помощью команды file"""
        try:
            result = await asyncio.create_subprocess_exec(
                'file', '-b', '--mime-type', str(file_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            stdout, _ = await result.communicate()
            mime_type = stdout.decode().strip().lower()
            
            if is_audio:
                return 'audio' in mime_type or 'mpeg' in mime_type
            else:
                return 'video' in mime_type or 'mp4' in mime_type
                
        except Exception:
            return True  # Если команда не работает, считаем файл валидным
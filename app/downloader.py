"""
Оптимизированный загрузчик видео с yt-dlp
"""
import asyncio
import os
import yt_dlp
from typing import Dict, Any
from pathlib import Path
import logging
from app.config import settings
from app.utils import get_download_config_for_url, normalize_title

logger = logging.getLogger(__name__)

class VideoDownloader:
    """Оптимизированный загрузчик видео"""
    
    # Кэш экземпляров yt-dlp для разных доменов
    _ydl_instances = {}
    
    def __init__(self):
        self.videos_path = Path(settings.storage.videos_path)
        self.temp_path = Path(settings.storage.temp_path)
        
    async def download(self, url: str, video_hash: str) -> Dict[str, Any]:
        """
        Асинхронно загружает видео
        
        Args:
            url: URL видео
            video_hash: 64-символьный хеш
            
        Returns:
            Словарь с информацией о загруженном видео
        """
        loop = asyncio.get_event_loop()
        
        try:
            # Получаем конфигурацию загрузки
            format_spec, extract_audio = get_download_config_for_url(url)
            
            # Создаем опции для yt-dlp
            ydl_opts = self._build_ydl_opts(format_spec, extract_audio, video_hash)
            
            # Используем ленивую загрузку yt-dlp
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Извлекаем информацию без загрузки
                info = await loop.run_in_executor(
                    None, 
                    ydl.extract_info, 
                    url, 
                    False
                )
                
                # Получаем расширение файла
                ext = info.get('ext', 'mp4')
                if extract_audio:
                    ext = 'mp3'
                
                # Путь для сохранения файла
                output_path = self.videos_path / f"{video_hash}.{ext}"
                
                # Скачиваем видео
                await loop.run_in_executor(None, ydl.download, [url])
                
                # Проверяем, что файл создан
                if not output_path.exists():
                    # Пробуем найти файл с другим расширением
                    for file in self.videos_path.glob(f"{video_hash}.*"):
                        output_path = file
                        ext = file.suffix[1:]  # Без точки
                        break
                
                # Получаем размер файла
                file_size = output_path.stat().st_size if output_path.exists() else None
                
                # Нормализуем название
                title = normalize_title(info.get('title', ''))
                
                return {
                    'file_path': str(output_path),
                    'title': title,
                    'duration': info.get('duration'),
                    'uploader': info.get('uploader'),
                    'file_size': file_size,
                    'file_ext': ext
                }
                
        except Exception as e:
            logger.error(f"Ошибка загрузки {url}: {e}")
            raise
    
    def _build_ydl_opts(
        self, 
        format_spec: str, 
        extract_audio: bool,
        video_hash: str
    ) -> Dict[str, Any]:
        """
        Создает оптимизированные опции для yt-dlp
        """
        # Шаблон для имени файла (только хеш)
        output_template = str(self.videos_path / f"{video_hash}.%(ext)s")
        
        opts = {
            'format': format_spec,
            'outtmpl': output_template,
            'quiet': True,
            'no_warnings': True,
            'no_color': True,
            'ignoreerrors': False,
            'consoletitle': False,
            'progress_hooks': [],  # Можно добавить хуки для прогресса
            'continuedl': True,
            'noprogress': True,
            'socket_timeout': 30,
            'retries': 3,
        }
        
        if extract_audio:
            opts.update({
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
            })
        else:
            # Оптимизации для видео
            opts.update({
                'merge_output_format': 'mp4',
                'prefer_ffmpeg': True,
            })
        
        return opts
    
    async def cleanup_temp_files(self):
        """Очищает временные файлы"""
        try:
            for temp_file in self.temp_path.glob("*"):
                if temp_file.is_file():
                    temp_file.unlink()
        except Exception as e:
            logger.warning(f"Ошибка очистки временных файлов: {e}")
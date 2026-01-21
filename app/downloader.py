"""
Оптимизированный загрузчик видео с yt-dlp
"""
import asyncio
import os
import yt_dlp
import time
import shutil
from typing import Dict, Any, Optional
from pathlib import Path
import logging
import subprocess
from app.config import settings
from app.utils import get_download_config_for_url, normalize_title
from app import logger

class VideoDownloader:
    """Оптимизированный загрузчик видео"""
    
    _download_progress: Dict[str, Dict[str, Any]] = {}
    
    def __init__(self):
        self.videos_path = Path(settings.storage.videos_path)
        self.temp_path = Path(settings.storage.temp_path)
        
        # Создаем директории если их нет
        self.videos_path.mkdir(parents=True, exist_ok=True)
        self.temp_path.mkdir(parents=True, exist_ok=True)
    
    async def download(self, url: str, video_hash: str) -> Dict[str, Any]:
        """
        Асинхронно загружает видео
        """
        loop = asyncio.get_event_loop()
        temp_file_path = None
        
        try:
            # Получаем конфигурацию загрузки
            format_spec, extract_audio = get_download_config_for_url(url)
            
            # Создаем опции для yt-dlp
            ydl_opts = self._build_ydl_opts(format_spec, extract_audio, video_hash, temp=True)
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Извлекаем информацию
                info = await loop.run_in_executor(None, ydl.extract_info, url, False)
                
                # Определяем расширение
                ext = 'mp3' if extract_audio else info.get('ext', 'mp4')
                
                # Пути файлов
                temp_filename = f"{video_hash}_temp.{ext}"
                temp_file_path = self.temp_path / temp_filename
                final_file_path = self.videos_path / f"{video_hash}.{ext}"
                
                # Скачиваем
                logger.info(f"Загрузка {video_hash[:12]}...")
                await loop.run_in_executor(None, ydl.download, [url])
                
                # ⚠️ ЖДЕМ завершения операций yt-dlp
                await asyncio.sleep(2)
                
                # Ищем фактический файл (может быть с другим именем)
                actual_temp_file = None
                for file in self.temp_path.glob(f"{video_hash}_temp*"):
                    if file.suffix != '.part':  # Игнорируем .part файлы
                        actual_temp_file = file
                        break
                
                if not actual_temp_file:
                    raise FileNotFoundError(f"Файл не найден в temp для {video_hash}")
                
                # Проверяем файл
                file_size = actual_temp_file.stat().st_size
                if file_size == 0:
                    raise ValueError("Файл имеет нулевой размер")
                
                # Базовые проверки целостности (опционально)
                if not extract_audio and await self._has_ffprobe():
                    if not await self._verify_with_ffprobe(actual_temp_file):
                        raise ValueError("Файл поврежден")
                
                # Переносим в финальную папку
                final_file_path.unlink(missing_ok=True)  # Тихий unlink
                shutil.move(str(actual_temp_file), str(final_file_path))
                
                if not final_file_path.exists():
                    raise FileNotFoundError(f"Файл не перемещен")
                
                # Возвращаем информацию
                return {
                    'file_path': str(final_file_path),
                    'title': normalize_title(info.get('title', '')),
                    'duration': info.get('duration'),
                    'uploader': info.get('uploader'),
                    'file_size': final_file_path.stat().st_size,
                    'file_ext': ext,
                    'original_url': url,
                    'hash': video_hash,
                    'download_success': True
                }
                
        except Exception as e:
            logger.error(f"Ошибка загрузки {url}: {e}")
            raise  # Просто пробрасываем исключение
        
        finally:
            # ТОЛЬКО ОДНА очистка в конце
            await self._safe_cleanup_after_download(video_hash)
    
    def _build_ydl_opts(
        self, 
        format_spec: str, 
        extract_audio: bool,
        video_hash: str,
        temp: bool = False
    ) -> Dict[str, Any]:
        """
        Создает опции для yt-dlp БЕЗ конвертации
        
        Исправления:
        1. Убраны несуществующие опции (prefer_ffmpeg)
        2. Исправлен merge_output_format
        3. Добавлена загрузка в temp
        """
        # Определяем путь для сохранения
        if temp:
            # Во временную папку с суффиксом _temp
            base_path = self.temp_path
            filename = f"{video_hash}_temp.%(ext)s"
        else:
            # В финальную папку
            base_path = self.videos_path
            filename = f"{video_hash}.%(ext)s"
        
        output_template = str(base_path / filename)
        
        opts = {
            'format': format_spec,
            'outtmpl': output_template,
            
            # Критически важные опции для стабильности:
            'continuedl': True,                # Продолжать прерванные загрузки
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'nooverwrites': False,
            
            # Хук прогресса
            'progress_hooks': [self._progress_hook],
            
            # Оптимизации для стабильности
            'retries': 10,
            'fragment_retries': 10,
            'extractor_retries': 3,
            'socket_timeout': 30,
            
            # Проверка форматов
            'check_formats': 'selected',       # Проверять только выбранные форматы
            
            # Исправление ошибок
            'fixup': 'detect_or_warn',         # Автоматически исправлять известные ошибки
            
            # Для сегментированных форматов (DASH/HLS)
            'concurrent_fragment_downloads': 1,  # 1 поток для стабильности
            'skip_unavailable_fragments': True,  # Пропускать недоступные фрагменты

            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            },
        }
        
        # Для аудио
        if extract_audio:
            opts.update({
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                }],
                'postprocessor_args': [
                    '-loglevel', 'panic'
                ],
                # Проверка аудио
                'check_formats': True,
            })
        else:
            # Для видео: настраиваем мердж форматов
            # yt-dlp автоматически определит лучший контейнер
            opts.update({
                'merge_output_format': None,  # Автовыбор контейнера
                'format': format_spec,
            })
        
        return opts
    
    async def _verify_with_ffprobe(self, file_path: Path) -> bool:
        """Проверяет видеофайл через ffprobe"""
        try:
            result = await asyncio.create_subprocess_exec(
                'ffprobe',
                '-v', 'error',
                '-show_format',
                '-show_streams',
                str(file_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            stdout, stderr = await result.communicate()
            
            if result.returncode != 0:
                logger.error(f"ffprobe ошибка: {stderr.decode()}")
                return False
            
            # Проверяем что есть видео и/или аудио потоки
            output = stdout.decode()
            if 'codec_type=video' not in output and 'codec_type=audio' not in output:
                logger.warning("Файл не содержит видео или аудио потоков")
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Не удалось проверить через ffprobe: {e}")
            # Если ffprobe нет, возвращаем True чтобы не блокировать загрузку
            return True
    
    async def _verify_audio_file(self, file_path: Path) -> bool:
        """Проверяет аудиофайл"""
        try:
            # Базовая проверка размера
            if file_path.stat().st_size < 1024:  # Минимум 1KB
                return False
            
            # Для mp3 проверяем заголовок
            if file_path.suffix.lower() == '.mp3':
                with open(file_path, 'rb') as f:
                    header = f.read(3)
                    if header != b'ID3' and header != b'\xFF\xFB':
                        logger.warning("MP3 файл имеет неверный заголовок")
                        # Не обязательно ошибка, некоторые mp3 без ID3 тегов
                        # return False
            
            return True
        except Exception as e:
            logger.error(f"Ошибка проверки аудио: {e}")
            return False
    
    async def _has_ffprobe(self) -> bool:
        """Проверяет наличие ffprobe в системе"""
        try:
            result = await asyncio.create_subprocess_exec(
                'ffprobe', '-version',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            await result.communicate()
            return result.returncode == 0
        except:
            return False
    
    async def _safe_cleanup_after_download(self, video_hash: str):
        """
        Безопасная очистка ПОСЛЕ завершения загрузки
        Удаляет только старые/завершенные файлы
        """
        try:
            import time
            current_time = time.time()
            
            # Ждем немного на всякий случай
            await asyncio.sleep(1)
            
            for temp_file in self.temp_path.glob(f"{video_hash}_temp*"):
                try:
                    if not temp_file.exists():
                        continue
                    
                    # НИКОГДА не удаляем .part файлы - yt-dlp сам управляет ими
                    if '.part' in temp_file.name:
                        # Удаляем только ОЧЕНЬ старые .part файлы (> 30 минут)
                        file_age = current_time - temp_file.stat().st_mtime
                        if file_age > 1800:  # 30 минут
                            temp_file.unlink()
                            logger.debug(f"Удалил старый .part файл: {temp_file.name}")
                        continue
                    
                    # Для обычных файлов - удаляем если они старше 5 минут
                    file_age = current_time - temp_file.stat().st_mtime
                    if file_age > 300:  # 5 минут
                        temp_file.unlink()
                        logger.debug(f"Удалил старый временный файл: {temp_file.name}")
                        
                except Exception as e:
                    logger.debug(f"Не удалось удалить {temp_file}: {e}")
                    
        except Exception as e:
            logger.debug(f"Ошибка безопасной очистки: {e}")
    
    def _progress_hook(self, d: Dict[str, Any]):
        """
        Хук для отслеживания прогресса загрузки yt-dlp
        """
        try:
            status = d.get('status', '')
            
            # Извлекаем video_hash из имени файла
            filename = d.get('filename', '')
            if filename:
                # Ищем хеш в имени файла (формат: /path/to/hash_temp.ext)
                import re
                match = re.search(r'([a-f0-9]{64})_temp', filename)
                if match:
                    video_hash = match.group(1)
                else:
                    # Или берем из info_dict
                    info_dict = d.get('info_dict', {})
                    video_hash = info_dict.get('_video_hash', '')
            else:
                video_hash = ''
            
            if status == 'downloading':
                # Прогресс загрузки
                downloaded = d.get('downloaded_bytes', 0)
                total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                speed = d.get('speed', 0)
                
                if total and total > 0:
                    percent = (downloaded / total) * 100
                    
                    # Логируем каждые 10% или если скорость изменилась значительно
                    if video_hash:
                        last_progress = self._download_progress.get(video_hash, {})
                        last_percent = last_progress.get('percent', 0)
                        
                        if percent - last_percent >= 10 or percent == 100:
                            # Форматируем размеры
                            downloaded_mb = downloaded / (1024 * 1024)
                            total_mb = total / (1024 * 1024)
                            speed_mb = speed / (1024 * 1024) if speed else 0
                            
                            location = "temp" if "_temp" in filename else "final"
                            
                            logger.info(
                                f"📥 Загрузка {video_hash[:12]}... ({location}): "
                                f"{percent:.1f}% ({downloaded_mb:.1f}/{total_mb:.1f} MB) "
                                f"@ {speed_mb:.1f} MB/s"
                            )
                            
                            # Сохраняем последний прогресс
                            self._download_progress[video_hash] = {
                                'percent': percent,
                                'downloaded': downloaded,
                                'total': total,
                                'speed': speed,
                                'timestamp': time.time(),
                                'location': location
                            }
                
                elif downloaded > 0:
                    # Если неизвестен общий размер, логируем по объему
                    downloaded_mb = downloaded / (1024 * 1024)
                    speed_mb = speed / (1024 * 1024) if speed else 0
                    location = "temp" if "_temp" in filename else "final"
                    
                    if video_hash:
                        logger.debug(
                            f"📥 Загрузка {video_hash[:12]}... ({location}): "
                            f"{downloaded_mb:.1f} MB @ {speed_mb:.1f} MB/s"
                        )
            
            elif status == 'finished':
                # Загрузка завершена
                if video_hash:
                    location = "temp" if "_temp" in filename else "final"
                    logger.info(f"✅ Загрузка завершена: {video_hash[:12]}... ({location})")
                    # Удаляем из словаря прогресса
                    self._download_progress.pop(video_hash, None)
            
            elif status == 'error':
                # Ошибка загрузки
                error_msg = d.get('error', 'Unknown error')
                if video_hash:
                    logger.error(f"❌ Ошибка загрузки {video_hash[:12]}...: {error_msg}")
            
        except Exception as e:
            logger.debug(f"Ошибка в progress hook: {e}")

    async def retry_download(self, url: str, video_hash: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Пытается загрузить видео с повторами в случае ошибок
        
        Args:
            url: URL видео
            video_hash: Хеш видео
            max_retries: Максимальное количество попыток
            
        Returns:
            Информация о видео или None при неудаче
        """
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Попытка загрузки {attempt}/{max_retries} для {video_hash[:12]}...")
                
                if attempt > 1:
                    # Увеличиваем время между попытками
                    await asyncio.sleep(attempt * 2)
                
                result = await self.download(url, video_hash)
                return result
                
            except Exception as e:
                last_error = e
                logger.warning(f"Попытка {attempt} не удалась для {video_hash[:12]}...: {e}")
                
                # Очищаем временные файлы перед следующей попыткой
                self._cleanup_temp_files(video_hash)
                
                # Удаляем возможные частично скачанные файлы
                for file in self.videos_path.glob(f"{video_hash}.*"):
                    try:
                        file.unlink()
                        logger.info(f"Удалил частичный файл: {file.name}")
                    except:
                        pass
        
        logger.error(f"Все {max_retries} попыток загрузки провалились для {video_hash[:12]}...")
        return None
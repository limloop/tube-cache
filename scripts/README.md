# YouTube Streaming Tools

<details>
<summary><i>🇷🇺 Русский</i></summary>

Набор скриптов для загрузки и воспроизведения YouTube-видео через сервер.

## Требования

- `yt-dlp` - для получения информации о видео с YouTube
- `curl` - для API-запросов
- `mpv` - для воспроизведения видео
- Бэкенд-сервер, запущенный по адресу `http://localhost:8000` (можно изменить)

### Установка

```bash
# Установка зависимостей (Ubuntu/Debian)
sudo apt install mpv curl
pip install yt-dlp

# Или на macOS
brew install mpv curl yt-dlp

# Сделать скрипты исполняемыми
chmod +x mpv.sh youtube-subscriber.sh
```

---

## Скрипт 1: `mpv.sh`

### Описание

Скрипт запрашивает загрузку видео у сервера и воспроизводит его через mpv после готовности. Ожидает завершения загрузки и автоматически запускает воспроизведение.

### Возможности

- Запрос загрузки видео у сервера
- Ожидание завершения загрузки/обработки
- Автоматический запуск воспроизведения
- Отображение прогресса загрузки
- Защита от зависания (таймаут)
- Передача параметров в mpv

### Использование

```bash
./mpv.sh <youtube-url> [параметры-mpv...]
```

### Примеры

**Обычное использование:**
```bash
./mpv.sh "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
```

**Только аудио:**
```bash
./mpv.sh "https://youtu.be/dQw4w9WgXcQ" --no-video
```

**С пользовательскими параметрами mpv:**
```bash
./mpv.sh "https://youtu.be/dQw4w9WgXcQ" --volume=50 --no-osc
```

### Настройка

Отредактируйте скрипт для изменения параметров:

```bash
SERVER="http://localhost:8000"     # URL бэкенд-сервера
CHECK_INTERVAL=3                   # Интервал проверки статуса (секунды)
TIMEOUT=3600                       # Максимальное время ожидания (секунды)
```

### Как это работает

1. Отправляет запрос на `$SERVER/video?url=...`
2. Получает хеш и статус видео
3. Если видео готово - сразу воспроизводит
4. Если загружается - опрашивает сервер каждые 3 секунды
5. Когда статус становится "ready" - запускает mpv с URL потока

### Коды возврата

- `0` - Успех (видео воспроизведено)
- `1` - Ошибка (сервер, таймаут или ошибка загрузки)

---

## Скрипт 2: `youtube-subscriber.sh`

### Описание

Пакетный скрипт, который получает последние видео с указанных YouTube-каналов и отправляет запросы на загрузку серверу без ожидания ответа.

### Возможности

- Мониторинг нескольких YouTube-каналов
- Получение последних N видео с каждого канала
- Асинхронная отправка запросов (отправил и забыл)
- Настраиваемое количество видео на канал
- Отображение прогресса в реальном времени
- Без ожидания ответов

### Использование

```bash
./youtube-subscriber.sh
```

### Настройка

Отредактируйте скрипт для настройки:

```bash
# Адрес сервера
SERVER="http://localhost:8000"

# Количество последних видео для получения с канала
MAX_VIDEOS=3

# Ваши каналы (добавляйте сколько нужно)
CHANNELS=(
    "https://www.youtube.com/@НазваниеКанала"
    "https://www.youtube.com/c/ДругойКанал"
    "https://www.youtube.com/@ЕщеОдинКанал"
)
```

### Примеры

**Редактирование каналов в скрипте:**
```bash
CHANNELS=(
    "https://www.youtube.com/@LinusTechTips"
    "https://www.youtube.com/@MKBHD"
    "https://www.youtube.com/@MrBeast"
)
```

**Изменение количества видео:**
```bash
MAX_VIDEOS=5   # Получить 5 последних видео с канала
```

**Смена сервера:**
```bash
SERVER="http://192.168.1.100:8000"
```

### Пример вывода

```
=== Sending videos to server ===
Server: http://localhost:8000
Max videos per channel: 3

Processing channel: https://www.youtube.com/@ExampleChannel
  1. Удивительное видео
    ✓ Request sent: Удивительное видео
  2. Еще одно отличное видео
    ✓ Request sent: Еще одно отличное видео
  3. Последний ролик
    ✓ Request sent: Последний ролик
  Sent: 3 videos

=== Total requests sent: 3 ===
Done!
```

### Как это работает

1. Для каждого канала из списка получает последние N видео через yt-dlp
2. Извлекает названия и URL видео
3. Отправляет асинхронные curl-запросы на `$SERVER/video?url=...` для каждого видео
4. НЕ ожидает ответов (фоновые процессы)
5. Сразу переходит к следующему видео/каналу

### Сценарии использования

- **Пакетная загрузка**: Предзагрузка видео из подписок
- **Офлайн-просмотр**: Запрос загрузки для просмотра позже
- **Мониторинг каналов**: Автоматическое получение нового контента
- **Тестирование сервера**: Отправка множества одновременных запросов

---

## Пример рабочего процесса

1. **Запустите бэкенд-сервер:**
```bash
./server.sh  # Ваш бэкенд-сервер
```

2. **Подпишитесь на каналы (запросите загрузку):**
```bash
./youtube-subscriber.sh
```
Это отправит запросы на загрузку всех последних видео.

3. **Посмотрите видео позже:**
```bash
./mpv.sh "https://youtube.com/watch?v=..."
```
Скрипт подождет, если видео еще загружается, или воспроизведет сразу.

---

## Продвинутое использование

### Запуск подписчика по расписанию (cron)

Добавьте в crontab для автоматической проверки каналов:

```bash
# Проверять каналы каждые 6 часов
0 */6 * * * /path/to/youtube-subscriber.sh

# Проверять ежедневно в 8 утра
0 8 * * * /path/to/youtube-subscriber.sh >> /var/log/youtube.log 2>&1
```

---

## Решение проблем

### Частые проблемы

**"yt-dlp: command not found"**
```bash
pip install yt-dlp
# или
pip3 install yt-dlp
```

**"mpv: command not found"**
```bash
# Ubuntu/Debian
sudo apt install mpv

# Fedora
sudo dnf install mpv

# macOS
brew install mpv
```

**"Server did not respond" / "Сервер не ответил"**
- Проверьте, запущен ли сервер: `curl http://localhost:8000`
- Убедитесь в правильности переменной SERVER в скриптах
- Проверьте настройки файрвола

**"No videos found from channel" / "Не найдено видео на канале"**
- Убедитесь, что URL канала правильный
- Проверьте, открыт ли канал (публичный)
- Проверьте вручную: `yt-dlp --flat-playlist --playlist-end 1 "URL_КАНАЛА"`

### Режим отладки

Для подробного вывода добавьте `-x` в shebang или запустите с отладкой bash:

```bash
bash -x youtube-subscriber.sh
```

---

## Примечания

- Все запросы отправляются асинхронно (отправил и забыл)
- Скрипты не проверяют доступность видео - это делает сервер
- mpv.sh ожидает завершения загрузки до TIMEOUT секунд
- Между запусками нет постоянного хранилища - каждый запуск получает свежие данные
- Соблюдайте условия использования YouTube и robots.txt

</details>

<details open>
<summary>🇬🇧 English</summary>

A set of scripts for downloading and streaming YouTube videos through a backend server.

## Prerequisites

- `yt-dlp` - for fetching YouTube video information
- `curl` - for API requests
- `mpv` - for video playback
- Backend server running at `http://localhost:8000` (or custom address)

### Installation

```bash
# Install dependencies (Ubuntu/Debian)
sudo apt install mpv curl
pip install yt-dlp

# Or on macOS
brew install mpv curl yt-dlp

# Make scripts executable
chmod +x mpv.sh youtube-subscriber.sh
```

---

## Script 1: `mpv.sh`

### Description

A script that requests video download from the server and plays it with mpv once ready. It handles waiting for downloads and automatic playback.

### Features

- Requests video download from the server
- Waits for download/processing to complete
- Automatically starts playback when ready
- Shows download progress
- Supports timeout protection
- Passes custom parameters to mpv

### Usage

```bash
./mpv.sh <youtube-url> [mpv-parameters...]
```

### Examples

**Basic usage:**
```bash
./mpv.sh "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
```

**Play audio only:**
```bash
./mpv.sh "https://youtu.be/dQw4w9WgXcQ" --no-video
```

**With custom mpv options:**
```bash
./mpv.sh "https://youtu.be/dQw4w9WgXcQ" --volume=50 --no-osc
```

### Configuration

Edit the script to change these settings:

```bash
SERVER="http://localhost:8000"     # Backend server URL
CHECK_INTERVAL=3                   # Status check interval (seconds)
TIMEOUT=3600                       # Maximum wait time (seconds)
```

### How it works

1. Sends request to `$SERVER/video?url=...`
2. Receives video hash and status
3. If video is ready, plays immediately
4. If downloading, polls server every 3 seconds
5. When status becomes "ready", starts mpv with stream URL

### Exit codes

- `0` - Success (video played)
- `1` - Error (server error, timeout, or download failed)

---

## Script 2: `youtube-subscriber.sh`

### Description

A batch script that fetches latest videos from specified YouTube channels and sends download requests to the server without waiting for responses.

### Features

- Monitors multiple YouTube channels
- Gets latest N videos from each channel
- Sends asynchronous requests to server (fire & forget)
- Configurable number of videos per channel
- Shows real-time progress
- No waiting for responses

### Usage

```bash
./youtube-subscriber.sh
```

### Configuration

Edit the script to customize:

```bash
# Server address
SERVER="http://localhost:8000"

# Number of latest videos to fetch per channel
MAX_VIDEOS=3

# Your channels (add as many as you want)
CHANNELS=(
    "https://www.youtube.com/@ChannelName"
    "https://www.youtube.com/c/AnotherChannel"
    "https://www.youtube.com/@OneMoreChannel"
)
```

### Examples

**Edit channels directly in script:**
```bash
CHANNELS=(
    "https://www.youtube.com/@LinusTechTips"
    "https://www.youtube.com/@MKBHD"
    "https://www.youtube.com/@MrBeast"
)
```

**Change number of videos:**
```bash
MAX_VIDEOS=5   # Get 5 latest videos per channel
```

**Change server:**
```bash
SERVER="http://192.168.1.100:8000"
```

### Output example

```
=== Sending videos to server ===
Server: http://localhost:8000
Max videos per channel: 3

Processing channel: https://www.youtube.com/@ExampleChannel
  1. Amazing Video Title
    ✓ Request sent: Amazing Video Title
  2. Another Great Video
    ✓ Request sent: Another Great Video
  3. Latest Upload
    ✓ Request sent: Latest Upload
  Sent: 3 videos

=== Total requests sent: 3 ===
Done!
```

### How it works

1. For each channel in the list, fetches latest N videos using yt-dlp
2. Extracts video titles and URLs
3. Sends async curl requests to `$SERVER/video?url=...` for each video
4. Does NOT wait for responses (background processes)
5. Moves to next video/channel immediately

### Use cases

- **Batch downloading**: Preload videos from subscribed channels
- **Offline viewing**: Request downloads for later watching
- **Channel monitoring**: Automatically fetch new content
- **Server testing**: Send multiple concurrent requests

---

## Workflow Example

1. **Start the backend server:**
```bash
./server.sh  # Your backend server
```

2. **Subscribe to channels (request downloads):**
```bash
./youtube-subscriber.sh
```
This sends requests for all latest videos to the server.

3. **Watch a video later:**
```bash
./mpv.sh "https://youtube.com/watch?v=..."
```
The script will wait if downloading or play immediately if ready.

---

## Advanced Usage

### Running subscriber on schedule (cron)

Add to crontab for automatic channel checking:

```bash
# Check channels every 6 hours
0 */6 * * * /path/to/youtube-subscriber.sh

# Check daily at 8 AM
0 8 * * * /path/to/youtube-subscriber.sh >> /var/log/youtube.log 2>&1
```

---

## Troubleshooting

### Common Issues

**"yt-dlp: command not found"**
```bash
pip install yt-dlp
# or
pip3 install yt-dlp
```

**"mpv: command not found"**
```bash
# Ubuntu/Debian
sudo apt install mpv

# Fedora
sudo dnf install mpv

# macOS
brew install mpv
```

**"Server did not respond"**
- Check if server is running: `curl http://localhost:8000`
- Verify SERVER variable in scripts
- Check firewall settings

**No videos found from channel**
- Verify channel URL is correct
- Check if channel is public
- Test manually: `yt-dlp --flat-playlist --playlist-end 1 "CHANNEL_URL"`

### Debug mode

For verbose output, add `-x` to shebang or run with bash debug:

```bash
bash -x youtube-subscriber.sh
```

---

## Notes

- All requests are sent asynchronously (fire & forget)
- Scripts don't validate video availability - server handles that
- mpv.sh waits up to TIMEOUT seconds for download completion
- No persistent storage between runs - each run fetches fresh data
- Respect YouTube's terms of service and robots.txt


</details>
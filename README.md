# **Tube-Cache** 🎬

**Asynchronous media cache and streaming server built on FastAPI**

Tube-Cache is a self-hosted media cache server that downloads, stores, and streams video content from online sources supported by **yt-dlp**.
It provides a local HTTP API and web interface for accessing cached videos and their metadata.

The project focuses on **task orchestration, caching strategies, and asynchronous background processing**, rather than video extraction itself.

---

## **Key Features**

### 🚀 Asynchronous download queue

* Background task queue based on **asyncio**
* Persistent task state (survives restarts)
* Configurable parallel downloads
* Automatic retries on failure

### 💾 Local media cache

* File-based cache with **LRU eviction**
* Metadata storage in SQLite
* Integrity checks on access
* Original formats preserved (no transcoding)

### 🌐 Source-agnostic by design

* Uses **yt-dlp** to support 100+ video platforms
* URL normalization to reduce duplicates
* Not limited to YouTube-specific logic

### 📊 API & Web Interface

* **API-first architecture**
* Web UI built with Bootstrap 5
* Download and cache status monitoring
* Structured logging with rotation

### 🔧 Technical stack

```
Backend:
  FastAPI     – ASGI web server
  yt-dlp      – Video extraction and download
  asyncio     – Background tasks and queues
  SQLite      – Metadata storage
  FFmpeg     – Media probing / validation

Frontend:
  Bootstrap 5 – UI
  Jinja2     – HTML templates
  Vanilla JS – Client-side logic
```

---

## **How it works**

```
1. Client → Request video by URL
2. Tube-Cache → Check local cache
   ├─ Cache hit → Stream immediately
   └─ Cache miss → Add task to download queue
3. yt-dlp → Download video in background
4. Server → Store file + metadata
5. Video becomes available for local streaming
```

---

## **Use cases**

* Local media cache for frequently accessed online videos
* Offline viewing inside a home or LAN network
* Backend example of an async task queue without external brokers
* Media source for local players (mpv, VLC, etc.)

---

## **Project goals**

* Minimal external dependencies
* Predictable and transparent behavior
* Emphasis on backend architecture and reliability
* Simple deployment and configuration

---

<details>
<summary><i>🇷🇺 Русская версия / Russian version</i></summary>

## **Tube-Cache**

Асинхронный сервер для кэширования и стриминга видео, написанный на FastAPI.

Tube-Cache загружает, сохраняет и отдаёт видео с онлайн-источников, поддерживаемых **yt-dlp**, предоставляя локальный HTTP API и веб-интерфейс для доступа к видео и метаданным.

Проект **не реализует логику извлечения видео самостоятельно**, а использует `yt-dlp` как внешний инструмент. Основной фокус — это:

* очередь фоновых задач
* стратегия кеширования
* асинхронная обработка
* локальная отдача медиафайлов

---

### Возможности

**Очередь загрузок**

* Асинхронная очередь на `asyncio`
* Восстановление задач после перезапуска
* Ограничение параллелизма
* Повторы при ошибках

**Кэш**

* Локальное файловое хранилище
* Очистка по LRU
* SQLite для метаданных
* Проверка целостности файлов

**Источники**

* Поддержка всех сайтов, доступных через `yt-dlp`
* Нормализация URL
* Отсутствие жёсткой привязки к YouTube

**Интерфейс**

* HTTP API
* Web UI на Bootstrap 5
* Статусы загрузок и кеша
* Логирование с ротацией

---

### Назначение проекта

* Локальный кэш интернет-видео
* Оффлайн-доступ внутри локальной сети
* Пример backend-сервиса с асинхронной очередью
* Личный pet-проект для изучения серверной архитектуры

</details>
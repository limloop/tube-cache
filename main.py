#!/usr/bin/env python3
"""
Точка входа в приложение Video Server
"""
import uvicorn
import sys
from app.config import settings

if __name__ == "__main__":
    # Запускаем сервер с настройками из config.py
    uvicorn.run(
        "app.api:app",
        host=settings.server.host,
        port=settings.server.port,
        workers=settings.server.workers,
        reload=False,
        log_level="info"
    )
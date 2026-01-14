#!/bin/bash

# Конфигурация
SERVER="http://localhost:8000"
CHECK_INTERVAL=3
TIMEOUT=3600

# Проверка аргументов
if [ $# -eq 0 ]; then
    echo "Использование: $0 <youtube-url> [mpv-параметры...]"
    exit 1
fi

URL="$1"
shift
MPV_ARGS="$@"

# Функция для парсинга JSON (простая реализация без зависимостей)
get_json_value() {
    echo "$1" | grep -o "\"$2\":\"[^\"]*\"" | head -1 | cut -d'"' -f4
}

get_json_value_raw() {
    echo "$1" | grep -o "\"$2\":\"[^\"]*\"" | head -1
}

# Шаг 1: Запрос на скачивание
echo "Запрос видео: $URL"
RESPONSE=$(curl -s -X 'GET' "$SERVER/video?url=$URL" -H 'accept: application/json')

if [ -z "$RESPONSE" ]; then
    echo "Ошибка: сервер не ответил"
    exit 1
fi

echo "Ответ сервера: $RESPONSE"

# Получаем хеш и статус
HASH=$(get_json_value "$RESPONSE" "hash")
STATUS=$(get_json_value "$RESPONSE" "status")
STREAM_URL=$(get_json_value "$RESPONSE" "stream_url")
MESSAGE=$(get_json_value "$RESPONSE" "message")

echo "Хеш: ${HASH}"
echo "Статус: $STATUS"
echo "Сообщение: $MESSAGE"

# Проверяем, получен ли хеш
if [ -z "$HASH" ] || [ "$HASH" = "null" ]; then
    echo "Ошибка: не получен хеш видео"
    exit 1
fi

# Если видео уже готово
if [ "$STATUS" = "ready" ] && [ -n "$STREAM_URL" ] && [ "$STREAM_URL" != "null" ]; then
    echo "Видео уже готово к воспроизведению"
    
    # Получаем название через /info/{hash}
    INFO_RESPONSE=$(curl -s "$SERVER/info/$HASH")
    TITLE=$(get_json_value "$INFO_RESPONSE" "title")
    
    # Если название не получено, используем хеш
    if [ -z "$TITLE" ] || [ "$TITLE" = "null" ]; then
        TITLE="Видео ${HASH:0:8}"
    fi
    
    echo "Запуск mpv: $TITLE"
    echo "URL потока: $SERVER$STREAM_URL"
    
    # Запускаем mpv с потоковой ссылкой
    mpv --title="$TITLE" --script-opts="osc-title=\"$TITLE\"" "$SERVER$STREAM_URL" $MPV_ARGS
    exit 0
fi

# Если статус не "ready" (скорее всего "downloading" или "processing")
echo "Ожидание загрузки видео..."
echo "Идентификатор: $HASH"
START_TIME=$(date +%s)

# Основной цикл ожидания
while true; do
    # Проверка таймаута
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    if [ $ELAPSED -gt $TIMEOUT ]; then
        echo "Таймаут ожидания ($TIMEOUT секунд)"
        exit 1
    fi
    
    echo -n "Проверка статуса ($ELAPSED сек)... "
    
    # ВАЖНО: Запрашиваем статус через ТОТ ЖЕ эндпоинт /video с параметром url
    # Или можно через /status/{hash}, если такой эндпоинт есть на сервере
    # Пробуем оба варианта
    
    # Вариант 1: через /video?url=... (предпочтительный)
    NEW_RESPONSE=$(curl -s -X 'GET' "$SERVER/video?url=$URL" -H 'accept: application/json')
    
    # Если ответ пустой, пробуем вариант 2: через /status/{hash}
    if [ -z "$NEW_RESPONSE" ]; then
        NEW_RESPONSE=$(curl -s "$SERVER/status/$HASH")
    fi
    
    # Если все еще пусто, пробуем вариант 3: через /info/{hash} (только для статуса)
    if [ -z "$NEW_RESPONSE" ]; then
        NEW_RESPONSE=$(curl -s "$SERVER/info/$HASH")
    fi
    
    if [ -z "$NEW_RESPONSE" ]; then
        echo "Сервер не ответил"
        sleep $CHECK_INTERVAL
        continue
    fi
    
    # Парсим новый ответ
    CURRENT_STATUS=$(get_json_value "$NEW_RESPONSE" "status")
    CURRENT_STREAM=$(get_json_value "$NEW_RESPONSE" "stream_url")
    CURRENT_MESSAGE=$(get_json_value "$NEW_RESPONSE" "message")
    
    # Отладочная информация
    echo "Статус: $CURRENT_STATUS"
    
    # Обработка статусов
    if [ "$CURRENT_STATUS" = "ready" ] && [ -n "$CURRENT_STREAM" ] && [ "$CURRENT_STREAM" != "null" ]; then
        echo "Загрузка завершена!"
        
        # Получаем название
        INFO_RESPONSE=$(curl -s "$SERVER/info/$HASH")
        TITLE=$(get_json_value "$INFO_RESPONSE" "title")
        
        if [ -z "$TITLE" ] || [ "$TITLE" = "null" ]; then
            TITLE="Видео ${HASH:0:8}"
        fi
        
        echo "Запуск: $TITLE"
        echo "Потоковая ссылка: $SERVER$CURRENT_STREAM"
        
        # Небольшая пауза, чтобы сервер успел подготовить поток
        sleep 1
        
        # Запускаем mpv
        mpv --title="$TITLE" --script-opts="osc-title=\"$TITLE\"" "$SERVER$CURRENT_STREAM" $MPV_ARGS
        exit 0
        
    elif [ "$CURRENT_STATUS" = "failed" ]; then
        echo "Ошибка загрузки: $CURRENT_MESSAGE"
        exit 1
        
    elif [ "$CURRENT_STATUS" = "downloading" ] || [ "$CURRENT_STATUS" = "processing" ]; then
        # Выводим прогресс, если есть сообщение
        if [ -n "$CURRENT_MESSAGE" ] && [ "$CURRENT_MESSAGE" != "null" ]; then
            echo "Прогресс: $CURRENT_MESSAGE"
        fi
        
    elif [ -z "$CURRENT_STATUS" ] || [ "$CURRENT_STATUS" = "null" ]; then
        echo "Статус не получен, продолжаем ожидание..."
    fi
    
    # Пауза перед следующей проверкой
    sleep $CHECK_INTERVAL
done
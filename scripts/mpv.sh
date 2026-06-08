#!/bin/bash

# Configuration
SERVER="http://localhost:8000"
CHECK_INTERVAL=3
TIMEOUT=3600

# Check arguments
if [ $# -eq 0 ]; then
    echo "Usage: $0 <youtube-url> [mpv-parameters...]"
    exit 1
fi

URL="$1"
shift
MPV_ARGS="$@"

# Function for parsing JSON (simple implementation without dependencies)
get_json_value() {
    echo "$1" | grep -o "\"$2\":\"[^\"]*\"" | head -1 | cut -d'"' -f4
}

get_json_value_raw() {
    echo "$1" | grep -o "\"$2\":\"[^\"]*\"" | head -1
}

# Step 1: Request download
echo "Requesting video: $URL"
RESPONSE=$(curl -s -X 'GET' "$SERVER/video?url=$URL" -H 'accept: application/json')

if [ -z "$RESPONSE" ]; then
    echo "Error: server did not respond"
    exit 1
fi

echo "Server response: $RESPONSE"

# Get hash and status
HASH=$(get_json_value "$RESPONSE" "hash")
STATUS=$(get_json_value "$RESPONSE" "status")
STREAM_URL=$(get_json_value "$RESPONSE" "stream_url")
MESSAGE=$(get_json_value "$RESPONSE" "message")

echo "Hash: ${HASH}"
echo "Status: $STATUS"
echo "Message: $MESSAGE"

# Check if hash was received
if [ -z "$HASH" ] || [ "$HASH" = "null" ]; then
    echo "Error: video hash not received"
    exit 1
fi

# If video is already ready
if [ "$STATUS" = "ready" ] && [ -n "$STREAM_URL" ] && [ "$STREAM_URL" != "null" ]; then
    echo "Video is already ready for playback"
    
    # Get title via /info/{hash}
    INFO_RESPONSE=$(curl -s "$SERVER/info/$HASH")
    TITLE=$(get_json_value "$INFO_RESPONSE" "title")
    
    # If title not received, use hash
    if [ -z "$TITLE" ] || [ "$TITLE" = "null" ]; then
        TITLE="Video ${HASH:0:8}"
    fi
    
    echo "Starting mpv: $TITLE"
    echo "Stream URL: $SERVER$STREAM_URL"
    
    # Start mpv with stream URL
    mpv --title="$TITLE" --script-opts="osc-title=\"$TITLE\"" "$SERVER$STREAM_URL" $MPV_ARGS
    exit 0
fi

# If status is not "ready" (likely "downloading" or "processing")
echo "Waiting for video download..."
echo "Identifier: $HASH"
START_TIME=$(date +%s)

# Main waiting loop
while true; do
    # Check timeout
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    if [ $ELAPSED -gt $TIMEOUT ]; then
        echo "Timeout ($TIMEOUT seconds)"
        exit 1
    fi
    
    echo -n "Checking status ($ELAPSED sec)... "
    
    # IMPORTANT: Request status through the SAME /video endpoint with url parameter
    # Or via /status/{hash} if that endpoint exists on the server
    # Trying both options
    
    # Option 1: via /video?url=... (preferred)
    NEW_RESPONSE=$(curl -s -X 'GET' "$SERVER/video?url=$URL" -H 'accept: application/json')
    
    # If response is empty, try option 2: via /status/{hash}
    if [ -z "$NEW_RESPONSE" ]; then
        NEW_RESPONSE=$(curl -s "$SERVER/status/$HASH")
    fi
    
    # If still empty, try option 3: via /info/{hash} (for status only)
    if [ -z "$NEW_RESPONSE" ]; then
        NEW_RESPONSE=$(curl -s "$SERVER/info/$HASH")
    fi
    
    if [ -z "$NEW_RESPONSE" ]; then
        echo "Server did not respond"
        sleep $CHECK_INTERVAL
        continue
    fi
    
    # Parse new response
    CURRENT_STATUS=$(get_json_value "$NEW_RESPONSE" "status")
    CURRENT_STREAM=$(get_json_value "$NEW_RESPONSE" "stream_url")
    CURRENT_MESSAGE=$(get_json_value "$NEW_RESPONSE" "message")
    
    # Debug info
    echo "Status: $CURRENT_STATUS"
    
    # Handle statuses
    if [ "$CURRENT_STATUS" = "ready" ] && [ -n "$CURRENT_STREAM" ] && [ "$CURRENT_STREAM" != "null" ]; then
        echo "Download complete!"
        
        # Get title
        INFO_RESPONSE=$(curl -s "$SERVER/info/$HASH")
        TITLE=$(get_json_value "$INFO_RESPONSE" "title")
        
        if [ -z "$TITLE" ] || [ "$TITLE" = "null" ]; then
            TITLE="Video ${HASH:0:8}"
        fi
        
        echo "Starting: $TITLE"
        echo "Stream URL: $SERVER$CURRENT_STREAM"
        
        # Small pause for server to prepare the stream
        sleep 1
        
        # Start mpv
        mpv --title="$TITLE" --script-opts="osc-title=\"$TITLE\"" "$SERVER$CURRENT_STREAM" $MPV_ARGS
        exit 0
        
    elif [ "$CURRENT_STATUS" = "failed" ]; then
        echo "Download error: $CURRENT_MESSAGE"
        exit 1
        
    elif [ "$CURRENT_STATUS" = "downloading" ] || [ "$CURRENT_STATUS" = "processing" ]; then
        # Show progress if there's a message
        if [ -n "$CURRENT_MESSAGE" ] && [ "$CURRENT_MESSAGE" != "null" ]; then
            echo "Progress: $CURRENT_MESSAGE"
        fi
        
    elif [ -z "$CURRENT_STATUS" ] || [ "$CURRENT_STATUS" = "null" ]; then
        echo "Status not received, continuing to wait..."
    fi
    
    # Pause before next check
    sleep $CHECK_INTERVAL
done
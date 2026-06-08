#!/bin/bash

# Configuration
SERVER="http://localhost:8000"
MAX_VIDEOS=3

# List of channels
CHANNELS=(
    # "https://www.youtube.com/@channel1"
    # "https://www.youtube.com/@channel2"
    # "https://www.youtube.com/@channel3"
)

# Function to get latest videos from channel
get_latest_videos() {
    local channel_url="$1"
    local max="$2"
    
    # Get videos in format: title|url
    yt-dlp --flat-playlist \
           --playlist-end "$max" \
           --print "%(title)s|%(webpage_url)s" \
           "$channel_url" 2>/dev/null | head -n "$max"
}

# Function to send request to server (no waiting)
send_to_server() {
    local video_url="$1"
    local title="$2"
    local channel="$3"
    
    # Send request in background, don't wait for response
    curl -s -X 'GET' "$SERVER/video?url=$video_url" \
         -H 'accept: application/json' \
         > /dev/null 2>&1 &
    
    echo "    ✓ Request sent: $title"
}

# Main function
main() {
    echo "=== Sending videos to server ==="
    echo "Server: $SERVER"
    echo "Max videos per channel: $MAX_VIDEOS"
    echo ""
    
    local total_sent=0
    
    for channel in "${CHANNELS[@]}"; do
        echo "Processing channel: $channel"
        
        # Get video list
        local videos=$(get_latest_videos "$channel" "$MAX_VIDEOS")
        
        if [ -z "$videos" ]; then
            echo "  ✗ Failed to get videos"
            echo ""
            continue
        fi
        
        local count=0
        while IFS='|' read -r title url; do
            if [ -n "$url" ]; then
                count=$((count + 1))
                echo "  $count. $title"
                send_to_server "$url" "$title" "$channel"
                total_sent=$((total_sent + 1))
            fi
        done <<< "$videos"
        
        echo "  Sent: $count videos"
        echo ""
    done
    
    echo "=== Total requests sent: $total_sent ==="
    
    # Small pause to let background curl processes finish
    sleep 1
    wait
    echo "Done!"
}

# Check dependencies
if ! command -v yt-dlp &> /dev/null; then
    echo "Error: yt-dlp is not installed"
    echo "Install: pip install yt-dlp"
    exit 1
fi

if ! command -v curl &> /dev/null; then
    echo "Error: curl is not installed"
    exit 1
fi

# Run
main
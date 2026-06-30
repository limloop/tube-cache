#!/bin/bash

# Configuration
SERVER="http://localhost:8000"

# Function to get all videos from playlist
get_playlist_videos() {
    local playlist_url="$1"
    
    # Get all videos in format: title|url
    yt-dlp --flat-playlist \
           --print "%(title)s|%(webpage_url)s" \
           "$playlist_url" 2>/dev/null
}

# Function to send request to server (no waiting)
send_to_server() {
    local video_url="$1"
    local title="$2"
    local playlist="$3"
    
    # Send request in background, don't wait for response
    curl -s -X 'GET' "$SERVER/video?url=$video_url" \
         -H 'accept: application/json' \
         > /dev/null 2>&1 &
    
    echo "    ✓ Request sent: $title"
}

# Function to display usage
usage() {
    echo "Usage: $0 <playlist_url>"
    echo ""
    echo "Example:"
    echo "  $0 'https://www.youtube.com/playlist?list=PLxxxxxx'"
    echo "  $0 'https://www.youtube.com/@channel/playlists'"
    exit 1
}

# Main function
main() {
    local playlist_url="$1"
    
    # Check if playlist URL provided
    if [ -z "$playlist_url" ]; then
        echo "Error: No playlist URL provided"
        usage
    fi
    
    echo "=== Sending playlist videos to server ==="
    echo "Server: $SERVER"
    echo "Playlist: $playlist_url"
    echo ""
    
    echo "Fetching videos from playlist..."
    
    # Get video list
    local videos=$(get_playlist_videos "$playlist_url")
    
    if [ -z "$videos" ]; then
        echo "  ✗ Failed to get videos from playlist"
        echo "  Please check the playlist URL and your internet connection"
        exit 1
    fi
    
    local total=0
    local count=0
    
    while IFS='|' read -r title url; do
        if [ -n "$url" ]; then
            count=$((count + 1))
            echo "$count. $title"
            send_to_server "$url" "$title" "$playlist_url"
            total=$((total + 1))
        fi
    done <<< "$videos"
    
    echo ""
    echo "=== Total videos found: $count ==="
    echo "=== Total requests sent: $total ==="
    
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

# Run with provided argument
main "$1"
import os
import uuid
import subprocess
import logging
from flask import Flask, request, send_file, render_template_string
import requests
import m3u8
from urllib.parse import urljoin
import threading
import time

# Setting up the app
app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# Configuration
TEMP_DIR = ".temp"
DELETE_DELAY = 300  # 5 minutes in seconds

# Ensure the temporary directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

# Queue to store files for deletion
delete_queue = []

def delayed_delete():
    """Background task to clean up temporary files."""
    while True:
        current_time = time.time()
        files_to_delete = [f for f in delete_queue if current_time - f['time'] > DELETE_DELAY]
        
        for file_info in files_to_delete:
            try:
                os.remove(file_info['path'])
                delete_queue.remove(file_info)
            except Exception as e:
                app.logger.error(f"Error deleting file {file_info['path']}: {e}")
        
        time.sleep(60)

# Start the background deletion task
threading.Thread(target=delayed_delete, daemon=True).start()

def generate_uuid(url):
    return uuid.uuid3(uuid.NAMESPACE_URL, url)

def get_highest_quality_stream(m3u8_url):
    """Get the highest quality stream from the M3U8 playlist."""
    try:
        response = requests.get(m3u8_url)
        response.raise_for_status()
        playlist = m3u8.loads(response.text)
        
        if not playlist.is_variant:
            return m3u8_url
            
        # Get highest bandwidth stream
        streams = [(p.stream_info.bandwidth, urljoin(m3u8_url, p.uri)) 
                  for p in playlist.playlists]
        best_stream = max(streams, key=lambda x: x[0])[1]
        
        return best_stream
    except Exception as e:
        app.logger.error(f"Error processing M3U8: {e}")
        raise

@app.route('/', methods=['GET'])
def index():
    return render_template_string('''
        <!DOCTYPE html>
        <html>
        <head>
            <title>M3U8 to MP4 Converter (H.265/MP3)</title>
        </head>
        <body>
            <h1>M3U8 to MP4 Converter</h1>
            <form action="/get" method="post">
                <input type="text" name="url" placeholder="Enter M3U8 URL" required /><br/>
                <input type="text" name="filename" placeholder="Output filename (without .mp4)" /><br/>
                <input type="submit" value="Convert">
            </form>
        </body>
        </html>
    ''')

@app.route('/get', methods=['POST', 'GET'])
def convert_m3u8_to_mp4():
    if request.is_json and request.json is not None:
        m3u8_url = request.json.get('url')
        output_filename = request.json.get('filename')
    else:
        m3u8_url = request.form.get('url')
        output_filename = request.form.get('filename')
    
    # Check if URL is provided, otherwise return an error
    if not m3u8_url:
        return {"error": "Missing URL or filename"}, 400
    
    # Generate a unique filename if not provided
    if not output_filename:
        output_filename = str(generate_uuid(m3u8_url))
    
    # Ensure filename ends with .mp4
    if not str(output_filename).endswith('.mp4'):
        output_filename = str(output_filename) + '.mp4'
    
    # Create temporary filename
    temp_filename = os.path.join(TEMP_DIR, f"temp_{uuid.uuid4()}.mp4")
    
    try:
        # Get highest quality stream
        stream_url = get_highest_quality_stream(m3u8_url)
        
        # Prepare FFmpeg command
        ffmpeg_command = [
            'ffmpeg',
            '-i', stream_url,
            '-c:v', 'libx265',  # H.265 video codec
            '-preset', 'medium',  # Balanced quality/speed preset
            '-crf', '23',  # Constant Rate Factor (lower = better quality)
            '-c:a', 'libmp3lame',  # MP3 audio codec
            '-q:a', '2',  # Audio quality (0-9, lower = better)
            '-movflags', '+faststart',  # Enable streaming
            temp_filename
        ]
        
        # Run FFmpeg
        result = subprocess.run(ffmpeg_command, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"FFmpeg error: {result.stderr}")
        
        # Queue file for deletion
        delete_queue.append({'path': temp_filename, 'time': time.time()})
        
        # Return the converted file
        return send_file(temp_filename, as_attachment=True, download_name=output_filename)
        
    except Exception as e:
        app.logger.error(f"Conversion error: {e}")
        return {"error": str(e)}, 500

if __name__ == '__main__':
    app.run(debug=False)

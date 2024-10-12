import os
import re
import uuid
import subprocess
from datetime import datetime, timezone
import time
import json
import logging

# Import Flask related modules
from flask import Flask, request, send_file, render_template_string, jsonify
import requests
import m3u8
from urllib.parse import urljoin

# Impoorting threading library to handle cleaning up of temporary files
import threading

# Importing Ollama-specific libraries should be done last to avoid any naming conflicts
import ollama

# Setting up the app
app = Flask(__name__)
app.logger.setLevel(logging.WARN)
logging.getLogger('werkzeug').setLevel(logging.WARN)
logging.getLogger('httpx').setLevel(logging.WARN)
logging.basicConfig(level=logging.INFO,
                    format='\033[1;37m' + '[%(asctime)s]' + '[%(name)s]: ' + '\u001b[0m' + '%(message)s',
                    datefmt='%H:%M')

# Setting up some hard coded configuration variables
TEMP_DIR = ".temp" # Temporary directory for downloaded files and generated streams
DELETE_DELAY = 300  # 5 minutes in seconds
FILENAME_MAX_LENGTH = 64 # Maximum length of filenames in bytes
OLLAMA_MODEL = "qwen2.5" # Qwen 2.5 model
USE_OLLAMA = True # Use ollama to generate streams

# Ensure the temporary directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

# Queue to store files for deletion
delete_queue = []

def delayed_delete():
    """
    Function that runs in a loop, periodically checking and deleting old files from the delete queue.
    """
    while True:
        current_time = time.time()
        # Filter out files older than DELETE_DELAY seconds
        files_to_delete = [f for f in delete_queue if current_time - f['time'] > DELETE_DELAY]

        for file_info in files_to_delete:
            try:
                # Attempt to remove the temporary file
                os.remove(file_info['path'])
                logging.debug(f"Deleted temporary file: {file_info['path']}")
                # Remove the processed entry from the queue
                delete_queue.remove(file_info)
            except Exception as e:
                # Log any errors that occur during deletion
                logging.error(f"Error deleting file {file_info['path']}: {e}")

        # Sleep for 60 seconds before checking again
        time.sleep(60)  # Check every minute

# Start the background deletion task
threading.Thread(target=delayed_delete, daemon=True).start()

def get_stream_info(m3u8_url):
    """
    Function to fetch and parse information about streams from an M3U8 URL.

    Parameters:
        m3u8_url (str): The URL of the M3U8 playlist.
    Returns:
        dict: A dictionary containing video streams, audio streams, and metadata.
                - 'video_streams': List of dictionaries with details about each video stream
                - 'audio_streams': Dictionary mapping audio group IDs to lists of audio streams
                - 'metadata': Dictionary with general information like duration, version etc.

    Raises:
        requests.RequestException: If there's an error fetching the M3U8 playlist.
    """
    try:
        # Send a request to the M3U8 URL and ensure it's successful
        response = requests.get(m3u8_url)
        response.raise_for_status()

        # Parse the M3U8 playlist data from the response
        playlist = m3u8.loads(response.text)

        if playlist.is_variant:
            video_streams = []
            audio_streams = {}

            # Process audio streams first
            for media in playlist.media:
                if media.type == 'AUDIO':
                    audio_stream = {
                        'group_id': media.group_id,
                        'language': media.language,
                        'name': media.name,
                        'default': media.default,
                        'autoselect': media.autoselect,
                        'uri': urljoin(m3u8_url, media.uri) if media.uri else None
                    }

                    # If the URI is specified for this audio stream, fetch its metadata
                    if media.uri:
                        # Fetch the audio stream's metadata
                        audio_stream.update(get_audio_stream_info(audio_stream['uri']))

                    # Group audio streams by their group ID
                    if media.group_id not in audio_streams:
                        audio_streams[media.group_id] = []
                    audio_streams[media.group_id].append(audio_stream)

            # Process video streams and associate them with audio streams
            for p in playlist.playlists:
                stream_info = {
                    'bandwidth': p.stream_info.bandwidth,
                    'resolution': p.stream_info.resolution,
                    'codecs': p.stream_info.codecs,
                    'uri': urljoin(m3u8_url, p.uri)
                }

                # Check if this stream has an associated audio group
                if hasattr(p.stream_info, 'audio') and p.stream_info.audio in audio_streams:
                    stream_info['audio_group'] = p.stream_info.audio
                    stream_info['associated_audio'] = find_best_audio_stream(audio_streams[p.stream_info.audio])
                else:
                    # If no associated audio group, find the best overall audio stream
                    best_audio = find_best_audio_stream([stream for group in audio_streams.values() for stream in group])
                    if best_audio:
                        stream_info['associated_audio'] = best_audio

                # Add the processed video stream to the list of video streams
                video_streams.append(stream_info)

            # Collect metadata about the playlist and its streams
            metadata = {
                'duration': playlist.target_duration,
                'version': playlist.version,
                'video_stream_count': len(video_streams),
                'audio_stream_count': sum(len(group) for group in audio_streams.values())
            }

            # Return a dictionary containing video streams, audio streams, and metadata
            return {
                'video_streams': video_streams,
                'audio_streams': audio_streams,
                'metadata': metadata
            }
        else:
            # If not a variant playlist, it's likely a simple playlist without different resolutions
            segments = playlist.segments
            duration = sum(segment.duration for segment in segments)

            # Return basic information about the single video stream and the audio streams, if any
            return {
                'video_streams': [{
                    'bandwidth': None,
                    'resolution': None,
                    'codecs': None,
                    'uri': m3u8_url  # The URI is the m3u8 URL itself in this case
                }],
                'audio_streams': {},  # No audio streams for simple playlist
                'metadata': {
                    'duration': duration,  # Total segment duration
                    'version': playlist.version,
                    'segment_count': len(segments)  # Number of segments in the playlist
                }
            }
    except requests.RequestException as e:
        logging.error(f"Error fetching M3U8: {e}")
        raise

def find_best_audio_stream(audio_streams):
    """
    Determines and returns the best audio stream from a list based on certain criteria.

    Parameters:
    - audio_streams (list): A list of dictionaries, each representing an audio stream with attributes like 'default', 'autoselect', and 'bandwidth'.

    Returns:
    - dict: The dictionary representing the chosen best audio stream. If no suitable streams are found, returns None.
    """
    if not audio_streams:
        return None

    # First, try to find a default stream
    default_streams = [stream for stream in audio_streams if stream.get('default') == 'YES']
    if default_streams:
        return max(default_streams, key=lambda s: s.get('bandwidth', 0))

    # If no default, try to find an autoselect stream
    autoselect_streams = [stream for stream in audio_streams if stream.get('autoselect') == 'YES']
    if autoselect_streams:
        return max(autoselect_streams, key=lambda s: s.get('bandwidth', 0))

    # If neither default nor autoselect, just return the highest bandwidth stream
    return max(audio_streams, key=lambda s: s.get('bandwidth', 0))

def get_audio_stream_info(audio_m3u8_url):
    """
    Fetches and analyzes an audio M3U8 playlist to determine the best available stream.

    Parameters:
    - audio_m3u8_url (str): The URL of the M3U8 playlist to analyze.
    Returns:
    - dict: A dictionary containing information about the selected audio stream, including its URI and estimated bitrate.
            If no suitable streams are found, returns a dictionary with `bandwidth` set to None and the original URI.
    """
    try:
        response = requests.get(audio_m3u8_url)
        response.raise_for_status()
        audio_playlist = m3u8.loads(response.text)

        # If the playlist is a variant playlist, recursively find the best stream
        if audio_playlist.is_variant:
            return max(
                (get_audio_stream_info(urljoin(audio_m3u8_url, p.uri)) for p in audio_playlist.playlists),
                key=lambda x: x.get('bandwidth', 0)
            )

        # Initialize variables to track the best byterange and its URI
        max_byterange = 0
        max_byterange_uri = None

        # Iterate over each segment to find the one with the largest byterange size
        for segment in audio_playlist.segments:
            if segment.byterange:
                # Split 'byterange' into offset and length using '@' as delimiter
                length, offset = map(int, segment.byterange.split('@'))

                # Update max_byterange_uri if current segment has a larger byterange size
                if offset + length > max_byterange:
                    max_byterange = offset + length
                    max_byterange_uri = segment.uri

        # Once the largest byterange URI is found, return it with its estimated bitrate
        if max_byterange_uri:
            return {'bandwidth': estimate_bitrate(audio_playlist), 'uri': urljoin(audio_m3u8_url, max_byterange_uri)}
        else:
            # Fallback to using the M3U8 URL itself as the URI if no byterange is found
            return {'bandwidth': estimate_bitrate(audio_playlist), 'uri': audio_m3u8_url}

    except requests.RequestException as e:
        logging.error(f"Error fetching audio M3U8: {e}")
        return {'bandwidth': None, 'uri': audio_m3u8_url}

def estimate_bitrate(playlist):
    """
    Estimates the bitrate of an audio stream based on its segments' durations and byterange sizes.

    Parameters:
    - playlist (m3u8.MediaPlaylist): The M3U8 playlist object containing segment information.

    Returns:
    - int: The estimated bitrate in bits per second. Returns None if no segments or total duration is zero.
    """
    # Calculate total duration of all segments in the playlist
    total_duration = sum(segment.duration for segment in playlist.segments)

    # Sum up the total byte range size (length) across all segments
    total_bytes = sum(int(segment.byterange.split('@')[0]) if segment.byterange else 0 for segment in playlist.segments)

    # If there is a total duration, calculate bitrate as (total bytes * 8 bits per byte) / total_duration (in seconds)
    if total_duration > 0:
        return int((total_bytes * 8) / total_duration)
    
    # Return None if no segments or total duration is zero
    return None

def generate_filename(url):
    """
    Generates a filename for the given URL.
    Tries to extract it from the Content-Disposition header, otherwise uses the last part of the URL path,
    removing '.m3u8' and appending .mp4 if necessary. If no suitable name is found, generates a UUID-based one.
    """
    response = requests.head(url)
    cd_header = response.headers.get('Content-Disposition')
    if cd_header:
        filename = re.findall("filename=(.+)", cd_header)
        if filename:
            return filename[0].strip('"')

    url_filename = url.split('/')[-1].split('?')[0]
    if url_filename.endswith('.m3u8'):
        return url_filename[:-5] + '.mp4'

    return str(uuid.uuid4()) + '.mp4'

def urljoin(base, url):
    """
    Helper function to join URLs.

    Parameters:
    - base (str): The base URL string.
    - url (str): The relative URL string.

    Returns:
    - str: The joined URL as a string.
    """
    from urllib.parse import urljoin
    return urljoin(base, url)

@app.route('/', methods=['GET'])
def index():
    return render_template_string('''
        <!DOCTYPE html>
        <html>
        <head>
            <title>M3U8 to MP4 Converter</title>
        </head>
        <body>
            <h1>M3U8 to MP4 Converter</h1>
            <form action="/convert" method="post">
                <input type="text" name="url" placeholder="Enter M3U8 URL" required /><br/>
                <input type="text" name="alt_url" placeholder="Enter alt. URL" required /><br/>
                <input type="text" name="title" placeholder="Enter video title (optional)" /><br/>
                <input type="text" name="video_id" placeholder="Enter video id (optional)" /><br/>
                <textarea name="description" placeholder="Enter video description (optional)" cols="40" rows="5"></textarea><br/>
                <input type="submit" value="Convert"><br/>
            </form>
            <hr>
            <h2>Get Stream Info</h2>
            <form action="/stream_info" method="get">
                <input type="text" name="url" placeholder="Enter M3U8 URL" required>
                <input type="submit" value="Get Info">
            </form>
        </body>
        </html>
    ''')

@app.route('/stream_info', methods=['GET'])
def stream_info():
    # Extract the M3U8 URL from the query parameters
    m3u8_url = request.args.get('url')
    
    # Check if a valid M3U8 URL is provided
    if not m3u8_url:
        return jsonify({"error": "No URL provided"}), 400

    try:
        # Retrieve stream information from the M3U8 URL
        info = get_stream_info(m3u8_url)
        
        # Return the retrieved information as a JSON response
        return jsonify(info)
    
    except Exception as e:
        # Log any unexpected errors that occur during the process
        logging.error(f"Error getting stream info: {e}")
        
        # If an error occurs, return it in the JSON response with a 500 status code
        return jsonify({"error": str(e)}), 500
    
@app.route('/convert', methods=['POST', 'GET'])
def convert_m3u8_to_mp4():
    if request.is_json:
        m3u8_url = request.json.get('url')
        alt_url = request.json.get('alt_url')
        title = request.json.get('title')
        video_id = request.json.get('video_id')
        description = request.json.get('description', '')
    else:
        m3u8_url = request.args.get('url')
        alt_url = request.args.get('alt_url')
        title = request.args.get('title')
        video_id = request.args.get('video_id')
        description = request.args.get('description', '')

    # If the url is not set, return Bad Request
    if not m3u8_url:
        logging.error(f"No URL provided: {m3u8_url}")
        return {"error": "No URL provided"}, 400

    # Temporary filename to store the downloaded file
    temp_filename = None

    try:
        logging.debug(f"Received M3U8 URL: {m3u8_url}")

        # Get stream info
        stream_info = get_stream_info(m3u8_url)

        # If no video stream is available, error out
        if not stream_info['video_streams']:
            return {"error": "No video streams found"}, 400

        best_video_stream = max(stream_info['video_streams'], key=lambda s: s.get('bandwidth', 0))

        try:
            # If Ollama is enabled then use it to summarize the title for a shorter filename
            if USE_OLLAMA:
                # Call the Ollama model
                ollama_response = ollama.chat(model=OLLAMA_MODEL, messages=[{'role': 'user', 'content': f"Summarize this title in English, around 64 characters or less, and return only that and nothing else: {title}"}])

                # Extract the response text
                output_filename = f"{ollama_response['message']['content']}.mp4"

                # If the output_filename contains multiple '.' then condense them down to max one in a row
                output_filename = re.sub('\.+', '.', output_filename)
        except ollama.ResponseError as e:
            logging.error(f"Unexpected error: {e}")

        # If we have a response, use it as the filename. Otherwise, use the title as filename or if no title, try to create one from the URL.
        if not output_filename:
            if title:
                output_filename = f"{title[:FILENAME_MAX_LENGTH]}.mp4"
            else:
                output_filename = generate_filename(m3u8_url)

        # Create a temporary filename to store the downloaded video
        temp_filename = os.path.join(TEMP_DIR, f"temp_{uuid.uuid4()}.mp4")

        # ffmpeg command to extract the video stream from the m3u8 file
        ffmpeg_command = ['ffmpeg', '-i', best_video_stream['uri']]

        # Handle audio stream selection
        audio_stream = None
        if 'associated_audio' in best_video_stream and best_video_stream['associated_audio']:
            # Get the first audio stream that matches the video stream's associated audio stream
            audio_stream = best_video_stream['associated_audio']
        elif stream_info['audio_streams']:
            # Get all audio streams
            all_audio_streams = [stream for group in stream_info['audio_streams'].values() for stream in group]

            # Get the stream with the highest bandwidth
            audio_stream = max(all_audio_streams, key=lambda s: s.get('bandwidth', 0))

        # Add audio stream to ffmpeg command
        if audio_stream and audio_stream.get('uri'):
            ffmpeg_command.extend(['-i', audio_stream['uri']])

        # Add metadata
        current_time = datetime.now(timezone.utc).isoformat()
        comment = json.dumps({
                "source_url":m3u8_url,
                "alt_url":alt_url,
                "video_id":video_id,
                "downloaded":current_time,
            }, indent=2)
        ffmpeg_command.extend([
            '-metadata', f'title={title}',
            '-metadata', f'description={description}',
            '-metadata', f'comment={comment}',
            '-c', 'copy',
            '-bsf:a', 'aac_adtstoasc',
            temp_filename
        ])

        # Run ffmpeg command
        logging.debug(f"Running FFmpeg command: {' '.join(ffmpeg_command)}")
        result = subprocess.run(ffmpeg_command, capture_output=True, text=False)

        # Check error code and return it
        if result.returncode != 0:
            logging.error(f"FFmpeg error: {result.stderr}")
            return {"error": f"FFmpeg error: {result.stderr}"}, 500

        # Queue the file for deletion
        delete_queue.append({'path': temp_filename, 'time': time.time()})

        # Return the file to the user
        logging.info(f"Returning '{output_filename}' to client.")
        return send_file(temp_filename, as_attachment=True, download_name=output_filename)

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return {"error": str(e)}, 500

if __name__ == '__main__':
    app.run(debug=False)


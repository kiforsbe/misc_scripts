import os
import uuid
import logging
import threading
import time
from flask import Flask, request, send_file, render_template_string, jsonify
import requests
import eyed3
import magic
from io import BytesIO
from mutagen.mp4 import MP4, MP4Cover
from PIL import Image
from audio_metadata import AudioMetadata, get_metadata_writer

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
eyed3.log.setLevel("ERROR")

# --- Import the music genre classifier ---
try:
    # Assuming music_style_classifier.py is in the same directory or accessible via PYTHONPATH
    from music_style_classifier import get_music_genre
    MUSIC_CLASSIFIER_AVAILABLE = True
    logging.info("Successfully imported music_style_classifier.")
except ImportError as e:
    logging.warning(f"Could not import music_style_classifier: {e}. Genre auto-detection will be disabled.")
    MUSIC_CLASSIFIER_AVAILABLE = False
    # Define a dummy function if import fails to avoid NameError later
    def get_music_genre(file_path: str, track_index: int = None) -> str | None:
        logging.warning("Music classifier not available, cannot auto-detect genre.")
        return None

# Use Flask's built-in server for development/testing
app = Flask(__name__)

# Temporary directory for storing files
TEMP_DIR = ".temp"
DELETE_DELAY = 300  # 5 minutes in seconds
os.makedirs(TEMP_DIR, exist_ok=True)

# Queue to store files for deletion
delete_queue = []
delete_lock = threading.Lock() # Add a lock for thread safety

def delayed_delete():
    while True:
        current_time = time.time()

        with delete_lock:
            files_to_delete = [f for f in delete_queue if current_time - f['time'] > DELETE_DELAY]
            
            for file_info in files_to_delete:
                try:
                    os.remove(file_info['path'])
                    logging.info(f"Deleted temporary file: {file_info['path']}")
                    delete_queue.remove(file_info)
                except FileNotFoundError:
                    logging.warning(f"Temporary file already deleted: {file_info['path']}")
                except Exception as e:
                    logging.error(f"Error deleting file {file_info['path']}: {e}")
        
        time.sleep(60)  # Check every minute

# Start the background deletion task
delete_thread = threading.Thread(target=delayed_delete, daemon=True)
delete_thread.start()

def generate_uuid(url):
    return uuid.uuid3(uuid.NAMESPACE_URL, url)

@app.route('/', methods=['GET'])
def home():
    return render_template_string('''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Audio downloader with external metadata</title>
        </head>
        <body>
            <h1>Audio downloader with external metadata</h1>
            <form action="/api/download_ext" method="post">
                <input type="text" name="audio_url" placeholder="Enter Audio URL (MP3 or M4A)" required /><br/>
                <input type="text" name="image_url" placeholder="Enter Cover Image URL" required /><br/>
                <input type="text" name="title" placeholder="Enter title" /><br/>
                <input type="text" name="artist" placeholder="Enter artist" /><br/>
                <input type="text" name="album" placeholder="Enter album" /><br/>
                <input type="text" name="genre" placeholder="Enter genre" /><br/>
                <input type="text" name="year" placeholder="Enter year" /><br/>
                <input type="submit" value="Download">
            </form>
        </body>
        </html>
    ''')

def convert_to_jpeg(image_data: bytes) -> bytes:
    """Convert image data to JPEG format."""
    try:
        # Open the image from bytes
        img = Image.open(BytesIO(image_data))
        # Convert to RGB if necessary (e.g., for PNG with transparency)
        if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.convert('RGBA').split()[-1])
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Save as JPEG
        output = BytesIO()
        img.save(output, format='JPEG', quality=95)
        return output.getvalue()
    except Exception as e:
        logging.error(f"Error converting image to JPEG: {e}", exc_info=True)
        return image_data  # Return original data if conversion fails

@app.route('/api/download_ext', methods=['POST', 'GET'])
def download_ext():
    if request.is_json:
        data = request.json
    elif request.method == 'POST':
        data = request.form
    else:
        data = request.args

    audio_url = data.get('audio_url', data.get('mp3_url'))  # Support both new and old parameter
    image_url = data.get('image_url')

    if not audio_url or not image_url:
        logging.warning("Missing audio_url or image_url in request.")
        return jsonify({"error": "Both 'audio_url' and 'image_url' parameters are required"}), 400

    # Determine audio_mime_type from URL
    is_m4a = audio_url.lower().endswith('.m4a')
    extension = '.m4a' if is_m4a else '.mp3'
    audio_mime_type = 'audio/mp4' if is_m4a else 'audio/mpeg'

    temp_audio_path = None
    try:
        # Set up MIME type detection
        mime_detector = magic.Magic(mime=True)

        # --- Download audio file ---
        logging.info(f"Downloading audio from: {audio_url}")
        audio_response = requests.get(audio_url, timeout=60)
        audio_response.raise_for_status()
        audio_bytes_io = BytesIO(audio_response.content)
        logging.info("Audio downloaded successfully.")

        # --- Detect audio mime-type ---
        audio_mime_type = mime_detector.from_buffer(audio_bytes_io.getvalue())
        logging.info(f"Detected audio MIME type: {audio_mime_type}")
        if audio_mime_type not in ['audio/mpeg', 'audio/mp4']:
            logging.warning(f"Downloaded file from audio_url might not be an audio file (MIME: {audio_mime_type}). Proceeding anyway.")
            # You could choose to return an error here if strict audio type is required

        # --- Download image file ---
        logging.info(f"Downloading cover image from: {image_url}")
        img_response = requests.get(image_url, timeout=30) # Add timeout
        img_response.raise_for_status()
        img_data = img_response.content
        logging.info("Cover image downloaded successfully.")

        # --- Detect image mime-type ---
        img_mime_type = mime_detector.from_buffer(img_data)
        logging.info(f"Detected image MIME type: {img_mime_type}")
        if not img_mime_type.startswith('image/'):
            logging.warning(f"Downloaded file from image_url might not be an image (MIME: {img_mime_type}). Proceeding anyway.")
            # You could choose to return an error here if strict image type is required

        # Convert image to JPEG if it's not already JPEG
        if not img_mime_type.lower() in ['image/jpeg', 'image/jpg']:
            logging.info(f"Converting {img_mime_type} image to JPEG")
            img_data = convert_to_jpeg(img_data)
            # Recheck mime type from the converted data
            img_mime_type = mime_detector.from_buffer(img_data)
            logging.info(f"Image converted and new mime type detected: {img_mime_type}")
            if not img_mime_type.lower() in ['image/jpeg', 'image/jpg']:
                logging.warning(f"Conversion may have failed, got {img_mime_type} instead of JPEG")

        # --- Create a temporary file for the audio ---
        temp_filename = f"temp_{generate_uuid(audio_url)}_{uuid.uuid4().hex[:8]}{extension}"
        temp_audio_path = os.path.join(TEMP_DIR, temp_filename)
        logging.info(f"Saving audio temporarily to: {temp_audio_path}")
        with open(temp_audio_path, 'wb') as f:
            f.write(audio_bytes_io.getvalue())

        # Get genre from classifier if available
        classified_genre = None
        if MUSIC_CLASSIFIER_AVAILABLE:
            logging.info("Attempting automatic genre classification...")
            try:
                classified_genre = get_music_genre(temp_audio_path)
                if classified_genre:
                    logging.info(f"Successfully classified genre: {classified_genre}")
                else:
                    logging.warning("Music classifier did not return a genre.")
            except Exception as classifier_err:
                logging.error(f"Error during music classification: {classifier_err}", exc_info=True)

        # Create metadata object with all available data
        try:
            year_value = int(data.get('year')) if data.get('year') else None
        except ValueError:
            logging.warning(f"Invalid year value: {data.get('year')}")
            year_value = None

        metadata = AudioMetadata(
            title=data.get('title'),
            artist=data.get('artist'),
            album=data.get('album'),
            genre=classified_genre or data.get('genre'),
            year=year_value,
            lyrics=data.get('lyrics'),
            canonical_url=data.get('canonical'),
            cover_art=img_data
        )

        # Get appropriate writer and apply metadata
        try:
            writer = get_metadata_writer(temp_audio_path)
            writer.write_metadata(temp_audio_path, metadata)
            logging.info("Audio metadata written successfully")
        except Exception as e:
            logging.error(f"Error writing metadata: {e}", exc_info=True)
            if os.path.exists(temp_audio_path):
                os.remove(temp_audio_path)
            return jsonify({"error": "Failed to write audio metadata"}), 500

        # --- Construct the final filename ---
        # Sanitize title and artist for use in filename
        def sanitize_filename(name):
            # Remove characters invalid for filenames in most OS
            # Keep it simple: replace common problematic chars
            if not name: return ""
            name = name.replace('/', '-').replace('\\', '-').replace(':', '-').replace('*', '-').replace('?', '').replace('"', "'").replace('<', '').replace('>', '').replace('|', '')
            return name.strip()

        # Use metadata for filename construction
        safe_artist = sanitize_filename(metadata.artist) if metadata.artist else ''
        safe_title = sanitize_filename(metadata.title) if metadata.title else ''

        if safe_artist and safe_title:
            filename = f"{safe_artist} - {safe_title}{extension}"
        elif safe_title:
            filename = f"{safe_title}{extension}"
        elif safe_artist:
            filename = f"{safe_artist} - Unknown Title{extension}"
        else:
            # Fallback to a generic name if no artist/title
            filename = f"processed_{generate_uuid(audio_url)}{extension}"
        logging.info(f"Generated download filename: {filename}")

        # --- Queue the file for deletion ---
        with delete_lock:
            delete_queue.append({'path': temp_audio_path, 'time': time.time()})
        logging.info(f"Scheduled temporary file for deletion: {temp_audio_path}")

        # --- Send the modified file back to the user ---
        logging.info("Sending modified audio file to client...")
        return send_file(
            temp_audio_path,
            as_attachment=True,
            download_name=filename,
            mimetype=audio_mime_type
            )

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error downloading files: {e}", exc_info=True)
        # Don't expose raw URLs in error messages to the client
        return jsonify({"error": f"Error downloading external resources. Please check the URLs."}), 502 # Bad Gateway might be appropriate
    except eyed3.Error as e:
        logging.error(f"eyed3 metadata processing error: {e}", exc_info=True)
        return jsonify({"error": f"Error processing MP3 metadata: {str(e)}"}), 500
    except magic.MagicException as e:
        logging.error(f"Error detecting image type: {e}", exc_info=True)
        return jsonify({"error": "Could not determine the type of the provided image file."}), 400
    except Exception as e:
        # Catch-all for unexpected errors
        logging.error(f"An unexpected error occurred: {e}", exc_info=True) # Log traceback for debugging
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        # Ensure cleanup happens even if send_file fails (though it shouldn't block)
        # The delayed deletion handles the primary cleanup. This 'finally' is less critical now.
        # If temp_mp3_path was created but not queued (e.g., error before queueing),
        # it might leak. Consider adding cleanup here ONLY IF it wasn't queued.
        pass

if __name__ == '__main__':
    # Use a production-ready server like Gunicorn or Waitress instead of app.run(debug=True)
    # For development:
    # Bind to 0.0.0.0 to make it accessible on the network if needed
    app.run(host='0.0.0.0', port=5000, debug=True) # Set debug=False for production
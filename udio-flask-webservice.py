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

# Set up logging
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')
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
            <title>MP3 downloader with external metadata</title>
        </head>
        <body>
            <h1>MP3 downloader with external metadata</h1>
            <form action="/api/download_ext" method="post">
                <input type="text" name="mp3_url" placeholder="Enter MP3 URL" required /><br/>
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

@app.route('/api/download_ext', methods=['POST', 'GET'])
def download_ext():
    if request.is_json:
        data = request.json
    elif request.method == 'POST':
        data = request.form
    else: # GET request
        data = request.args

    mp3_url = data.get('mp3_url')
    image_url = data.get('image_url')

    if not mp3_url or not image_url:
        logging.warning("Missing mp3_url or image_url in request.")
        return jsonify({"error": "Both 'mp3_url' and 'image_url' parameters are required"}), 400

    temp_mp3_path = None # Initialize path variable
    try:
        # --- Download MP3 file ---
        logging.info(f"Downloading MP3 from: {mp3_url}")
        mp3_response = requests.get(mp3_url, timeout=60) # Add timeout
        mp3_response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        mp3_bytes_io = BytesIO(mp3_response.content)
        logging.info("MP3 downloaded successfully.")

        # --- Download image file ---
        logging.info(f"Downloading cover image from: {image_url}")
        img_response = requests.get(image_url, timeout=30) # Add timeout
        img_response.raise_for_status()
        img_data = img_response.content
        logging.info("Cover image downloaded successfully.")

        # --- Detect image mime-type ---
        # Use context manager for magic object if possible, though it's usually fine globally
        mime_detector = magic.Magic(mime=True)
        mime_type = mime_detector.from_buffer(img_data)
        logging.info(f"Detected image MIME type: {mime_type}")
        if not mime_type.startswith('image/'):
            logging.warning(f"Downloaded file from image_url might not be an image (MIME: {mime_type}). Proceeding anyway.")
            # You could choose to return an error here if strict image type is required

        # --- Create a temporary file for the MP3 ---
        # Generate a unique name for the temp file
        temp_filename = f"temp_{generate_uuid(mp3_url)}_{uuid.uuid4().hex[:8]}.mp3"
        temp_mp3_path = os.path.join(TEMP_DIR, temp_filename)
        logging.info(f"Saving MP3 temporarily to: {temp_mp3_path}")
        with open(temp_mp3_path, 'wb') as f:
            f.write(mp3_bytes_io.getvalue()) # Use getvalue() instead of reading again

        # --- Load MP3 file with eyed3 ---
        logging.info("Loading MP3 metadata with eyed3.")
        audio = eyed3.load(temp_mp3_path)
        if audio is None:
            # This can happen if the file is not a valid MP3 or unsupported version
            logging.error(f"eyed3 failed to load the MP3 file: {temp_mp3_path}. It might be corrupt or not an MP3.")
            # Clean up the temp file immediately in this case
            if os.path.exists(temp_mp3_path):
                os.remove(temp_mp3_path)
            return jsonify({"error": "Failed to process the MP3 file. It might be corrupt or not a valid MP3."}), 422 # Unprocessable Entity

        if audio.tag is None:
            logging.info("No existing ID3 tag found, initializing a new one.")
            audio.initTag(version=eyed3.id3.ID3_V2_3) # Specify a version if desired
        else:
            logging.info(f"Loaded existing ID3 tag (Version: {audio.tag.version})")

        # --- Update ID3 tags ---
        # Set the ID3 version to 2.3 for better compatibility
        logging.info("Updating metadata tags...")

        # Set Cover Art
        # Remove existing front cover before adding new one to avoid duplicates
        existing_covers = [img for img in audio.tag.images if img.picture_type == eyed3.id3.frames.ImageFrame.FRONT_COVER]
        for cover in existing_covers:
            audio.tag.images.remove(cover.description)
            logging.debug("Removed existing front cover image.")
        audio.tag.images.set(eyed3.id3.frames.ImageFrame.FRONT_COVER, img_data, mime_type, u'Cover')
        logging.info("Set front cover image.")

        # Set other text tags
        title = data.get('title')
        artist = data.get('artist')
        provided_genre = data.get('genre') # Store provided genre separately

        # Set title and artist if provided
        if title:
            audio.tag.title = title
            logging.info(f"Set title: {title}")
            
        # Set artist if provided, else use the one from the MP3 file
        if artist:
            audio.tag.artist = artist
            logging.info(f"Set artist: {artist}")

        # Set album if provided
        if data.get('album'):
            audio.tag.album = data.get('album')
            logging.info(f"Set album: {data.get('album')}")

        # Set genre
        # Check if genre is provided or if classifier is available
        # Use provided genre if available, else try to classify
        # If both are available, prefer the provided genre
        # This allows the user to override the classifier if they want
        if provided_genre:
            audio.tag.genre = provided_genre
            logging.info(f"Set genre from provided value: {provided_genre}")
        elif MUSIC_CLASSIFIER_AVAILABLE:
            logging.info("Genre not provided. Attempting automatic classification...")
            try:
                # Call the classifier on the temporary MP3 file
                predicted_genre = get_music_genre(temp_mp3_path)
                if predicted_genre:
                    audio.tag.genre = predicted_genre
                    logging.info(f"Automatically classified and set genre: {predicted_genre}")
                else:
                    logging.warning("Music classifier did not return a genre.")
            except Exception as classifier_err:
                # Log the error but don't fail the whole request
                logging.error(f"Error during music classification: {classifier_err}", exc_info=True) # Log traceback
        else:
            logging.info("Genre not provided and music classifier is not available. Skipping genre tag.")

        # Set the year (recording date)
        if data.get('year'):
            try:
                audio.tag.recording_date = eyed3.core.Date(int(data.get('year'))) # Use recording_date for year
                # audio.tag.year = int(data.get('year')) # 'year' attribute might be deprecated/less standard
                logging.info(f"Set year: {data.get('year')}")
            except ValueError:
                logging.warning(f"Invalid year provided: {data.get('year')}. Skipping year tag.")

        # Set the canonical URL (WXXX frame)
        if data.get('canonical'):
            audio.tag.audio_file_url = data.get('canonical') # WXXX frame
            logging.info(f"Set canonical URL: {data.get('canonical')}")

        # Set the lyrics (USLT frame)
        lyrics_text = data.get('lyrics')
        if lyrics_text:
            try:
                # Ensure lyrics are added correctly (description='', lang=b"eng")
                # Pass lang as bytes as required by eyed3's internal validation
                audio.tag.lyrics.set(lyrics_text, description=u"", lang=b"eng")
                logging.info("Set lyrics.")
            except Exception as lyrics_err:
                # Log potential errors during lyrics setting but don't stop the process
                logging.error(f"Error setting lyrics: {lyrics_err}", exc_info=True)

        # --- Save the changes to the temporary file ---
        logging.info("Saving updated ID3 tags...")
        # Use version=eyed3.id3.ID3_V2_3 for better compatibility if needed
        audio.tag.save(version=eyed3.id3.ID3_V2_3, encoding='utf-8')
        logging.info("Tags saved successfully.")

        # --- Construct the final filename ---
        # Sanitize title and artist for use in filename
        def sanitize_filename(name):
            # Remove characters invalid for filenames in most OS
            # Keep it simple: replace common problematic chars
            if not name: return ""
            name = name.replace('/', '-').replace('\\', '-').replace(':', '-').replace('*', '-').replace('?', '').replace('"', "'").replace('<', '').replace('>', '').replace('|', '')
            return name.strip()

        safe_artist = sanitize_filename(audio.tag.artist)
        safe_title = sanitize_filename(audio.tag.title)

        if safe_artist and safe_title:
            filename = f"{safe_artist} - {safe_title}.mp3"
        elif safe_title:
            filename = f"{safe_title}.mp3"
        elif safe_artist:
            filename = f"{safe_artist} - Unknown Title.mp3"
        else:
            # Fallback to a generic name if no artist/title
            filename = f"processed_{generate_uuid(mp3_url)}.mp3"
        logging.info(f"Generated download filename: {filename}")

        # --- Queue the file for deletion ---
        with delete_lock:
            delete_queue.append({'path': temp_mp3_path, 'time': time.time()})
        logging.info(f"Scheduled temporary file for deletion: {temp_mp3_path}")

        # --- Send the modified file back to the user ---
        logging.info("Sending modified MP3 file to client...")
        return send_file(
            temp_mp3_path,
            as_attachment=True,
            download_name=filename,
            mimetype='audio/mpeg' # Explicitly set mimetype
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
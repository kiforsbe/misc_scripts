import os
import uuid
import logging
import threading
import time
from flask import Flask, request, send_file, render_template_string, jsonify
import requests
import eyed3
from io import BytesIO

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

TEMP_DIR = ".temp"
DELETE_DELAY = 300  # 5 minutes in seconds
os.makedirs(TEMP_DIR, exist_ok=True)

# Queue to store files for deletion
delete_queue = []

def delayed_delete():
    while True:
        current_time = time.time()
        files_to_delete = [f for f in delete_queue if current_time - f['time'] > DELETE_DELAY]
        
        for file_info in files_to_delete:
            try:
                os.remove(file_info['path'])
                logging.info(f"Deleted temporary file: {file_info['path']}")
                delete_queue.remove(file_info)
            except Exception as e:
                logging.error(f"Error deleting file {file_info['path']}: {e}")
        
        time.sleep(60)  # Check every minute

# Start the background deletion task
threading.Thread(target=delayed_delete, daemon=True).start()

@app.route('/', methods=['POST'])
def index():
    return render_template_string('''
        <!DOCTYPE html>
        <html>
        <head>
            <title>MP3 Metadata Editor</title>
        </head>
        <body>
            <h1>MP3 Metadata Editor</h1>
            <form action="/edit_metadata" method="post">
                <input type="text" name="mp3_url" placeholder="Enter MP3 URL" required /><br/>
                <input type="text" name="image_url" placeholder="Enter Cover Image URL" required /><br/>
                <input type="text" name="title" placeholder="Enter title" /><br/>
                <input type="text" name="artist" placeholder="Enter artist" /><br/>
                <input type="text" name="album" placeholder="Enter album" /><br/>
                <input type="text" name="genre" placeholder="Enter genre" /><br/>
                <input type="text" name="year" placeholder="Enter year" /><br/>
                <input type="submit" value="Edit Metadata">
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
    else:
        data = request.args

    mp3_url = data.get('mp3_url')
    image_url = data.get('image_url')

    if not mp3_url or not image_url:
        return jsonify({"error": "Both MP3 and image URLs are required"}), 400

    try:
        # Download MP3 file
        mp3_response = requests.get(mp3_url)
        mp3_response.raise_for_status()
        mp3_data = BytesIO(mp3_response.content)

        # Download image file
        img_response = requests.get(image_url)
        img_response.raise_for_status()
        img_data = img_response.content

        # Create a temporary file to save the MP3
        temp_mp3 = os.path.join(TEMP_DIR, f"temp_{uuid.uuid1()}.mp3")
        with open(temp_mp3, 'wb') as f:
            f.write(mp3_data.getvalue())

        # Load MP3 file with eyed3
        audio = eyed3.load(temp_mp3)
        if audio.tag is None:
            audio.initTag()

        # Add cover art
        audio.tag.images.set(3, img_data, 'image/jpeg', u'Cover')

        # Add other metadata
        title = data.get('title', '')
        artist = data.get('artist', '')

        if title:
            audio.tag.title = title
        if artist:
            audio.tag.artist = artist
        if data.get('album'):
            audio.tag.album = data.get('album')
        if data.get('genre'):
            audio.tag.genre = data.get('genre')
        if data.get('year'):
            audio.tag.year = int(data.get('year'))
        if data.get('canonical'):
            audio.tag.audio_file_url = data.get('canonical')
        if data.get('lyrics'):
            audio.tag.lyrics.set(data.get('lyrics'))

        # Save the changes
        audio.tag.save()

        # Construct the filename
        if artist and title:
            filename = f"{artist} - {title}.mp3"
        elif title:
            filename = f"{title}.mp3"
        else:
            filename = f"temp_{uuid.uuid1()}.mp3"

        # Queue the file for deletion
        delete_queue.append({'path': temp_mp3, 'time': time.time()})

        # Send the modified file back to the user
        return send_file(temp_mp3, as_attachment=True, download_name=filename)

    except requests.RequestException as e:
        logging.error(f"Error downloading files: {e}")
        return jsonify({"error": f"Error downloading files: {str(e)}"}), 500
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
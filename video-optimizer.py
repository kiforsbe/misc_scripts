import os
import re
import ffmpeg
import inquirer
import subprocess
import logging
from typing import List
from tqdm import tqdm
import time
import sys
import threading

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("transcoder.log"),
        logging.StreamHandler()
    ]
)

# Helper function to get media information using ffmpeg
def get_media_info(file_path):
    logging.info(f"Probing media info for file: {file_path}")
    try:
        probe = ffmpeg.probe(file_path)
        logging.info(f"Successfully probed media info for {file_path}")
        return probe
    except ffmpeg.Error as e:
        logging.error(f"Error probing {file_path}: {e}")
        return None

# Helper function to extract track languages and other details
def extract_track_details(probe_data, stream_type):
    logging.info(f"Extracting {stream_type} tracks from media")
    return [stream for stream in probe_data['streams'] if stream['codec_type'] == stream_type]

# Helper function to monitor the transcoding progress
def monitor_progress(process, total_duration, progress_bar):
    while process.poll() is None:
        try:
            duration = float(process.stdout.readline().decode('utf-8').strip())
            progress_bar.n = duration
            progress_bar.refresh()
        except ValueError:
            pass
    progress_bar.n = total_duration
    progress_bar.refresh()

# Helper function to transcode a file based on user settings
def transcode_file(input_file, output_file, settings, use_nvenc, apply_denoise):
    audio_bitrate = settings['audio_bitrate']
    video_bitrate = settings['video_bitrate']
    resolution = settings['resolution']
    fps = settings['fps']

    logging.info(f"Starting transcoding for {input_file}")
    logging.info(f"Target resolution: {resolution}, FPS: {fps}, Audio bitrate: {audio_bitrate}, Video bitrate: {video_bitrate}")

    # Presets and codec selection
    video_codec = 'hevc_nvenc' if use_nvenc else 'libx265'
    codec_preset = 'slow' if use_nvenc else 'medium'
    vf_options = f"scale={resolution}"

    if apply_denoise:
        vf_options += ",hqdn3d=3:2:6:4"  # Denoise filter with medium settings

    logging.info(f"Video codec: {video_codec}")
    logging.info(f"Denoise filter applied: {apply_denoise}")

    # Build the ffmpeg command
    ffmpeg_cmd = [
        'ffmpeg',
        '-i', input_file,
        '-vcodec', video_codec,
        '-acodec', 'aac',
        '-ab', audio_bitrate,
        '-vf', vf_options,
        '-pix_fmt', 'yuv420p10le',
        '-r', str(fps),
        '-preset', codec_preset,
        '-movflags', 'faststart',
        '-f', 'null', '-'  # Output progress information to stdout
    ]

    try:
        # Start the process
        process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        # Function to read from the stderr pipe
        def read_stderr(stderr, queue):
            for line in iter(stderr.readline, ''):
                queue.put(line)
            stderr.close()

        # Start thread to read stderr
        import queue
        stderr_queue = queue.Queue()
        stderr_thread = threading.Thread(target=read_stderr, args=(process.stderr, stderr_queue))
        stderr_thread.start()

        # Get the total duration from the input file
        total_duration = None
        while True:
            line = stderr_queue.get()
            if 'Duration' in line:
                match = re.search(r'Duration: (\d{2}):(\d{2}):(\d{2})\.(\d{2})', line)
                if match:
                    hours, minutes, seconds, _ = match.groups()
                    total_duration = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                    break

        if total_duration is None:
            raise Exception('Could not determine total duration.')

        # Initialize the progress bar
        pbar = tqdm(total=total_duration, unit='s')

        # Read the stderr pipe and update progress bar
        while process.poll() is None:
            try:
                line = stderr_queue.get(timeout=1)
                if 'time=' in line:
                    match = re.search(r'time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})', line)
                    if match:
                        hours, minutes, seconds, _ = match.groups()
                        elapsed_time = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                        pbar.update(elapsed_time - pbar.n)  # Update progress bar
            except queue.Empty:
                continue

        process.wait()
        pbar.close()

        logging.info(f"Transcoding complete for {input_file}. Output saved to {output_file}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error transcoding {input_file}: {e}")
        pbar.close()
        input("An error occurred. Press Enter to exit...")
        sys.exit(1)

# Function to offer user settings and transcoding options
def get_transcoding_settings():
    logging.info("Offering transcoding profile options to the user")
    profile_questions = [
        inquirer.List(
            'profile',
            message="Select the transcoding profile:",
            choices=[
                'phone (480p, 25fps, H265, 96kbps audio, 8MB/min)',
                'remote-streaming (720p, H265, 128kbps audio, 12MB/min)',
                'home-streaming (1080p, H265, 128kbps audio, 18MB/min)'
            ]
        )
    ]
    profile_answers = inquirer.prompt(profile_questions)
    profile = profile_answers['profile']
    logging.info(f"User selected profile: {profile}")

    if profile.startswith('phone'):
        return {
            'resolution': '854x480',  # Maintain aspect ratio for 480p
            'fps': 25,
            'audio_bitrate': '96k',
            'video_bitrate': 'medium',
            'target_size_mb_per_minute': 8
        }
    elif profile.startswith('remote-streaming'):
        return {
            'resolution': '1280x720',  # Maintain aspect ratio for 720p
            'fps': 30,
            'audio_bitrate': '128k',
            'video_bitrate': 'medium',
            'target_size_mb_per_minute': 12
        }
    else:
        return {
            'resolution': '1920x1080',  # Maintain aspect ratio for 1080p
            'fps': 30,
            'audio_bitrate': '128k',
            'video_bitrate': 'low',
            'target_size_mb_per_minute': 18
        }

# Function to let the user select whether to use NVENC and apply denoise
def get_encoding_and_filter_options():
    nvenc_supported = detect_nvenc_support()

    nvenc_question = inquirer.Confirm('use_nvenc', message="Use NVENC hardware encoding (if available)?", default=nvenc_supported)
    denoise_question = inquirer.Confirm('apply_denoise', message="Apply denoise filter (hqdn3d at medium settings)?", default=True)  # Set denoise option to True by default

    answers = inquirer.prompt([nvenc_question, denoise_question])

    logging.info(f"NVENC selected: {answers['use_nvenc']}")
    logging.info(f"Denoise selected: {answers['apply_denoise']}")

    return answers['use_nvenc'], answers['apply_denoise']

# Detect if NVENC is supported on the machine
def detect_nvenc_support():
    try:
        result = subprocess.run(['ffmpeg', '-hide_banner', '-encoders'], capture_output=True, text=True)
        if 'hevc_nvenc' in result.stdout:
            logging.info("NVENC hardware encoding (H265) supported")
            return True
        else:
            logging.info("NVENC hardware encoding (H265) not supported")
            return False
    except Exception as e:
        logging.error(f"Error detecting NVENC support: {e}")
        return False

# Function to let the user select default audio and subtitle tracks
def select_default_tracks(files):
    audio_languages = []
    subtitle_languages = []

    for file in files:
        media_info = get_media_info(file)
        if not media_info:
            logging.warning(f"Skipping file {file} due to failed media info probing")
            continue

        # Extract audio tracks and subtitle tracks
        audio_tracks = extract_track_details(media_info, 'audio')
        subtitle_tracks = extract_track_details(media_info, 'subtitle')

        audio_languages.extend([track['tags'].get('language', 'Unknown') for track in audio_tracks])
        subtitle_languages.extend([track['tags'].get('language', 'Unknown') for track in subtitle_tracks])

    # Get unique languages
    unique_audio_languages = list(set(audio_languages))
    unique_subtitle_languages = list(set(subtitle_languages))

    logging.info(f"Audio languages available: {unique_audio_languages}")
    logging.info(f"Subtitle languages available: {unique_subtitle_languages}")

    # Let the user select the default audio and subtitle tracks
    questions = [
        inquirer.List('audio_language', message="Select default audio language:", choices=unique_audio_languages),
        inquirer.List('subtitle_language', message="Select default subtitle language:", choices=unique_subtitle_languages)
    ]
    answers = inquirer.prompt(questions)
    logging.info(f"User selected default audio: {answers['audio_language']}, default subtitle: {answers['subtitle_language']}")
    return answers['audio_language'], answers['subtitle_language']

# Main function to handle files and transcoding
def main():
    if len(sys.argv) < 2:
        print("Drag and drop your media files onto this script to transcode them.")
        input("Press Enter to exit...")
        sys.exit(1)

    file_paths = sys.argv[1:]  # Read file paths from command-line arguments
    logging.info(f"Files received for transcoding: {file_paths}")

    # Analyze the files
    audio_language, subtitle_language = select_default_tracks(file_paths)

    # Offer transcoding settings
    settings = get_transcoding_settings()

    # Offer NVENC and denoise options
    use_nvenc, apply_denoise = get_encoding_and_filter_options()

    # Transcode each file
    for file_path in file_paths:
        output_file = os.path.splitext(file_path)[0] + "_transcoded.mp4"
        logging.info(f"Starting transcoding for {file_path} with output {output_file}")
        transcode_file(file_path, output_file, settings, use_nvenc, apply_denoise)

    input("Transcoding finished. Press Enter to exit...")

if __name__ == "__main__":
    logging.info("Starting transcoder script")
    main()
    logging.info("Transcoder script finished")
    
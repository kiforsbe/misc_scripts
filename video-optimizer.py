import os
import re
import ffmpeg
import inquirer
import subprocess
import logging
from tqdm import tqdm
import sys
import threading

# Set up logging
logging.basicConfig(
    level=logging.ERROR,
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

def truncate_filename(filename, max_length=40):
    if len(filename) > max_length:
        half_length = (max_length - 3) // 2  # Subtract 3 for the "..."
        return f"{filename[:half_length]}...{filename[-half_length:]}"
    return filename

# Helper function to transcode a file based on user settings
def transcode_file(input_file, output_file, extension, settings, use_nvenc, apply_denoise, audio_language_name, subtitle_language_name):
    audio_bitrate = settings['audio_bitrate']
    video_bitrate = settings['video_bitrate']
    resolution = settings['resolution']
    video_codec = settings['video_codec']
    codec_preset = settings['codec_preset']
    crf = settings['crf']
    fps = settings['fps']

    # Add extension to the output file
    output_file = f"{output_file}.{extension}"

    # Extract only the filename from the path
    filename = os.path.basename(input_file)
    outfilename = os.path.basename(output_file)

    # Truncate filename if it's too long
    display_filename = truncate_filename(filename)

    logging.info(f"Starting transcoding for {display_filename}")
    logging.info(f"Output path {outfilename}")
    logging.info(f"Target resolution: {resolution}, FPS: {fps}, Audio bitrate: {audio_bitrate}, Video bitrate: {video_bitrate}")

    # Print the processing file
    print(f"Processing file: {display_filename}")

    # Codec selection
    if video_codec == 'h265':
        video_codec = 'hevc_nvenc' if use_nvenc else 'libx265'
    elif video_codec == 'h264':
        video_codec = 'h264_nvenc' if use_nvenc else 'libx264'
    else:
        video_codec = 'hevc_nvenc' if use_nvenc else 'libx265'

    # Scale / resolution
    vf_options = f"scale={resolution}"

    # Apply denoising filter
    if apply_denoise:
        vf_options += ",hqdn3d=3:2:6:4"  # Denoise filter with medium settings
    
    # Probe the input file to get stream information
    probe = ffmpeg.probe(input_file)
    audio_streams = [s for s in probe['streams'] if s['codec_type'] == 'audio']
    subtitle_streams = [s for s in probe['streams'] if s['codec_type'] == 'subtitle']

    # Prepare audio mapping
    audio_index = 0
    audio_found = False
    if audio_language_name:
        for i, stream in enumerate(audio_streams):
            # Check if the language matches
            if 'tags' in stream and 'language' in stream['tags'] and stream['tags']['language'] == audio_language_name:
                audio_index = i
                audio_found = True
                logging.info(f"Default audio track set to language: {audio_language_name}")
                break

    # If no applicable audio stream is found, use the first one
    if not audio_found and len(subtitle_streams) > 0:
        logging.warning("No audio track found matching language, using first available")
        logging.info(f"Default audio track set to language: {audio_streams[audio_index]['tags']['language']}")

    # Prepare subtitle mapping
    subtitle_index = 0
    subtitle_found = False
    if subtitle_language_name:
        for i, stream in enumerate(subtitle_streams):
            # Check if the language matches
            if 'tags' in stream and 'language' in stream['tags'] and stream['tags']['language'] == subtitle_language_name:
                #subtitle_map.append(f"-map 0:s:{i}")  # Select specific subtitle track by index
                subtitle_index = i
                subtitle_found = True
                logging.info(f"Default subtitle track set to language: {subtitle_language_name}")
                break

    # If no applicable subtitle is found, use the first one
    if not subtitle_found and len(subtitle_streams) > 0:
        logging.warning("No subtitle track found matching language, using first available")
        logging.info(f"Default subtitle track set to language: {subtitle_streams[subtitle_index]['tags']['language']}")

    logging.info(f"Video codec: {video_codec}")
    logging.info(f"Denoise filter applied: {apply_denoise}")

    # Set up subtitle format, if the output extension is 'mp4' then, and only then use 'mov_ext' otherwise just copy 'copy' the source
    if extension == "mp4":
        # Use mov_ext format for mp4 output files as nothing else but srt is supported in mp4 container
        subtitle_format = "mov_text"
    else:
        # Just copy the source if the target extension is anything other than 'mp4'
        subtitle_format = "copy"

    ffmpeg_cmd = [
        'ffmpeg',
        '-i', input_file,
        '-ab', audio_bitrate,
        '-vf', vf_options,
        '-rc', 'vbr',
        '-cq', str(crf),
        '-qmin', str(int(crf*0.8)),
        '-qmax', str(int(crf*1.2)),
        '-pix_fmt', 'yuv420p10le',
        '-preset', codec_preset,
        '-movflags', 'faststart',
        '-map', '0:v',
        '-map', '0:a',
        '-map', '0:s',
        '-c:v', video_codec,
        '-c:a', 'aac',
        '-c:s', subtitle_format,
        f"-disposition:a:{audio_index}", 'default',
        f"-disposition:s:{subtitle_index}", 'default',
        '-y', output_file
    ]

    #print(' '.join(ffmpeg_cmd))

    try:
        # Start the process
        process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        # Function to read from the stderr pipe
        def read_stderr(stderr, queue):
            for line in iter(stderr.readline, ''):
                #print(line)
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
        pbar = tqdm(total=total_duration, unit='s', desc=f"Transcoding {display_filename}")

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

# Helper function to get the aspect ratio of a file
def get_aspect_ratio(file_path):
    media_info = get_media_info(file_path)
    if media_info and 'streams' in media_info:
        video_streams = [stream for stream in media_info['streams'] if stream['codec_type'] == 'video']
        if video_streams:
            width = video_streams[0]['width']
            height = video_streams[0]['height']
            return width, height
    return None

# Function to offer user settings and transcoding options
def get_transcoding_settings(file_path):
    logging.info("Offering transcoding profile options to the user")

    width, height = get_aspect_ratio(file_path)
    aspect_ratio = width / height

    # Set default resolution based on selected profile
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
        target_width = 854
        target_height = int(target_width / aspect_ratio) if aspect_ratio else 480
        return {
            'resolution': f'{target_width}x{target_height}',  # Adjusted based on aspect ratio
            'fps': 25,
            'audio_bitrate': '96k',
            'video_codec': 'h265',
            'video_bitrate': '8MB/min',
            'codec_preset': 'medium',
            'crf': 25,
        }
    elif profile.startswith('remote-streaming'):
        target_width = 1280
        target_height = int(target_width / aspect_ratio) if aspect_ratio else 720
        return {
            'resolution': f'{target_width}x{target_height}',  # Adjusted based on aspect ratio
            'fps': 30,
            'audio_bitrate': '128k',
            'video_codec': 'h265',
            'video_bitrate': '12MB/min',
            'codec_preset': 'medium',
            'crf': 20,
        }
    else:
        target_width = 1920
        target_height = int(target_width / aspect_ratio) if aspect_ratio else 1080
        return {
            'resolution': f'{target_width}x{target_height}',  # Adjusted based on aspect ratio
            'fps': 30,
            'audio_bitrate': '128k',
            'video_codec': 'h265',
            'video_bitrate': 'low',
            'codec_preset': '18MB/min',
            'crf': 15,
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

    # Determine aspect ratios from the input files
    aspect_ratios = []
    for file_path in file_paths:
        aspect_ratio = get_aspect_ratio(file_path)
        if aspect_ratio:
            aspect_ratios.append(aspect_ratio)
            logging.info(f"Aspect ratio for {file_path}: {aspect_ratio[0]}:{aspect_ratio[1]}")
        else:
            logging.warning(f"Could not determine aspect ratio for {file_path}")

    if not aspect_ratios:
        print("No valid aspect ratios found. Exiting...")
        input("Press Enter to exit...")
        sys.exit(1)

    # Select default audio and subtitle tracks
    audio_language, subtitle_language = select_default_tracks(file_paths)

    # Offer transcoding settings using the first file's aspect ratio
    first_file_path = file_paths[0]
    settings = get_transcoding_settings(first_file_path)

    # Offer NVENC and denoise options
    use_nvenc, apply_denoise = get_encoding_and_filter_options()

    # Transcode each file
    for file_path in file_paths:
        output_file = os.path.splitext(file_path)[0] + "_transcoded"
        logging.info(f"Starting transcoding for {file_path} with output {output_file}")
        transcode_file(file_path, output_file, 'mp4', settings, use_nvenc, apply_denoise, audio_language, subtitle_language)

    input("Transcoding finished. Press Enter to exit...")

if __name__ == "__main__":
    logging.info("Starting transcoder script")
    main()
    logging.info("Transcoder script finished")
    
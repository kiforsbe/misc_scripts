import os
import re
import ffmpeg
import inquirer
import subprocess
import logging
from tqdm import tqdm
import sys
import threading
from mutagen.mp4 import MP4, MP4Cover

# Set up logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("transcoder.log"),
        logging.StreamHandler()
    ]
)

# Declare some default profiles in case there is no profiles.json available
default_profiles = [
    {
        'profile_id': 'iphone_480p',
        'description': 'iPhone, 480p, H264 High Profile, AAC audio, MP4',
        'settings': {
            'horizontal_resolution': 854,
            'audio_bitrate': '128k',
            'video_codec': 'h264',
            'codec_preset': 'medium',  # Better compatibility than 'fast'
            'constant_quality': 23,
            'pix_fmt': 'yuv420p',  # Standard 8-bit color for maximum compatibility
            'profile': 'high',  # H.264 High Profile
            'level': '4.0',    # Compatible level for iOS
            'max_muxing_queue_size': 1024  # Helps with MP4 muxing
        }
    },
    {
        'profile_id': 'iphone_hevc_480p',
        'description': 'iPhone (HEVC), 480p, H265 Main Profile, AAC audio, MP4',
        'settings': {
            'horizontal_resolution': 854,
            'audio_bitrate': '128k',
            'video_codec': 'h265',
            'codec_preset': 'medium',
            'constant_quality': 28,
            'pix_fmt': 'yuv420p',
            'profile': 'main',
            'level': '4.0',
            'max_muxing_queue_size': 1024
        }
    },
    {
        'profile_id': '480p',
        'description': '480p, H265, 96kbps audio, 8MB/min',
        'settings': {
            'horizontal_resolution': 854,
            'audio_bitrate': '96k',
            'video_codec': 'h265',
            'codec_preset': 'fast',
            'constant_quality': 27,
        }
    },
    {
        'profile_id': '720p',
        'description': '720p, H265, 128kbps audio, 9MB/min',
        'settings': {
            'horizontal_resolution': 1280,
            'audio_bitrate': '96k',
            'video_codec': 'h265',
            'codec_preset': 'fast',
            'constant_quality': 27,
        }
    },
    {
        'profile_id': '1080p',
        'description': '1080p, H265, 128kbps audio, 18MB/min',
        'settings': {
            'horizontal_resolution': 1920,
            'audio_bitrate': '128k',
            'video_codec': 'h265',
            'codec_preset': 'fast',
            'constant_quality': 27,
        }
    },
    {
        'profile_id': '2160p',
        'description': '2160p, H265, 128kbps audio, 38MB/min',
        'settings': {
            'horizontal_resolution': 3840,
            'audio_bitrate': '128k',
            'video_codec': 'h265',
            'codec_preset': 'fast',
            'constant_quality': 27,
        }
    },
]

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

# Helper function to extract video tracks from media
def truncate_filename(filename, max_length=40):
    if len(filename) > max_length:
        half_length = (max_length - 3) // 2  # Subtract 3 for the "..."
        return f"{filename[:half_length]}...{filename[-half_length:]}"
    return filename

def get_aspect_ratio_corrected_resolution_string(settings, media_info):
    """
    Returns the aspect ratio corrected resolution when targeting a new horizontal resolution.

    Args:
        settings (dict): Settings dictionary containing 'horizontal_resolution'.
        media_info: Media info object containing width and height information.

    Returns:
        str: Aspect ratio corrected resolution as a string.
    """

    # Get the original resolution of the input file
    width, height = get_resolution(media_info)

    # TODO: This can be simplified: https://trac.ffmpeg.org/wiki/Scaling#KeepingtheAspectRatio
    if settings['horizontal_resolution'] == 'keep':
        # If horizontal_resolution is set to keep, return the original resolution
        # TODO: This can be simplified: https://trac.ffmpeg.org/wiki/Scaling#AvoidingUpscaling
        return f"{width}x{height}"
    else:
        # Keep aspect ratio
        target_width = settings['horizontal_resolution']
        target_height = int(height * (target_width / width))

        # Set the new resolution and vf_options
        return f"{target_width}x{target_height}"

def get_video_encoder(settings, use_nvenc):
    """
    Returns the correct video codec based on use_nvenc and settings.

    Args:
        use_nvenc (bool): Whether to use NVENC or not.
        settings (dict): A dictionary containing 'video_codec' setting.

    Returns:
        str: The name of the video codec to use.
    """

    # Mapping table for video codecs
    codec_mapping = {
        'h265': {'nvenc': 'hevc_nvenc', 'non-nvenc': 'libx265'},
        'h264': {'nvenc': 'h264_nvenc', 'non-nvenc': 'libx264'},
        'av1': {'nvenc': 'av1_nvenc', 'non-nvenc': 'libaom-av1'},
    }

    video_codec = settings.get('video_codec', '')

    # Get the corresponding NVENC or non-NVENC variant
    encoder = codec_mapping[video_codec].get('nvenc' if use_nvenc else 'non-nvenc')

    return encoder

def get_stream_index_by_language(streams, language):
    """
    Returns the index of the stream matching the specified language.

    Args:
        audio_streams (list): List of dictionaries containing stream information.
        language (str): The desired language for the stream.

    Returns:
        int: Index of the stream matching the specified language. If no match is found, returns 0.
    """
    index = -1  # Initialize index to an invalid value

    # Try to find a matching language
    if language:
        for i, stream in enumerate(streams):
            # Check if the language matches
            if 'tags' in stream and 'language' in stream['tags'] and stream['tags']['language'] == language:
                index = i  # Update index if match found
                break

    return index

def map_subtitle_for_transcode(container):
    if container == 'mp4':
            return 'mov_text'
        
    return 'copy'

# Helper function to transcode a file based on user settings
def transcode_file(input_file, output_file, extension, settings, use_nvenc, apply_denoise, audio_language_name, subtitle_language_name):
    # Add extension to the output file
    output_file = f"{output_file}.{extension}"

    # Extract only the filename from the path
    filename = os.path.basename(input_file)
    outfilename = os.path.basename(output_file)

    # Truncate filename if it's too long
    display_filename = truncate_filename(filename)

    # Print the processing file
    print(f"Processing file: {display_filename}")

    # Probe the input file to get stream information
    media_info = get_media_info(input_file)
    audio_streams = [s for s in media_info['streams'] if s['codec_type'] == 'audio']
    subtitle_streams = [s for s in media_info['streams'] if s['codec_type'] == 'subtitle']

    # Get the audio and subtitle stream index based on the prefered default language
    audio_index = get_stream_index_by_language(audio_streams, audio_language_name)
    subtitle_index = get_stream_index_by_language(subtitle_streams, subtitle_language_name)

    # Check if an appropriate default stream was found as default, and if so output some information on it, and if not, change the audio_index to 0
    if 0 <= audio_index < len(audio_streams):
        # Use the audio_index directly to find the stream information
        stream = audio_streams[audio_index]

        # Summarize audio details into a string
        details = f"Language: {stream['tags']['language']}, Codec Name: {stream['codec_name']}, Sample Rate: {stream['sample_rate']}, Channels: {stream['channels']} ({'stereo' if stream['channel_layout'] == 'stereo' else 'unknown'})"
        logging.info(f"Audio details: {details}")
    else:
        logging.warning("No matching audio stream found.")
        audio_index = 0

    # Check if an appropriate default stream was found as default, and if so output some information on it, and if not, change the subtitle_index to 0
    if 0 <= subtitle_index < len(subtitle_streams):
        # Use the subtitle_index directly to find the stream information
        stream = subtitle_streams[audio_index]

        # Summarize audio details into a string
        details = f"Language: {stream['tags']['language']}, Codec Name: {stream['codec_name']}"
        logging.info(f"Subtitle details: {details}")
    else:
        logging.warning("No matching audio stream found.")
        subtitle_index = 0

    # Encoder selection
    encoder = get_video_encoder(settings, use_nvenc)

    # Return aspect ratio corrected resolution when targeting a new horizontal resolution
    resolution = get_aspect_ratio_corrected_resolution_string(settings, media_info)
    vf_options = f"scale={resolution}"

    # Apply denoising filter to vf_options
    if apply_denoise:
        vf_options += ",hqdn3d=3:2:6:4"  # Denoise filter with medium settings

    # Set up subtitle format, if the output extension is 'mp4' then, and only then use 'mov_text' otherwise just copy 'copy' the source
    subtitle_format = map_subtitle_for_transcode(extension)

    # Set up some locals based on settings to make things easier
    constant_quality = settings['constant_quality']
    audio_bitrate = settings['audio_bitrate']
    codec_preset = settings['codec_preset']

    # Output some logging before setting up the ffmpeg command
    logging.info(f"Starting transcoding for {display_filename}")
    logging.info(f"Output path {outfilename}")
    logging.info(f"Video encoder: {encoder}, Target resolution: {resolution}, Audio bitrate: {audio_bitrate}, Quality setting: {constant_quality}, Denoise filter applied: {apply_denoise}")

    # Set up ffmpeg command with modified parameters for iPhone compatibility
    ffmpeg_cmd = [
        'ffmpeg',
        '-i', input_file,
        '-ab', audio_bitrate,
        '-vf', vf_options,
        '-rc', 'vbr',
        '-cq', str(constant_quality),
        '-pix_fmt', settings.get('pix_fmt', 'yuv420p10le'),  # Use profile-specific format if available
        '-preset', codec_preset
    ]

    # Add profile-specific parameters if they exist
    if 'profile' in settings:
        ffmpeg_cmd.extend(['-profile:v', settings['profile']])
    if 'level' in settings:
        ffmpeg_cmd.extend(['-level', settings['level']])
    if 'max_muxing_queue_size' in settings:
        ffmpeg_cmd.extend(['-max_muxing_queue_size', str(settings['max_muxing_queue_size'])])

    # Continue with existing command parameters
    ffmpeg_cmd.extend([
        '-movflags', 'faststart',
        '-map', '0:v',
        '-map', '0:a',
        '-map', '0:s',
        '-c:v', encoder,
        '-c:a', 'aac',
        '-c:s', subtitle_format,
        f"-disposition:a:{audio_index}", 'default',
        f"-disposition:s:{subtitle_index}", 'default',
        '-y', output_file
    ])

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

        # Wait until the process is finished
        process.wait()

        # Close the progress bar
        pbar.close()

        logging.info(f"Transcoding complete for {input_file}. Output saved to {output_file}")

        # After successful transcode, add metadata
        try:
            metadata = parse_filename(os.path.basename(input_file))
            if metadata:
                add_metadata(output_file, metadata, extension)
                logging.info(f"Added metadata: {metadata}")
        except Exception as e:
            logging.error(f"Failed to add metadata: {e}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Error transcoding {input_file}: {e}")
        pbar.close()
        input("An error occurred. Press Enter to exit...")
        sys.exit(1)

# Helper function to get the aspect ratio of a file
def get_resolution(media_info):
    if media_info and 'streams' in media_info:
        video_streams = [stream for stream in media_info['streams'] if stream['codec_type'] == 'video']
        if video_streams:
            width = video_streams[0]['width']
            height = video_streams[0]['height']
            return width, height
    return None

# Function to offer user settings and transcoding options
def get_transcoding_settings(profiles):
    logging.info("Offering transcoding profile options to the user")

    # Set default resolution based on selected profile
    profile_questions = [
        inquirer.List(
            'profile',
            message="Select the transcoding profile:",
            # make list of choices from default_profiles using the description and profile_id
            choices=[(profile['description'], profile['profile_id']) for profile in profiles]
        )
    ]
    profile_answers = inquirer.prompt(profile_questions)
    profile_id = profile_answers['profile']
    logging.info(f"User selected profile: {profile_id}")

    # Select the profile object from profiles based on the profile_id
    profile_object = [profile for profile in profiles if profile['profile_id'] == profile_id][0]

    # Return the selected profile settings from the profile_object
    return profile_object['settings']


# Function to let the user select whether to use NVENC and apply denoise
def get_encoding_and_filter_options():
    nvenc_supported = detect_nvenc_support()

    nvenc_question = inquirer.Confirm('use_nvenc', message="Use NVENC hardware encoding (if available)?", default=nvenc_supported)
    denoise_question = inquirer.Confirm('apply_denoise', message="Apply denoise filter (hqdn3d at medium settings)?", default=True)  # Set denoise option to True by default

    answers = inquirer.prompt([nvenc_question, denoise_question])

    logging.info(f"NVENC selected: {answers['use_nvenc']}")
    logging.info(f"Denoise selected: {answers['apply_denoise']}")

    return answers['use_nvenc'], answers['apply_denoise']

# Function to let the user select whether to use NVENC and apply denoise
def get_output_container():
    question = inquirer.List('output_container', message='Select the output container:', choices=['mp4','mkv'])
    answer = inquirer.prompt([question])

    logging.info(f"NVENC selected: {answer['output_container']}")

    return answer['output_container']

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

def parse_filename(filename):
    """Extract show information from filename with better season/episode parsing."""
    # Strip file extension
    basename = os.path.splitext(filename)[0]
    
    # Pattern for common anime/TV show naming formats:
    # [Group] Show - S01E02 or [Group] Show - 01x02 or [Group] Show - 02
    patterns = [
        # [Group] Show - S01E02
        r'\[([^\]]+)\]\s*([^-]+?)\s*-\s*S(\d+)E(\d+)(?:\s*\[[^\]]+\])*$',
        # [Group] Show - 01x02
        r'\[([^\]]+)\]\s*([^-]+?)\s*-\s*(\d+)x(\d+)(?:\s*\[[^\]]+\])*$',
        # [Group] Show - 02 (assume season 1 if no season specified)
        r'\[([^\]]+)\]\s*([^-]+?)\s*-\s*(\d+)(?:\s*\[[^\]]+\])*$'
    ]
    
    for pattern in patterns:
        match = re.match(pattern, basename)
        if match:
            if len(match.groups()) == 4:  # Format with explicit season
                release_group, show_name, season_num, episode_num = match.groups()
            else:  # Format with just episode number
                release_group, show_name, episode_num = match.groups()
                season_num = '1'  # Default to season 1
                
            try:
                season_int = int(season_num)
                episode_int = int(episode_num)
            except ValueError:
                logging.warning(f"Could not parse season/episode numbers from {filename}")
                return None
                
            return {
                'TVSHOW': show_name.strip(),
                'TVSEASON': season_int,
                'TVEPISODE': episode_int,
                'RELEASE GROUP': release_group.strip()
            }
    return None

def add_metadata(output_file, metadata, container):
    """Add metadata tags to the output file with proper Apple TV compatible tags."""
    if not metadata:
        return

    try:
        # Extract cover image first
        cover_image_path = extract_cover_image(output_file)
        
        if container == 'mp4':
            video = MP4(output_file)
            # Map to MP4 tags with proper Apple TV compatibility
            # stik=10 indicates TV Show content type for Apple TV
            video['\xa9nam'] = metadata.get('TVSHOW', '')    # Show name as title
            video['tvsh'] = metadata.get('TVSHOW', '')       # TV Show name
            video['tvsn'] = [metadata.get('TVSEASON', 1)]    # TV Season number
            video['tves'] = [metadata.get('TVEPISODE', 1)]   # TV Episode number
            video['stik'] = [10]                             # Content type = TV Show
            video['hdvd'] = [1]                             # HD flag
            # Set episode title as "Show - SxxEyy"
            episode_title = f"{metadata.get('TVSHOW', '')} - S{metadata.get('TVSEASON', 1):02d}E{metadata.get('TVEPISODE', 1):02d}"
            video['desc'] = [episode_title]                  # Description/summary
            
            # Add cover art if available
            if cover_image_path and os.path.exists(cover_image_path):
                with open(cover_image_path, 'rb') as f:
                    cover_data = f.read()
                    # Add as both PNG and JPEG for maximum compatibility
                    video['covr'] = [MP4Cover(cover_data, imageformat=MP4Cover.FORMAT_JPEG)]
                # Clean up the temporary cover image file
                try:
                    os.remove(cover_image_path)
                except Exception as e:
                    logging.warning(f"Failed to clean up cover image: {e}")
            
            video.save()
        
        elif container == 'mkv':
            # For MKV, use mkvpropedit command line tool
            cmd = ['mkvpropedit', output_file, '--edit', 'info']
            # Set tags in a format compatible with MKV
            if 'TVSHOW' in metadata and 'TVEPISODE' in metadata:
                episode_title = f"{metadata['TVSHOW']} - S{metadata['TVSEASON']:02d}E{metadata['TVEPISODE']:02d}"
                cmd.extend(['--set', f'title={episode_title}'])
                
                # Add cover art if available
                if cover_image_path and os.path.exists(cover_image_path):
                    cmd.extend(['--attachment-mime-type', 'image/jpeg'])
                    cmd.extend(['--add-attachment', cover_image_path])
                    
            subprocess.run(cmd, check=True)
            
            # Clean up the temporary cover image file for MKV case
            if cover_image_path and os.path.exists(cover_image_path):
                try:
                    os.remove(cover_image_path)
                except Exception as e:
                    logging.warning(f"Failed to clean up cover image: {e}")
            
    except Exception as e:
        logging.error(f"Failed to add metadata: {e}")
        # Clean up the temporary cover image file in case of error
        if cover_image_path and os.path.exists(cover_image_path):
            try:
                os.remove(cover_image_path)
            except Exception as e:
                logging.warning(f"Failed to clean up cover image: {e}")

def extract_cover_image(input_file):
    """Extract a frame at 20% duration to use as cover art."""
    try:
        # Get video duration first
        probe = ffmpeg.probe(input_file)
        duration = float(probe['format']['duration'])
        
        # Calculate timestamp at 20% of duration
        timestamp = duration * 0.2
        
        # Create temporary file path for the cover image
        temp_cover = os.path.splitext(input_file)[0] + "_cover.jpg"
        
        # Extract the frame using ffmpeg
        ffmpeg_cmd = [
            'ffmpeg',
            '-ss', str(timestamp),  # Seek to 20% position
            '-i', input_file,
            '-vframes', '1',        # Extract exactly one frame
            '-q:v', '2',           # High quality JPEG
            '-y',                  # Overwrite if exists
            temp_cover
        ]
        
        subprocess.run(ffmpeg_cmd, check=True, capture_output=True)
        
        return temp_cover
    except Exception as e:
        logging.error(f"Failed to extract cover image: {e}")
        return None

# Main function to handle files and transcoding
def main():
    if len(sys.argv) < 2:
        print("Drag and drop your media files onto this script to transcode them.")
        input("Press Enter to exit...")
        sys.exit(1)

    file_paths = sys.argv[1:]  # Read file paths from command-line arguments
    logging.info(f"Files received for transcoding: {file_paths}")

    # Select default audio and subtitle tracks
    audio_language, subtitle_language = select_default_tracks(file_paths)

    # Offer transcoding settings using the first file's aspect ratio
    settings = get_transcoding_settings(default_profiles)

    # Offer NVENC and denoise options
    use_nvenc, apply_denoise = get_encoding_and_filter_options()

    # Select output container / extension
    extension = get_output_container()

    # Transcode each file
    for file_path in file_paths:
        output_file = os.path.splitext(file_path)[0] + "_transcoded"
        logging.info(f"Starting transcoding for {file_path} with output {output_file}")
        transcode_file(file_path, output_file, extension, settings, use_nvenc, apply_denoise, audio_language, subtitle_language)

    input("Transcoding finished. Press Enter to exit...")

if __name__ == "__main__":
    logging.info("Starting transcoder script")
    main()
    logging.info("Transcoder script finished")

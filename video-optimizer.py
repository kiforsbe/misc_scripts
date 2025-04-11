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
import requests
import json
from datetime import datetime, timedelta
from rapidfuzz import fuzz, process
from pathlib import Path
from typing import Dict, Any, List, Optional
from metadata_provider import MetadataManager, EpisodeInfo, TitleInfo
from anime_metadata import AnimeDataProvider
from imdb_metadata import IMDbDataProvider

# Initialize metadata manager as a global variable
METADATA_MANAGER = None

def get_metadata_manager():
    """Get or initialize the metadata manager"""
    global METADATA_MANAGER
    if (METADATA_MANAGER is None):
        # Initialize providers
        anime_provider = AnimeDataProvider()
        imdb_provider = IMDbDataProvider()
        METADATA_MANAGER = MetadataManager([anime_provider, imdb_provider])
    return METADATA_MANAGER

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
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
            'max_muxing_queue_size': 1024,  # Helps with MP4 muxing
            'movflags': '+faststart+use_metadata_tags'
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
            'pix_fmt': 'yuv420p',  # iPhone requires 8-bit color for compatibility
            'profile': 'main',    # HEVC Main profile is well supported
            'level': '4.1',      # Common HEVC level for mobile
            'max_muxing_queue_size': 1024,
            'movflags': '+faststart+use_metadata_tags',
            'tag:v': 'hvc1',     # Essential for Apple device compatibility
            'brand': 'mp42,iso6,isom,msdh,dby1'  # Compatible brands for iOS
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
    if (container == 'mp4'):
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

    # Check if an appropriate default stream was found as default, and if not, change the audio_index to 0
    if 0 <= audio_index < len(audio_streams):
        # Use the audio_index directly to find the stream information
        stream = audio_streams[audio_index]

        # Summarize audio details into a string
        details = f"Language: {stream['tags']['language']}, Codec Name: {stream['codec_name']}, Sample Rate: {stream['sample_rate']}, Channels: {stream['channels']} ({'stereo' if stream['channel_layout'] == 'stereo' else 'unknown'})"
        logging.info(f"Audio details: {details}")
    else:
        logging.warning("No matching audio stream found.")
        audio_index = 0

    # Check if an appropriate default stream was found as default, and if not, change the subtitle_index to 0
    if 0 <= subtitle_index < len(subtitle_streams):
        # Use the subtitle_index directly to find the stream information
        stream = subtitle_streams[audio_index]

        # Summarize subtitle details into a string
        details = f"Language: {stream['tags']['language']}, Codec Name: {stream['codec_name']}"
        logging.info(f"Subtitle details: {details}")
    else:
        logging.warning("No matching subtitle stream found.")
        subtitle_index = 0

    # Encoder selection
    encoder = get_video_encoder(settings, use_nvenc)

    # Return aspect ratio corrected resolution when targeting a new horizontal resolution
    resolution = get_aspect_ratio_corrected_resolution_string(settings, media_info)
    vf_options = f"scale={resolution}"

    # Apply denoising filter to vf_options
    if apply_denoise:
        vf_options += ",hqdn3d=3:2:6:4"  # Denoise filter with medium settings

    # Set up subtitle format
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
    if 'profile' in settings:
        ffmpeg_cmd.extend(['-profile:v', settings['profile']])
    
    # Add brand for Apple compatibility if using NVENC
    if settings.get('video_codec') == 'h265' and use_nvenc:
        ffmpeg_cmd.extend(['-tag:v', 'hvc1'])

    # Add stream mapping and codec selection
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

        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, ffmpeg_cmd)

        logging.info(f"Transcoding complete for {input_file}. Output saved to {output_file}")

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

def select_default_tracks(files):
    """Select default audio and subtitle tracks, with languages sorted alphabetically"""
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

        # Add languages while maintaining uniqueness
        for track in audio_tracks:
            lang = track['tags'].get('language', 'Unknown')
            if lang not in audio_languages:
                audio_languages.append(lang)
        
        for track in subtitle_tracks:
            lang = track['tags'].get('language', 'Unknown')
            if lang not in subtitle_languages:
                subtitle_languages.append(lang)

    # Sort languages alphabetically, but keep 'Unknown' at the end if present
    audio_languages.sort(key=lambda x: ('zzz' if x == 'Unknown' else x.lower()))
    subtitle_languages.sort(key=lambda x: ('zzz' if x == 'Unknown' else x.lower()))

    logging.info(f"Audio languages available (alphabetically): {audio_languages}")
    logging.info(f"Subtitle languages available (alphabetically): {subtitle_languages}")

    # Let the user select the default audio and subtitle tracks
    questions = [
        inquirer.List('audio_language', 
                     message="Select default audio language:", 
                     choices=audio_languages,
                     default=audio_languages[0] if audio_languages else None),
        inquirer.List('subtitle_language', 
                     message="Select default subtitle language:", 
                     choices=subtitle_languages,
                     default=subtitle_languages[0] if subtitle_languages else None)
    ]
    answers = inquirer.prompt(questions)
    logging.info(f"User selected default audio: {answers['audio_language']}, default subtitle: {answers['subtitle_language']}")
    return answers['audio_language'], answers['subtitle_language']

def parse_filename(filename):
    """Extract metadata from filename using unified metadata interface."""
    basename = os.path.splitext(filename)[0]
    metadata_manager = get_metadata_manager()
    
    # Updated patterns to handle various filename formats
    patterns = [
        # Modern streaming format: Show.Year.S01E02.Title.Quality.Encoding-Group
        r'^([\w\.]*)\.(\d{4})\.S(\d+)E(\d+)\.([^.]+(?:\.[^.]+)*)\.([^-]*)-(.+)$',
        # Movie format: Movie.Name.Year.Quality.Encoding-Group
        r'^([\w\.]*)\.(\d{4})\.([^-]+)-(.+)$',
        # [Group] Show Title - Episode [Quality][Tags][CRC]
        r'\[([^\]]+)\]\s*([^-]+(?:\s*-\s*[^-]+)*)\s*-\s*(\d+)(?:\s*\[[^\]]+\])*$',
        # [Group] Show - S01E02 (standard format)
        r'\[([^\]]+)\]\s*([^-]+?)\s*-\s*S(\d+)E(\d+)(?:\s*\[[^\]]+\])*$',
        # [Group] Show - 01x02
        r'\[([^\]]+)\]\s*([^-]+?)\s*-\s*(\d+)x(\d+)(?:\s*\[[^\]]+\])*$'
    ]
    
    for pattern in patterns:
        match = re.match(pattern, basename)
        if match:
            groups = match.groups()
            
            # Extract common metadata fields
            release_group = None
            quality = None
            
            # Modern streaming TV show format
            if len(groups) == 7:
                show_title_raw, year, season_num, episode_num, episode_title, quality, release_group = groups
                title = show_title_raw.replace('.', ' ').strip()
                try:
                    year_int = int(year)
                    season_int = int(season_num)
                    episode_int = int(episode_num)
                    quality_info = quality.replace('.', ' ')
                except ValueError:
                    logging.warning(f"Could not parse numbers from {filename}")
                    return None
                
                # Find title in metadata databases
                title_info, provider = metadata_manager.find_title(title, year_int)
                if title_info:
                    episode_info = metadata_manager.get_episode_info(provider, title_info.id, season_int, episode_int)
                    
                    if episode_info:
                        return {
                            'MEDIA TYPE': title_info.type,
                            'TITLE': title_info.title,
                            'TVSHOW': title_info.title,
                            'TVSEASON': season_int,
                            'TVEPISODE': episode_int,
                            'EPISODE TITLE': episode_info.title or episode_title.replace('.', ' ').strip(),
                            'RELEASE GROUP': release_group.strip(),
                            'YEAR': year_int,
                            'QUALITY': quality_info,
                            'RATING': title_info.rating,
                            'EPISODE RATING': episode_info.rating,
                            'VOTES': title_info.votes,
                            'EPISODE VOTES': episode_info.votes,
                            'GENRES': title_info.genres,
                            'TAGS': title_info.tags,
                            'STATUS': title_info.status,
                            'TOTAL EPISODES': title_info.total_episodes,
                            'TOTAL SEASONS': title_info.total_seasons,
                            'START YEAR': title_info.start_year,
                            'END YEAR': title_info.end_year,
                            'SOURCES': title_info.sources
                        }
            
            # Movie format
            elif len(groups) == 4:
                movie_title_raw, year, quality, release_group = groups
                title = movie_title_raw.replace('.', ' ').strip()
                
                try:
                    year_int = int(year)
                    quality_info = quality.replace('.', ' ')
                except ValueError:
                    logging.warning(f"Could not parse year from {filename}")
                    return None
                
                # Find movie in metadata databases
                title_info, provider = metadata_manager.find_title(title, year_int)
                if title_info:
                    return {
                        'MEDIA TYPE': title_info.type,
                        'TITLE': title_info.title,
                        'YEAR': year_int,
                        'QUALITY': quality_info,
                        'RELEASE GROUP': release_group.strip(),
                        'RATING': title_info.rating,
                        'VOTES': title_info.votes,
                        'GENRES': title_info.genres,
                        'TAGS': title_info.tags,
                        'STATUS': title_info.status,
                        'SOURCES': title_info.sources
                    }
            
            # Anime-style episode formats
            else:
                release_group = groups[0]
                show_title = groups[1].strip()
                
                if len(groups) == 3:  # Simple episode format
                    season_int = 1  # Default season
                    try:
                        episode_int = int(groups[2])
                    except ValueError:
                        logging.warning(f"Could not parse episode number from {filename}")
                        return None
                else:  # Standard S01E02 or 01x02 format
                    try:
                        season_int = int(groups[2])
                        episode_int = int(groups[3])
                    except ValueError:
                        logging.warning(f"Could not parse season/episode numbers from {filename}")
                        return None
                
                # Find title in metadata databases
                title_info, provider = metadata_manager.find_title(show_title)
                if title_info:
                    episode_info = metadata_manager.get_episode_info(provider, title_info.id, season_int, episode_int)
                    
                    if episode_info:
                        return {
                            'MEDIA TYPE': title_info.type,
                            'TITLE': title_info.title,
                            'TVSHOW': title_info.title,
                            'TVSEASON': season_int,
                            'TVEPISODE': episode_int,
                            'EPISODE TITLE': episode_info.title,
                            'RELEASE GROUP': release_group.strip(),
                            'YEAR': title_info.year,
                            'RATING': title_info.rating,
                            'EPISODE RATING': episode_info.rating,
                            'VOTES': title_info.votes,
                            'EPISODE VOTES': episode_info.votes,
                            'GENRES': title_info.genres,
                            'TAGS': title_info.tags,
                            'STATUS': title_info.status,
                            'TOTAL EPISODES': title_info.total_episodes,
                            'TOTAL SEASONS': title_info.total_seasons,
                            'START YEAR': title_info.start_year,
                            'END YEAR': title_info.end_year,
                            'SOURCES': title_info.sources
                        }
    
    return None

class VideoMetadata:
    def __init__(self, filename: str, metadata: Dict[str, Any]):
        self.filename = filename
        self.metadata = metadata
        self.cover_image_path = None

def gather_metadata(files: List[str]) -> List[VideoMetadata]:
    """Gather metadata for all files before processing"""
    logging.info("Gathering metadata for all files...")
    metadata_list = []
    
    for file_path in files:
        filename = os.path.basename(file_path)
        metadata = parse_filename(filename)
        metadata_list.append(VideoMetadata(file_path, metadata or {}))
    
    return metadata_list

def display_metadata_preview(metadata_list: List[VideoMetadata]):
    """Display a preview of all files and their metadata in a concise single-line format"""
    print("\nFiles to be processed:")
    print("=" * 120)
    
    for video_meta in metadata_list:
        filename = os.path.basename(video_meta.filename)
        
        if not video_meta.metadata:
            print("No metadata could be extracted")
            continue
            
        # Create concise metadata line
        show_title = video_meta.metadata.get('ANIME DB TITLE', video_meta.metadata.get('TVSHOW', 'Unknown'))
        show_type = video_meta.metadata.get('SHOW TYPE', 'Unknown')
        season = video_meta.metadata.get('TVSEASON', 1)
        episode = video_meta.metadata.get('TVEPISODE', 1)
        total_episodes = video_meta.metadata.get('TOTAL EPISODES', '?')
        status = video_meta.metadata.get('STATUS', 'Unknown')
        release_group = video_meta.metadata.get('RELEASE GROUP', 'Unknown')
        
        metadata_line = f"{show_title}, {show_type}, S{season:02d}E{episode:02d}/{total_episodes}, {status}, {release_group}"
        print(metadata_line)
    
    print("=" * 120)

def add_metadata(output_file: str, video_meta: VideoMetadata, container: str):
    """Add metadata tags to the output file with proper Apple TV compatible tags."""
    if not video_meta.metadata:
        logging.warning(f"No metadata available for {output_file}")
        return

    try:
        # Extract cover image after transcoding is complete
        cover_image_path = extract_cover_image(output_file)
        
        if container == 'mp4':
            try:
                video = MP4(output_file)
                # Map to MP4 tags with proper Apple TV compatibility
                show_title = video_meta.metadata.get('ANIME DB TITLE', video_meta.metadata.get('TVSHOW', ''))
                season = video_meta.metadata.get('TVSEASON', 1)
                episode = video_meta.metadata.get('TVEPISODE', 1)
                episode_title = f"Episode {episode}"
                
                video['\xa9nam'] = episode_title
                video['tvsh'] = show_title if show_title else "Unknown Show"
                video['tvsn'] = [season]
                video['tves'] = [episode]
                video['stik'] = [10]  # Content type = TV Show
                video['hdvd'] = [1]   # HD flag
                
                # Create a rich description including anime-specific metadata
                description_parts = []
                if video_meta.metadata.get('SHOW TYPE'):
                    description_parts.append(f"Type: {video_meta.metadata['SHOW TYPE']}")
                if video_meta.metadata.get('TOTAL EPISODES'):
                    description_parts.append(f"Total Episodes: {video_meta.metadata['TOTAL EPISODES']}")
                if video_meta.metadata.get('STATUS'):
                    description_parts.append(f"Status: {video_meta.metadata['STATUS']}")
                if video_meta.metadata.get('TAGS'):
                    description_parts.append(f"Tags: {', '.join(video_meta.metadata['TAGS'][:5])}")
                
                description = " | ".join(description_parts)
                if description:
                    video['\xa9cmt'] = description
                    video['desc'] = [description]
                
                # Add genre tags from anime database
                if video_meta.metadata.get('TAGS'):
                    video['\xa9gen'] = video_meta.metadata['TAGS'][:5]
                
                # Add cover art if available
                if cover_image_path and os.path.exists(cover_image_path):
                    with open(cover_image_path, 'rb') as f:
                        cover_data = f.read()
                        video['covr'] = [MP4Cover(cover_data, imageformat=MP4Cover.FORMAT_JPEG)]
                
                # Save changes
                video.save()
                logging.info(f"Successfully saved MP4 metadata for {output_file}")
                
            except Exception as mp4_error:
                logging.error(f"Failed to save MP4 metadata for {output_file}: {mp4_error}")
                raise
            
        elif container == 'mkv':
            try:
                # Create XML tags file for MKV
                tags_file = output_file + "_tags.xml"
                with open(tags_file, 'w', encoding='utf-8') as f:
                    f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                    f.write('<Tags>\n')
                    f.write('  <Tag>\n')
                    f.write('    <Targets>\n')
                    f.write('      <TargetTypeValue>50</TargetTypeValue>\n')
                    f.write('    </Targets>\n')
                    
                    # Write each piece of metadata as separate Simple tags
                    show_title = video_meta.metadata.get('ANIME DB TITLE', video_meta.metadata.get('TVSHOW', ''))
                    season = video_meta.metadata.get('TVSEASON', 1)
                    episode = video_meta.metadata.get('TVEPISODE', 1)
                    
                    if show_title:
                        episode_title = f"{show_title} - S{season:02d}E{episode:02d}"
                        f.write('    <Simple>\n')
                        f.write('      <Name>TITLE</Name>\n')
                        f.write(f'      <String>{episode_title}</String>\n')
                        f.write('    </Simple>\n')
                        
                        f.write('    <Simple>\n')
                        f.write('      <Name>SERIES</Name>\n')
                        f.write(f'      <String>{show_title}</String>\n')
                        f.write('    </Simple>\n')
                    
                    # Add season and episode numbers
                    f.write('    <Simple>\n')
                    f.write('      <Name>SEASON</Name>\n')
                    f.write(f'      <String>{season}</String>\n')
                    f.write('    </Simple>\n')
                    
                    f.write('    <Simple>\n')
                    f.write('      <Name>EPISODE</Name>\n')
                    f.write(f'      <String>{episode}</String>\n')
                    f.write('    </Simple>\n')
                    
                    # Add anime-specific metadata
                    if video_meta.metadata.get('SHOW TYPE'):
                        f.write('    <Simple>\n')
                        f.write('      <Name>SHOW_TYPE</Name>\n')
                        f.write(f'      <String>{video_meta.metadata["SHOW TYPE"]}</String>\n')
                        f.write('    </Simple>\n')
                    
                    if video_meta.metadata.get('TOTAL EPISODES'):
                        f.write('    <Simple>\n')
                        f.write('      <Name>TOTAL_EPISODES</Name>\n')
                        f.write(f'      <String>{video_meta.metadata["TOTAL EPISODES"]}</String>\n')
                        f.write('    </Simple>\n')
                    
                    if video_meta.metadata.get('STATUS'):
                        f.write('    <Simple>\n')
                        f.write('      <Name>STATUS</Name>\n')
                        f.write(f'      <String>{video_meta.metadata["STATUS"]}</String>\n')
                        f.write('    </Simple>\n')
                    
                    if video_meta.metadata.get('TAGS'):
                        f.write('    <Simple>\n')
                        f.write('      <Name>TAGS</Name>\n')
                        f.write(f'      <String>{", ".join(video_meta.metadata["TAGS"][:5])}</String>\n')
                        f.write('    </Simple>\n')
                    
                    f.write('  </Tag>\n')
                    f.write('</Tags>\n')
                
                # Add tags file to MKV using mkvpropedit
                cmd = ['mkvpropedit', output_file, '--tags', 'global:' + tags_file]
                
                # Add cover art if available
                if cover_image_path and os.path.exists(cover_image_path):
                    cmd.extend(['--attachment-name', 'cover.jpg'])
                    cmd.extend(['--attachment-mime-type', 'image/jpeg'])
                    cmd.extend(['--add-attachment', cover_image_path])
                
                # Run mkvpropedit and capture any errors
                result = subprocess.run(cmd, check=True, capture_output=True, text=True)
                if result.stderr:
                    logging.warning(f"MKVPropedit output for {output_file}: {result.stderr}")
                else:
                    logging.info(f"Successfully saved MKV metadata for {output_file}")
                
            except subprocess.CalledProcessError as mkv_error:
                logging.error(f"Failed to save MKV metadata for {output_file}: {mkv_error}")
                if mkv_error.stderr:
                    logging.error(f"MKVPropedit error output: {mkv_error.stderr}")
                raise
            except Exception as mkv_error:
                logging.error(f"Unexpected error saving MKV metadata for {output_file}: {mkv_error}")
                raise
            finally:
                # Clean up temporary files
                if 'tags_file' in locals() and os.path.exists(tags_file):
                    try:
                        os.remove(tags_file)
                    except Exception as e:
                        logging.warning(f"Failed to clean up tags file: {e}")
    
    except Exception as e:
        logging.error(f"Failed to add metadata to {output_file}: {e}")
    finally:
        # Clean up cover image in all cases
        if 'cover_image_path' in locals() and cover_image_path and os.path.exists(cover_image_path):
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

def initialize_metadata_providers():
    """Initialize metadata providers with progress indication"""
    print("\nInitializing metadata providers...")
    
    metadata_manager = get_metadata_manager()
    if metadata_manager and metadata_manager.providers:
        # Pre-load the databases to show progress upfront
        with tqdm(total=len(metadata_manager.providers), desc="Loading metadata databases") as pbar:
            for provider in metadata_manager.providers:
                provider_name = provider.__class__.__name__.replace('DataProvider', '')
                pbar.set_description(f"Loading {provider_name} database")
                if hasattr(provider, 'load_database'):
                    provider.load_database()
                elif hasattr(provider, 'load_datasets'):
                    provider.load_datasets()
                pbar.update(1)
    
    return metadata_manager

# Main function to handle files and transcoding
def main():
    if len(sys.argv) < 2:
        print("Drag and drop your media files onto this script to transcode them.")
        input("Press Enter to exit...")
        sys.exit(1)

    file_paths = sys.argv[1:]
    logging.info(f"Files received for transcoding: {file_paths}")

    # Initialize metadata providers with progress indication
    metadata_manager = initialize_metadata_providers()

    # Gather metadata for all files upfront
    metadata_list = gather_metadata(file_paths)

    # Select default audio and subtitle tracks
    audio_language, subtitle_language = select_default_tracks(file_paths)

    # Offer transcoding settings using the first file's aspect ratio
    settings = get_transcoding_settings(default_profiles)

    # Offer NVENC and denoise options
    use_nvenc, apply_denoise = get_encoding_and_filter_options()

    # Select output container / extension
    extension = get_output_container()

    # Display metadata preview before processing
    display_metadata_preview(metadata_list)

    # Transcode each file and immediately apply metadata
    for video_meta in metadata_list:
        output_file = os.path.splitext(video_meta.filename)[0] + "_transcoded"
        logging.info(f"Starting transcoding for {video_meta.filename}")
        
        # First transcode the file
        transcode_file(video_meta.filename, output_file, extension, settings, use_nvenc, apply_denoise, audio_language, subtitle_language)
        
        # Immediately apply metadata after transcoding is complete
        final_output = f"{output_file}.{extension}"
        if os.path.exists(final_output):
            logging.info(f"Applying metadata to {final_output}")
            add_metadata(final_output, video_meta, extension)
            logging.info(f"Completed processing {video_meta.filename}")
        else:
            logging.error(f"Transcoded file not found: {final_output}")

    input("Transcoding finished. Press Enter to exit...")

if __name__ == "__main__":
    logging.info("Starting transcoder script")
    main()
    logging.info("Transcoder script finished")

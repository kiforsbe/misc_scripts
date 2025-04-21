from transformers import pipeline
import librosa
import argparse
import torch
import random
import ffmpeg # ffmpeg-python library
import tempfile
import os
import sys
import warnings
import logging # Import the logging library

# --- Configure Logging ---
# Log to stderr
# Set level to INFO to see general progress messages
# Include timestamp, level name, and message in the log format
logging.basicConfig(
    level=logging.WARN,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr # Explicitly direct logs to stderr
)

# --- Constants ---
TARGET_SR = 16000
MAX_DURATION_SECONDS = 15
MODEL_NAME = "mtg-upf/discogs-maest-30s-pw-73e-ts"

# --- Global Variables for Lazy Loading ---
_pipeline = None
_device = None
_device_name = "Unknown"

# --- Helper Functions ---

def _get_device():
    """Determines the appropriate device (CUDA or CPU) and stores it."""
    global _device, _device_name
    if _device is None:
        if torch.cuda.is_available():
            _device = 0 # Use GPU 0
            _device_name = f"CUDA ({torch.cuda.get_device_name(0)})"
            logging.info(f"CUDA available. Using device: {_device_name}")
        else:
            _device = -1 # Use CPU
            _device_name = "CPU"
            logging.info("CUDA not available. Using device: CPU.")
    return _device

def _init_pipeline():
    """Initializes the classification pipeline lazily."""
    global _pipeline
    if _pipeline is None:
        device_id = _get_device()
        try:
            logging.info(f"Initializing audio classification pipeline ({MODEL_NAME}) on {_device_name}...")
            # Suppress specific Hugging Face warnings if desired via logging level later
            # warnings.filterwarnings("ignore", message=".*Using default sampling rate.*")
            # warnings.filterwarnings("ignore", message=".*is shorter than 30s.*")
            _pipeline = pipeline(
                "audio-classification",
                model=MODEL_NAME,
                device=device_id,
                use_safetensors=True, # Use safetensors if available
                trust_remote_code=True, # Trust remote code for model loading
            )
            logging.info("Pipeline initialized successfully.")
        except Exception as e:
            logging.exception(f"Error initializing Hugging Face pipeline: {e}") # Use logging.exception to include traceback
            logging.error("Please ensure you have 'torch' and 'transformers' installed correctly.")
            if _device == 0:
                logging.error("If using GPU, ensure CUDA drivers and toolkit are compatible with your PyTorch installation.")
            _pipeline = None # Ensure it's None if init fails
    return _pipeline

def list_audio_tracks(file_path):
    """Lists available audio tracks in a media file using ffmpeg."""
    logging.info(f"Probing audio tracks for: {file_path}")
    try:
        probe = ffmpeg.probe(file_path)
        audio_streams = [s for s in probe.get('streams', []) if s.get('codec_type') == 'audio']

        if not audio_streams:
            logging.warning("No audio streams found in this file.") # Use warning level
            # Still print to stdout for this specific user action
            print("No audio streams found in this file.")
            return False

        print("\nAvailable audio tracks:")
        print("-" * 25)
        for stream in audio_streams:
            index = stream.get('index', 'N/A')
            codec = stream.get('codec_name', 'N/A')
            lang_tags = stream.get('tags', {})
            lang = lang_tags.get('language', lang_tags.get('LANGUAGE', 'N/A'))
            channels = stream.get('channels', 'N/A')
            channel_layout = stream.get('channel_layout', 'N/A')
            bit_rate_kb = int(stream.get('bit_rate', 0)) // 1000 if stream.get('bit_rate') else 'N/A'
            sample_rate = stream.get('sample_rate', 'N/A')

            print(f"  Track Index: {index}")
            print(f"    Codec:       {codec}")
            print(f"    Language:    {lang}")
            print(f"    Channels:    {channels} ({channel_layout})")
            print(f"    Sample Rate: {sample_rate} Hz")
            print(f"    Bitrate:     {bit_rate_kb} kb/s" if bit_rate_kb != 'N/A' else "    Bitrate:     N/A")
            print("-" * 25)
        return True
    except ffmpeg.Error as e:
        err_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e) # Decode stderr safely
        logging.error(f"Error probing file with ffmpeg: {err_msg}")
        logging.error("Please ensure ffmpeg is installed and in your system's PATH.")
        return False
    except Exception as e:
        logging.exception(f"An unexpected error occurred during probing: {e}") # Log exception with traceback
        return False

def load_audio_segment(file_path, target_sr=TARGET_SR, max_duration=MAX_DURATION_SECONDS, track_index=None):
    """
    Loads a random segment of audio from a file, handling track selection.

    Args:
        file_path (str): Path to the audio or video file.
        target_sr (int): Target sample rate.
        max_duration (int): Maximum duration of the segment in seconds.
        track_index (int, optional): The specific audio track index to load (from ffmpeg probe).
                                     Defaults to the first available audio track if None.

    Returns:
        tuple: (numpy.ndarray, int) containing the audio data and sample rate, or (None, None) on error.
    """
    temp_audio_file = None
    input_path_for_librosa = file_path
    selected_stream_map = None # For ffmpeg extraction

    try:
        # 1. Probe the file to get stream info and validate track_index
        logging.info(f"Probing file details: {file_path}")
        probe = ffmpeg.probe(file_path)
        audio_streams = [s for s in probe.get('streams', []) if s.get('codec_type') == 'audio']

        if not audio_streams:
            logging.error(f"No audio streams found in '{file_path}'.")
            return None, None

        valid_indices = [s['index'] for s in audio_streams]
        default_stream_index = audio_streams[0]['index'] # Use the first audio stream by default

        if track_index is not None:
            if track_index not in valid_indices:
                logging.error(f"Invalid track index {track_index}. Available indices: {valid_indices}")
                logging.error("Use the --list-tracks option to see available tracks.")
                return None, None
            selected_stream_index = track_index
            logging.info(f"User selected audio track index: {selected_stream_index}")
        else:
            selected_stream_index = default_stream_index
            if len(audio_streams) > 1:
                logging.info(f"Multiple audio tracks found. Using default track index: {selected_stream_index}")
            else:
                logging.info(f"Using audio track index: {selected_stream_index}")

        # Map specifier for ffmpeg (e.g., '0:a:0' or '0:3' if index is 3)
        selected_stream_map = f'0:{selected_stream_index}'

        # 2. Extract the selected audio track using ffmpeg if necessary
        # We extract to a temporary WAV file for reliable loading with librosa,
        # especially for complex formats or specific track selection.
        logging.info(f"Extracting audio track (index {selected_stream_index}) to temporary WAV file...")
        # Create a temporary file that will be deleted automatically on context exit if possible,
        # but manage deletion manually due to potential errors.
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmpfile:
            temp_audio_file = tmpfile.name

        try:
            # Extract using ffmpeg-python: force mono, target sample rate, 16-bit PCM WAV
            process = (
                ffmpeg
                .input(file_path)
                .output(temp_audio_file,
                        map=selected_stream_map, # Select the specific stream
                        acodec='pcm_s16le',      # Output codec: 16-bit PCM
                        ac=1,                    # Output channels: 1 (mono)
                        ar=target_sr,            # Output sample rate
                        **{'loglevel': 'error'}  # Suppress verbose ffmpeg output
                    )
                .overwrite_output()
                .run_async(pipe_stderr=True) # Run async to capture stderr
            )
            _, stderr = process.communicate() # Wait for completion and get stderr
            if process.returncode != 0:
                raise ffmpeg.Error('ffmpeg', stdout=None, stderr=stderr) # Raise error if ffmpeg failed

            input_path_for_librosa = temp_audio_file
            logging.info(f"Successfully extracted track {selected_stream_index} to temporary file.") # Don't log temp file path by default

        except ffmpeg.Error as e:
            err_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e)
            logging.error(f"Error extracting audio track with ffmpeg: {err_msg}")
            return None, None

        # 3. Get duration of the extracted audio track
        # Use librosa.get_duration on the temporary WAV file
        total_duration = librosa.get_duration(path=input_path_for_librosa)
        logging.info(f"Duration of extracted track: {total_duration:.2f} seconds")

        if total_duration < 0.1: # Check for very short/empty audio
            logging.error("Extracted audio track is too short or empty.")
            return None, None

        # 4. Determine loading parameters (offset, duration)
        start_time = 0
        load_duration = min(total_duration, max_duration)

        if total_duration > max_duration:
            # Select a random start time ensuring the segment fits
            max_start_time = total_duration - max_duration
            start_time = random.uniform(0, max_start_time)
            load_duration = max_duration # Load exactly max_duration
            logging.info(f"Track longer than {max_duration}s. Loading random {max_duration:.1f}s segment starting at {start_time:.2f}s.")
        else:
            logging.info(f"Track duration ({total_duration:.2f}s) <= {max_duration}s. Loading full extracted duration.")

        # 5. Load the audio segment using librosa from the temporary file
        logging.info(f"Loading audio segment with Librosa (offset={start_time:.2f}s, duration={load_duration:.2f}s)...")
        # Since we extracted to target SR and mono, librosa just needs to load it.
        # Explicitly setting sr and mono is still good practice.
        with warnings.catch_warnings():
            # Suppress librosa warnings about audioread/soundfile backends if they occur
            warnings.simplefilter("ignore") # Suppress potential librosa backend warnings
            audio_array, sr = librosa.load(
                input_path_for_librosa,
                sr=target_sr, # Ensure target sample rate
                mono=True,    # Ensure mono
                offset=start_time,
                duration=load_duration
            )
        logging.info(f"Loaded audio segment shape: {audio_array.shape}, Sample Rate: {sr} Hz")

        # Final check on sample rate
        if sr != target_sr:
            logging.warning(f"Loaded audio SR ({sr}) differs from target SR ({target_sr}). This might indicate an issue.")
            # Optionally, force resampling again, though ffmpeg should have handled it.
            # audio_array = librosa.resample(y=audio_array, orig_sr=sr, target_sr=target_sr)
            # sr = target_sr

        return audio_array, sr

    except librosa.LibrosaError as e:
        logging.error(f"Error loading audio with librosa: {e}")
        return None, None
    except FileNotFoundError:
        logging.error(f"Input file not found at '{file_path}'")
        return None, None
    except ffmpeg.Error as e: # Catch potential probing errors here too
        err_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e)
        logging.error(f"ffmpeg error during probing or processing: {err_msg}")
        return None, None
    except Exception as e:
        logging.exception(f"An unexpected error occurred during audio loading") # Log full traceback
        return None, None
    finally:
        # 6. Clean up the temporary file if it was created
        if temp_audio_file and os.path.exists(temp_audio_file):
            try:
                os.remove(temp_audio_file)
                logging.debug(f"Cleaned up temporary file: {temp_audio_file}")
            except OSError as e:
                logging.warning(f"Could not remove temporary file {temp_audio_file}: {e}")


def classify_audio(audio_array, sample_rate):
    """
    Classifies the audio using the pre-trained model pipeline.

    Args:
        audio_array (numpy.ndarray): The audio data.
        sample_rate (int): The sample rate of the audio data.

    Returns:
        str: The predicted music genre label, or None on error.
    """
    if audio_array is None or audio_array.size == 0:
        logging.error("Cannot classify empty or invalid audio array.")
        return None

    # Ensure the pipeline is initialized
    pipe = _init_pipeline()
    if pipe is None:
        logging.error("Classification pipeline is not available.")
        return None # Pipeline initialization failed earlier

    try:
        logging.info(f"Running classification on {audio_array.shape[0] / sample_rate:.2f}s of audio...")
        # The pipeline expects raw waveform and sampling rate
        # Use context manager to potentially suppress warnings during inference
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning) # Suppress common user warnings
            result = pipe({"raw": audio_array, "sampling_rate": sample_rate})

        # Result format is typically: [{'score': 0.99, 'label': 'Techno'}, ...]
        if not result:
            logging.warning("Classification returned no results.")
            return None

        # Sort by score (descending) and return the top label
        top_result = sorted(result, key=lambda x: x['score'], reverse=True)[0]
        genre = top_result['label']
        score = top_result['score']
        logging.info(f"Classification complete. Top result: {genre} (Score: {score:.4f})")
        return genre

    except Exception as e:
        logging.exception(f"Error during classification pipeline inference") # Log full traceback
        return None

# --- Public API Function ---

def get_music_genre(file_path: str, track_index: int = None) -> str | None:
    """
    Classifies the music genre of an audio file or a specific track within it.

    Loads a random segment (up to 15s), processes it, and uses the
    mtg-upf/discogs-maest-30s-pw-73e-ts model for classification.
    Uses CUDA if available.

    Args:
        file_path (str): Path to the audio or video file.
        track_index (int, optional): The specific audio track index to use
                                     (obtained via ffmpeg probe, e.g., using
                                     the --list-tracks option). Defaults to the
                                     first available audio track if None.

    Returns:
        str: The classified music genre (e.g., "Techno", "Rock", "Classical").
             Returns None if classification fails, the file is invalid,
             or no suitable audio track is found.
    """
    logging.info(f"--- Starting Music Genre Classification ---")
    logging.info(f"File: {file_path}")
    if track_index is not None:
        logging.info(f"Requested Track Index: {track_index}")

    # 1. Load the specified audio segment
    # Device selection happens implicitly when the pipeline is initialized later
    audio_array, sr = load_audio_segment(
        file_path,
        target_sr=TARGET_SR,
        max_duration=MAX_DURATION_SECONDS,
        track_index=track_index
    )

    # Check if loading was successful
    if audio_array is None or sr is None:
        logging.error("--- Classification Failed: Audio Loading Error ---")
        return None

    # 2. Classify the loaded audio
    genre = classify_audio(audio_array, sr)
    
    # 3. Replace any '---' with spaces in the genre label
    genre = genre.replace('---', ' ') if genre else None

    if not genre:
        logging.error("--- Classification Failed: Inference Error ---")

    return genre

# --- Command Line Interface Logic ---

def main():
    parser = argparse.ArgumentParser(
        description=f"Classify the music genre of an audio file (or track within a video file) using the {MODEL_NAME} model. Loads a random max {MAX_DURATION_SECONDS}s segment.",
        formatter_class=argparse.RawTextHelpFormatter # Preserve newline formatting in help
        )
    parser.add_argument(
        "file_path",
        help="Path to the input audio or video file (e.g., mp3, wav, ogg, m4a, mp4, mkv)."
        )
    parser.add_argument(
        "-l", "--list-tracks",
        action="store_true",
        help="List available audio tracks to stdout and exit. (Logs to stderr)"
        )
    parser.add_argument(
        "-s", "--select-track",
        type=int,
        default=None,
        metavar="INDEX",
        help="Select a specific audio track index to classify.\nUse --list-tracks to see available indices.\nDefaults to the first audio track found."
        )
    parser.add_argument(
        "--log-level",
        default="ERROR", # Default to INFO level
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: ERROR)."
        )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG level) logging to stderr."
        )

    # Handle case where no arguments are given
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    # Get the numeric level corresponding to the chosen string
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {args.log_level}')

    # Get the root logger and set its level
    # We reconfigure basicConfig here to ensure the level is set correctly
    # Note: basicConfig can only be called once effectively. If logging was used
    # before this point (e.g., by imported libraries), this might not reconfigure
    # everything. For more complex scenarios, more advanced logging setup is needed.
    # However, for this script's structure, this should work.
    logging.basicConfig(
        level=numeric_level, # Set level from args
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stderr,
        force=True # Add force=True to allow reconfiguration
    )
    logging.info(f"Logging level set to: {args.log_level}")

    # Adjust logging level if verbose flag is set
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Verbose logging enabled.")

    # Validate input file path
    if not os.path.isfile(args.file_path):
        logging.error(f"Input file not found or is not a file: {args.file_path}")
        sys.exit(1)

    # Handle --list-tracks functionality
    if args.list_tracks:
        if not list_audio_tracks(args.file_path):
            sys.exit(1) # Exit with error if probing failed
        sys.exit(0) # Exit successfully after listing

    # --- Main script execution: Classify the file ---
    # Use the public API function for the core logic
    predicted_genre = get_music_genre(args.file_path, track_index=args.select_track)

    # Exit with appropriate status code
    if predicted_genre:
        print(predicted_genre)
        sys.exit(0)
    else:
        # Failure message already logged to stderr by get_music_genre or its sub-functions
        logging.error("Music genre classification failed.")
        sys.exit(1) # Exit with non-zero status code

if __name__ == "__main__":
    main()

    # # --- Example of using the public API function from another script ---
    # # (Commented out by default)
    # print("\n--- Example API Usage (commented out in script) ---")
    # example_file = "path/to/your/test/audio_or_video.mp4" # <--- CHANGE THIS PATH
    # if os.path.exists(example_file):
    #     print(f"Running API example on: {example_file}")
    #     # Example 1: Use default track
    #     genre1 = get_music_genre(example_file)
    #     print(f"API Example Result (default track): {genre1}")

    #     # Example 2: Specify a track index (e.g., index 3, if it exists)
    #     # genre2 = get_music_genre(example_file, track_index=3)
    #     # print(f"API Example Result (track 3): {genre2}")
    # else:
    #      print(f"Skipping API example: File not found at {example_file}")
    # print("--- End Example API Usage ---")

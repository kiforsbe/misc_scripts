"""Music genre classification via a pre-trained Hugging Face audio-classification
pipeline (mtg-upf/discogs-maest-30s-pw-73e-ts). Loads a short random segment of
an audio/video file's audio track and classifies its genre.

The pipeline is lazily initialized on first use (a real model download/load,
seconds to minutes on first run); call warm_up() at startup to do that eagerly
instead of stalling the first real request that needs it.
"""
from transformers import pipeline
import librosa
import torch
import random
import ffmpeg  # ffmpeg-python library
import tempfile
import os
import threading
import warnings
import logging

# --- Constants ---
TARGET_SR = 16000
MAX_DURATION_SECONDS = 15
MODEL_NAME = "mtg-upf/discogs-maest-30s-pw-73e-ts"

# --- Global Variables for Lazy Loading ---
_pipeline = None
_device = None
_device_name = "Unknown"
_pipeline_lock = threading.Lock()


def _get_device():
    """Determines the appropriate device (CUDA or CPU) and stores it."""
    global _device, _device_name
    if _device is None:
        if torch.cuda.is_available():
            _device = 0  # Use GPU 0
            _device_name = f"CUDA ({torch.cuda.get_device_name(0)})"
            logging.info(f"CUDA available. Using device: {_device_name}")
        else:
            _device = -1  # Use CPU
            _device_name = "CPU"
            logging.info("CUDA not available. Using device: CPU.")
    return _device


def _init_pipeline():
    """
    Initializes the classification pipeline (normally lazily, on first use;
    see warm_up() for eager initialization). Safe to call from multiple
    threads concurrently - e.g. a startup warm-up thread racing a real
    classification request - without loading the model twice.
    """
    global _pipeline
    if _pipeline is not None:
        return _pipeline
    with _pipeline_lock:
        if _pipeline is not None:  # Re-check: another thread may have won the race.
            return _pipeline
        device_id = _get_device()
        try:
            logging.info(f"Initializing audio classification pipeline ({MODEL_NAME}) on {_device_name}...")
            _pipeline = pipeline(
                "audio-classification",
                model=MODEL_NAME,
                device=device_id,
                use_safetensors=True,  # Use safetensors if available
                trust_remote_code=True,  # Trust remote code for model loading
            )
            logging.info("Pipeline initialized successfully.")
        except Exception as e:
            logging.exception(f"Error initializing Hugging Face pipeline: {e}")
            logging.error("Please ensure you have 'torch' and 'transformers' installed correctly.")
            if _device == 0:
                logging.error("If using GPU, ensure CUDA drivers and toolkit are compatible with your PyTorch installation.")
            _pipeline = None  # Ensure it's None if init fails
    return _pipeline


def warm_up() -> bool:
    """
    Eagerly initializes the classification pipeline instead of waiting for
    the first real call to get_music_genre(). Intended to be called once
    at application startup (when genre recognition is enabled) so the
    model download/load - which can take anywhere from seconds to minutes
    on first run - happens up front instead of silently stalling whichever
    request happens to need it first.

    Returns True if the pipeline is ready to use.
    """
    return _init_pipeline() is not None


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
    selected_stream_map = None  # For ffmpeg extraction

    try:
        # 1. Probe the file to get stream info and validate track_index
        logging.info(f"Probing file details: {file_path}")
        probe = ffmpeg.probe(file_path)
        audio_streams = [s for s in probe.get('streams', []) if s.get('codec_type') == 'audio']

        if not audio_streams:
            logging.error(f"No audio streams found in '{file_path}'.")
            return None, None

        valid_indices = [s['index'] for s in audio_streams]
        default_stream_index = audio_streams[0]['index']  # Use the first audio stream by default

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
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmpfile:
            temp_audio_file = tmpfile.name

        try:
            # Extract using ffmpeg-python: force mono, target sample rate, 16-bit PCM WAV
            process = (
                ffmpeg
                .input(file_path)
                .output(temp_audio_file,
                        map=selected_stream_map,  # Select the specific stream
                        acodec='pcm_s16le',       # Output codec: 16-bit PCM
                        ac=1,                     # Output channels: 1 (mono)
                        ar=target_sr,             # Output sample rate
                        **{'loglevel': 'error'}   # Suppress verbose ffmpeg output
                    )
                .overwrite_output()
                .run_async(pipe_stderr=True)  # Run async to capture stderr
            )
            _, stderr = process.communicate()  # Wait for completion and get stderr
            if process.returncode != 0:
                raise ffmpeg.Error('ffmpeg', stdout=None, stderr=stderr)  # Raise error if ffmpeg failed

            input_path_for_librosa = temp_audio_file
            logging.info(f"Successfully extracted track {selected_stream_index} to temporary file.")

        except ffmpeg.Error as e:
            err_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e)
            logging.error(f"Error extracting audio track with ffmpeg: {err_msg}")
            return None, None

        # 3. Get duration of the extracted audio track
        total_duration = librosa.get_duration(path=input_path_for_librosa)
        logging.info(f"Duration of extracted track: {total_duration:.2f} seconds")

        if total_duration < 0.1:  # Check for very short/empty audio
            logging.error("Extracted audio track is too short or empty.")
            return None, None

        # 4. Determine loading parameters (offset, duration)
        start_time = 0
        load_duration = min(total_duration, max_duration)

        if total_duration > max_duration:
            # Select a random start time ensuring the segment fits
            max_start_time = total_duration - max_duration
            start_time = random.uniform(0, max_start_time)
            load_duration = max_duration  # Load exactly max_duration
            logging.info(f"Track longer than {max_duration}s. Loading random {max_duration:.1f}s segment starting at {start_time:.2f}s.")
        else:
            logging.info(f"Track duration ({total_duration:.2f}s) <= {max_duration}s. Loading full extracted duration.")

        # 5. Load the audio segment using librosa from the temporary file
        logging.info(f"Loading audio segment with Librosa (offset={start_time:.2f}s, duration={load_duration:.2f}s)...")
        with warnings.catch_warnings():
            # Suppress librosa warnings about audioread/soundfile backends if they occur
            warnings.simplefilter("ignore")
            audio_array, sr = librosa.load(
                input_path_for_librosa,
                sr=target_sr,  # Ensure target sample rate
                mono=True,     # Ensure mono
                offset=start_time,
                duration=load_duration
            )
        logging.info(f"Loaded audio segment shape: {audio_array.shape}, Sample Rate: {sr} Hz")

        # Final check on sample rate
        if sr != target_sr:
            logging.warning(f"Loaded audio SR ({sr}) differs from target SR ({target_sr}). This might indicate an issue.")

        return audio_array, sr

    except librosa.LibrosaError as e:
        logging.error(f"Error loading audio with librosa: {e}")
        return None, None
    except FileNotFoundError:
        logging.error(f"Input file not found at '{file_path}'")
        return None, None
    except ffmpeg.Error as e:  # Catch potential probing errors here too
        err_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e)
        logging.error(f"ffmpeg error during probing or processing: {err_msg}")
        return None, None
    except Exception as e:
        logging.exception(f"An unexpected error occurred during audio loading")
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
        return None  # Pipeline initialization failed earlier

    try:
        logging.info(f"Running classification on {audio_array.shape[0] / sample_rate:.2f}s of audio...")
        # The pipeline expects raw waveform and sampling rate
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)  # Suppress common user warnings
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
        logging.exception(f"Error during classification pipeline inference")
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

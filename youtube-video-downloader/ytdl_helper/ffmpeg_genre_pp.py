import os
import sys
import shutil
import tempfile
import logging
import uuid # Import uuid for generating unique names
from pathlib import Path

# --- Import the Genre Classifier ---
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # repo root
classifier_found = False
try:
    from common.music_style_classifier import get_music_genre
    classifier_found = True
    logging.info("Successfully imported common.music_style_classifier.")
except ImportError as e:
    logging.error(
        f"Could not import common.music_style_classifier: {e}. "
        "Ensure its dependencies (torch, transformers, librosa, ffmpeg-python) are installed."
    )

    def get_music_genre(*args, **kwargs):
        logging.warning("common.music_style_classifier not available. Cannot determine genre.")
        return None

# --- Continue with other imports ---

from yt_dlp.postprocessor.ffmpeg import FFmpegPostProcessor
from yt_dlp.utils import PostProcessingError, encodeFilename

# Configure logging for the classifier if it uses logging internally
# This is a basic setup; adjust if the classifier needs specific config
logger = logging.getLogger(__name__) # Use the logger defined in the module
# classifier_logger.setLevel(logging.WARNING) # Set level as needed


def is_classifier_available() -> bool:
    """True if music_style_classifier was found and imported successfully
    (its dependencies - transformers/torch/librosa/ffmpeg-python - are
    installed), i.e. genre detection can actually run rather than silently
    no-op via the fallback stub."""
    return classifier_found


def warm_up_genre_classifier() -> bool:
    """
    Eagerly loads the genre classifier's underlying ML model, if the
    classifier module is available. Intended to be called once at server
    startup when genre detection is enabled, so the (potentially slow,
    first-run) model download/load happens up front instead of stalling
    the first real download that needs it. A no-op returning False when
    the classifier module couldn't be imported (e.g. its dependencies
    aren't installed) - genre detection is then already going to no-op
    via the fallback stub, so there is nothing to warm up.
    """
    if not is_classifier_available():
        return False
    try:
        from common.music_style_classifier import warm_up as _warm_up_classifier
        return _warm_up_classifier()
    except Exception:
        logging.exception("Failed to warm up music style classifier.")
        return False


class FFmpegGenrePP(FFmpegPostProcessor):
    """
    Post processor that uses common.music_style_classifier to determine
    the genre of the downloaded file and embed it as metadata.
    """

    def __init__(self, downloader=None, **kwargs):
        # We don't need any specific options beyond what FFmpegPostProcessor provides
        super().__init__(downloader)
        self._kwargs = kwargs  # Store any potential future options

    #@PostProcessingError.catch_network_errors  # Decorator from yt-dlp utils
    def run(self, info):
        """Run the post-processing step."""
        filepath = info.get("filepath")
        if not filepath or not os.path.exists(filepath):
            self.report_warning(
                f"Filepath missing or file not found: {filepath}. Skipping genre detection."
            )
            logging.warning(
                f"Filepath missing or file not found: {filepath}. Skipping genre detection."
            )
            return [], info  # Must return ([files_to_delete], info)

        if not is_classifier_available():
            self.report_warning("Skipping genre detection because music_style_classifier could not be loaded.")
            logging.warning("Skipping genre detection because music_style_classifier could not be loaded.")
            return [], info


        self.to_screen(f'[genre] Analyzing genre for "{os.path.basename(filepath)}"')

        # --- Call the Genre Classifier ---
        predicted_genre = None # Initialize

        # --- Call the Genre Classifier ---
        try:
            # Ensure classifier logging is at least WARNING to avoid flooding logs
            # logging.getLogger().setLevel(logging.WARNING) # Or configure specific classifier logger

            # Call the classifier function
            predicted_genre = get_music_genre(filepath)

        except Exception as e:
            self.report_error(f"Error running music genre classifier on {os.path.basename(filepath)}: {e}", exc_info=True)
            logging.error(f"Error running music genre classifier on {os.path.basename(filepath)}: {e}", exc_info=True)
            # Decide whether to stop processing or continue without genre
            # For now, let's continue without genre
            predicted_genre = None

        if not predicted_genre:
            self.report_warning(
                f"Could not determine genre for {os.path.basename(filepath)}. Skipping metadata embedding."
            )
            logging.warning(
                f"Could not determine genre for {os.path.basename(filepath)}. Skipping metadata embedding."
            )
            return [], info

        self.to_screen(f"[genre] Determined genre for {os.path.basename(filepath)}: {predicted_genre}")

        # --- Embed Metadata using FFmpeg ---
        temp_filename_path = None # Initialize path variable
        try:
            # --- Manually construct a simpler temporary filename ---
            # 1. Get directory and original extension
            file_dir, original_filename = os.path.split(filepath)
            _, original_ext = os.path.splitext(original_filename)
            # 2. Create a unique base name
            unique_part = uuid.uuid4().hex[:8]
            temp_base_name = f"genre_temp_{unique_part}{original_ext}"
            # 3. Combine path, base name, original extension, and a .tmp suffix
            temp_filename_path = os.path.join(file_dir, temp_base_name)

            # Ensure it doesn't exist (highly unlikely, but good practice)
            if os.path.exists(temp_filename_path):
                os.remove(temp_filename_path)
            # --- End of manual temp filename construction ---

            # Prepare ffmpeg command options (excluding input/output paths)
            # These options apply to the output file in run_ffmpeg
            output_options = [
                "-c", "copy",             # Copy all streams
                "-map_metadata", "0",     # Copy global metadata from input (index 0)
                "-metadata", f"genre={predicted_genre}", # Set the genre
                "-loglevel", "error",     # Or 'warning' for more info
            ]

            self.to_screen(
                f'[ffmpeg] Embedding genre metadata into "{os.path.basename(filepath)}"'
            )

            # Call run_ffmpeg(input_path, output_path, output_options)
            # The base class handles encoding the paths correctly for ffmpeg.
            self.run_ffmpeg(filepath, temp_filename_path, output_options)

            # Replace the original file with the new one containing metadata
            # Use shutil.move for better cross-filesystem compatibility
            shutil.move(temp_filename_path, filepath)
            self.to_screen(f"[genre] Successfully embedded genre metadata.")
            files_to_delete = [] # No files left to delete by yt-dlp itself

        except PostProcessingError as ffmpeg_err: # Catch specific ffmpeg errors
            self.report_error(
                f"Failed to embed genre metadata (ffmpeg execution failed): {ffmpeg_err}"
            )
            logging.error(
                f"Failed to embed genre metadata (ffmpeg execution failed): {ffmpeg_err}"
            )
            files_to_delete = []
            # Clean up temp file on error
            if temp_filename_path and os.path.exists(temp_filename_path):
                try: os.remove(temp_filename_path)
                except OSError: self.report_warning(f"Could not remove temporary file {temp_filename_path}")
        except Exception as e:
            self.report_error(f"Unexpected error during genre embedding: {e}", exc_info=True) # Log traceback for unexpected errors
            logging.error(f"Unexpected error during genre embedding: {e}", exc_info=True)
            files_to_delete = []
            # Clean up temp file on error
            if temp_filename_path and os.path.exists(temp_filename_path):
                try: os.remove(temp_filename_path)
                except OSError: 
                    self.report_warning(f"Could not remove temporary file {temp_filename_path}")
                    logging.warning(f"Could not remove temporary file {temp_filename_path}")

        return files_to_delete, info

import os
import sys
import shutil
import tempfile
import logging
import uuid # Import uuid for generating unique names

# --- Find and Import the Genre Classifier ---
classifier_found = False
classifier_path_found = None
expected_locations = []
_get_music_genre_fallback = None # Placeholder for fallback function

try:
    # Get the directory containing the current script (ffmpeg_genre_pp.py)
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    # Get the parent directory
    parent_dir = os.path.dirname(current_script_dir)
    # Get the parent's parent directory
    grandparent_dir = os.path.dirname(parent_dir)

    # Define the name of the module file we are looking for
    module_filename = "music_style_classifier.py"
    module_name = "music_style_classifier" # Name used for import

    # List of directories to search, starting from the parent's parent
    # We prioritize finding it further up, assuming that's the intended structure.
    # You could reverse this list if you prefer checking closer directories first.
    search_dirs = [grandparent_dir, parent_dir, current_script_dir]

    for search_dir in search_dirs:
        # Ensure we have a valid directory path (os.path.dirname can return empty strings)
        if not search_dir:
            continue

        potential_path = os.path.join(search_dir, module_filename)
        expected_locations.append(potential_path) # Track searched paths for error messages

        if os.path.isfile(potential_path):
            # Found the file! Add its directory to sys.path if not already present.
            if search_dir not in sys.path:
                sys.path.insert(0, search_dir) # Insert at beginning for priority
                # print(f"DEBUG: Added '{search_dir}' to sys.path") # Optional debug print

            # Attempt the import now that the path is set
            try:
                # Use the module name derived from the filename
                from music_style_classifier import (
                    get_music_genre as imported_get_music_genre,
                    main as classifier_main, # Keep importing main if needed
                )
                # Assign the imported function to the main variable name
                get_music_genre = imported_get_music_genre
                classifier_path_found = potential_path
                classifier_found = True
                logging.info(f"Successfully imported {module_name} from {search_dir}")
                break # Stop searching once successfully imported
            except ImportError as import_err:
                # This might happen if the file exists but has internal import errors
                logging.warning(f"Found '{potential_path}' but failed to import: {import_err}")
                
                # Remove the path if we added it and it caused an error? Optional.
                # if search_dir == sys.path[0]:
                #     sys.path.pop(0)
                continue # Continue searching other locations

    if not classifier_found:
        # If the loop finishes without finding and importing the module
        raise ImportError("Module not found in search paths.")

# --- Handle Import Failure Gracefully ---
except ImportError as e:
    # Construct a helpful error message showing where we looked
    error_msg = (
        f"Could not find or import '{module_name}'.\n"
        f"Searched for '{module_filename}' in the following locations relative to this script:\n"
    )
    # Use unique locations in the error message
    unique_locations = sorted(list(set(expected_locations)))
    for loc in unique_locations:
        error_msg += f"  - {loc}\n"
    error_msg += (
        f"Ensure '{module_filename}' exists in one of these directories "
        "or is installed/accessible via PYTHONPATH.\n"
    )
    if str(e) != "Module not found in search paths.": # Add original error if it wasn't ours
         error_msg += f"Specific import error encountered: {e}"
    logging.error(error_msg)

    # Define a dummy function so the PP doesn't crash immediately
    def get_music_genre_fallback(*args, **kwargs):
        logging.warning(f"{module_name} not found or failed to import. Cannot determine genre.")
        return None

    # Assign the fallback to the main function name
    get_music_genre = get_music_genre_fallback
    # Define dummy main as well if needed
    def classifier_main(*args, **kwargs):
        logging.warning(f"{module_name} not found or failed to import. Cannot run its main function.")
        pass

# --- Continue with other imports ---

from yt_dlp.postprocessor.ffmpeg import FFmpegPostProcessor
from yt_dlp.utils import PostProcessingError, encodeFilename

# --- Import the Genre Classifier ---
# This assumes 'music_style_classifier.py' is accessible in the Python path
# or you adjust the import path accordingly.
# If it's one level up from ytdl_helper, you might need path adjustments.
# For simplicity, let's assume it can be imported directly for now.
try:
    # If music_style_classifier.py is in the parent directory of ytdl_helper
    # and the parent directory is in sys.path:
    # from .. import music_style_classifier # Use relative import if applicable
    # Or if it's installed as a module:
    # from music_style_classifier_module import get_music_genre
    # --- Assuming it's directly importable ---
    from music_style_classifier import (
        get_music_genre,
        main as classifier_main,
    )  # Import main to potentially configure logging if needed
except ImportError as e:
    logging.error(f"Could not import music_style_classifier: {e}")
    logging.error(
        "Ensure music_style_classifier.py is in the Python path or adjust the import."
    )

    # Define a dummy function so the PP doesn't crash immediately if import fails,
    # but it won't do anything useful.
    def get_music_genre(*args, **kwargs):
        logging.warning("music_style_classifier not found, cannot determine genre.")
        return None


# Configure logging for the classifier if it uses logging internally
# This is a basic setup; adjust if the classifier needs specific config
logger = logging.getLogger(__name__) # Use the logger defined in the module
# classifier_logger.setLevel(logging.WARNING) # Set level as needed


class FFmpegGenrePP(FFmpegPostProcessor):
    """
    Post processor that uses music_style_classifier.py to determine
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
            return [], info  # Must return ([files_to_delete], info)        # Check if the fallback function is being used (meaning import failed)
        if get_music_genre == _get_music_genre_fallback:
            self.report_warning("Skipping genre detection because music_style_classifier could not be loaded.")
            logging.warning("Skipping genre detection because music_style_classifier could not be loaded.")
            return [], info

        # Check if the fallback function is being used (meaning import failed)
        # Use a more robust check, e.g., checking its __name__ or a flag
        is_fallback = getattr(get_music_genre, '_is_fallback', False)
        if is_fallback:
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

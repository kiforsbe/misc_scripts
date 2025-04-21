# file: set_music_genre.py
import argparse
import logging
import os
import sys
import ffmpeg # ffmpeg-python library
import tempfile # For in-place modification
import shutil # For safer file replacement

# --- Import from your classifier script ---
# Assuming music_style_classifier.py is in the same directory or accessible
try:
    import music_style_classifier
except ImportError:
    logging.critical("Error: Could not import 'music_style_classifier.py'. "
                     "Ensure it's in the same directory or accessible in PYTHONPATH.")
    sys.exit(1)
# ---

def setup_logging(log_level_str):
    """Configures logging based on the provided level string."""
    numeric_level = getattr(logging, log_level_str.upper(), None)
    if not isinstance(numeric_level, int):
        logging.warning(f"Invalid log level: {log_level_str}. Defaulting to WARNING.")
        numeric_level = logging.WARNING

    # Configure root logger
    # Use force=True to allow reconfiguration if logging was already used
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        stream=sys.stderr,
        force=True
    )
    logging.info(f"Logging level set to: {log_level_str.upper()}")

def generate_suffixed_output_path(input_path):
    """Generates the output path with '_upd' appended before the extension."""
    directory, filename = os.path.split(input_path)
    name, ext = os.path.splitext(filename)
    output_filename = f"{name}_upd{ext}"
    return os.path.join(directory, output_filename)

def set_file_genre(input_file, output_file, genre):
    """
    Uses ffmpeg-python to copy the input file and set the genre metadata.

    Args:
        input_file (str): Path to the input file.
        output_file (str): Path to the temporary or final output file.
        genre (str): The genre string to set.

    Returns:
        bool: True if successful, False otherwise.
    """
    logging.info(f"Setting genre '{genre}' for output file: {os.path.basename(output_file)}")
    try:
        process = (
            ffmpeg
            .input(input_file)
            .output(
                output_file,
                map='0',                    # Map all streams from input 0
                c='copy',                   # Copy all codecs without re-encoding
                metadata=f'genre={genre}',  # Set the genre metadata tag
                **{'loglevel': 'warning'}   # Show only warnings and errors from ffmpeg
            )
            .overwrite_output()
            .run_async(pipe_stderr=True)    # Run async to capture stderr
        )
        _, stderr = process.communicate()   # Wait for completion and get stderr

        if process.returncode != 0:
            err_msg = stderr.decode(errors='ignore') if stderr else "Unknown ffmpeg error"
            logging.error(f"ffmpeg failed for {input_file}: {err_msg.strip()}")
            return False
        else:
            logging.debug(f"Successfully created/updated file with genre: {output_file}")
            return True

    except ffmpeg.Error as e:
        err_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e)
        logging.error(f"ffmpeg error processing {input_file}: {err_msg.strip()}")
        return False
    except Exception as e:
        logging.exception(f"Unexpected error setting genre for {input_file}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Update the 'genre' metadata of media files using music_style_classifier.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "file_paths",
        nargs='+', # Accept one or more file paths
        help="Path(s) to the input audio or video file(s)."
    )

    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "-o", "--output-folder",
        metavar="FOLDER",
        help="Specify an output folder. Original filenames are kept.\n"
             "Mutually exclusive with --in-place and --suffix."
    )
    output_group.add_argument(
        "-i", "--in-place",
        action="store_true",
        help="Modify the original files directly (uses a temporary file).\n"
             "Mutually exclusive with --output-folder and --suffix.\n"
             "This is the DEFAULT if no other output option is specified."
    )
    output_group.add_argument(
        "--suffix",
        action="store_true",
        help="Create new files with '_upd' appended before the extension (old behavior).\n"
             "Mutually exclusive with --output-folder and --in-place."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)."
    )
    # Optional: Add argument for track selection if needed
    parser.add_argument(
        "-s", "--select-track",
        type=int,
        default=None,
        metavar="INDEX",
        help="Select a specific audio track index to classify (optional).\n"
             "Use music_style_classifier.py --list-tracks to find indices."
    )

    args = parser.parse_args()

    setup_logging(args.log_level)

    mode = "in-place" # Default
    if args.output_folder:
        mode = "folder"
        # Ensure output folder exists
        try:
            os.makedirs(args.output_folder, exist_ok=True)
            logging.info(f"Outputting files to folder: {args.output_folder}")
        except OSError as e:
            logging.critical(f"Could not create output folder '{args.output_folder}': {e}")
            sys.exit(1)
    elif args.suffix:
        mode = "suffix"
        logging.info("Outputting files with '_upd' suffix.")
    elif args.in_place:
        # Explicitly chosen in-place, log it
        logging.info("Modifying files in-place.")
    else:
        # Defaulting to in-place, log it
        logging.info("Defaulting to in-place modification.")

    success_count = 0
    fail_count = 0

    for input_path in args.file_paths:
        if not os.path.isfile(input_path):
            logging.warning(f"Skipping non-existent file: {input_path}")
            fail_count += 1
            continue

        logging.info(f"--- Processing file: {input_path} ---")

        # 1. Get the genre using the imported classifier function
        predicted_genre = music_style_classifier.get_music_genre(
            input_path,
            track_index=args.select_track
        )

        if predicted_genre is None:
            logging.error(f"Could not determine genre for: {input_path}. Skipping metadata update.")
            fail_count += 1
            continue # Skip to the next file

        logging.info(f"Predicted genre for {input_path}: {predicted_genre}")

        # 2. Handle file output based on mode
        temp_output_path = None # For in-place mode cleanup
        try:
            if mode == "in-place":
                # Create a temporary file in the same directory
                temp_fd, temp_output_path = tempfile.mkstemp(
                    suffix=os.path.splitext(input_path)[1], # Keep original extension
                    prefix=f"{os.path.splitext(os.path.basename(input_path))[0]}_tmp_",
                    dir=os.path.dirname(input_path)
                )
                os.close(temp_fd) # Close the file handle, ffmpeg will open it
                logging.debug(f"Using temporary file for in-place update: {temp_output_path}")

                if set_file_genre(input_path, temp_output_path, predicted_genre):
                    # Replace original file with the temporary one
                    try:
                        shutil.move(temp_output_path, input_path) # More robust than os.replace sometimes
                        logging.info(f"Successfully updated in-place: {input_path}")
                        success_count += 1
                        temp_output_path = None # Prevent deletion in finally block
                    except Exception as e:
                        logging.error(f"Failed to replace original file {input_path} with temp file {temp_output_path}: {e}")
                        fail_count += 1
                else:
                    fail_count += 1 # set_file_genre failed

            else: # mode == "folder" or mode == "suffix"
                if mode == "folder":
                    output_path = os.path.join(args.output_folder, os.path.basename(input_path))
                else: # mode == "suffix"
                    output_path = generate_suffixed_output_path(input_path)

                if set_file_genre(input_path, output_path, predicted_genre):
                    logging.info(f"Successfully created output file: {output_path}")
                    success_count += 1
                else:
                    fail_count += 1
                    # Attempt to clean up potentially incomplete output file on failure
                    if os.path.exists(output_path):
                        try:
                            os.remove(output_path)
                            logging.warning(f"Removed potentially incomplete output file: {output_path}")
                        except OSError as e:
                            logging.error(f"Could not remove failed output file {output_path}: {e}")

        except Exception as e:
            logging.exception(f"Unexpected error during processing loop for {input_path}: {e}")
            fail_count += 1
        finally:
            # Clean up temporary file if it still exists (e.g., on failure)
            if temp_output_path and os.path.exists(temp_output_path):
                try:
                    os.remove(temp_output_path)
                    logging.debug(f"Cleaned up temporary file: {temp_output_path}")
                except OSError as e:
                    logging.error(f"Could not remove temporary file {temp_output_path}: {e}")


    logging.info("--- Processing Summary ---")
    logging.info(f"Successfully processed: {success_count} file(s)")
    logging.info(f"Failed/Skipped:       {fail_count} file(s)")
    logging.info("--------------------------")

    if fail_count > 0:
        sys.exit(1) # Exit with error code if any files failed
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()

"""CLI for classifying the music genre of an audio/video file.

The actual classifier (model loading, audio extraction, inference) lives in
common/music_style_classifier.py, which other tools import as a library
(e.g. get_music_genre(), warm_up()). This script is the standalone CLI
wrapper around it, plus --list-tracks probing (a CLI-only concern).
"""
import argparse
import sys
import os
import logging
import ffmpeg  # ffmpeg-python library
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[0]))
from common.music_style_classifier import MAX_DURATION_SECONDS, MODEL_NAME, get_music_genre


def list_audio_tracks(file_path):
    """Lists available audio tracks in a media file using ffmpeg."""
    logging.info(f"Probing audio tracks for: {file_path}")
    try:
        probe = ffmpeg.probe(file_path)
        audio_streams = [s for s in probe.get('streams', []) if s.get('codec_type') == 'audio']

        if not audio_streams:
            logging.warning("No audio streams found in this file.")
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
        err_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e)
        logging.error(f"Error probing file with ffmpeg: {err_msg}")
        logging.error("Please ensure ffmpeg is installed and in your system's PATH.")
        return False
    except Exception as e:
        logging.exception(f"An unexpected error occurred during probing: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description=f"Classify the music genre of an audio file (or track within a video file) using the {MODEL_NAME} model. Loads a random max {MAX_DURATION_SECONDS}s segment.",
        formatter_class=argparse.RawTextHelpFormatter
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
        default="ERROR",
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

    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stderr,
        force=True
    )
    logging.info(f"Logging level set to: {args.log_level}")

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
            sys.exit(1)
        sys.exit(0)

    # --- Main script execution: Classify the file ---
    predicted_genre = get_music_genre(args.file_path, track_index=args.select_track)

    if predicted_genre:
        print(predicted_genre)
        sys.exit(0)
    else:
        logging.error("Music genre classification failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()

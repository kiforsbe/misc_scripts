import argparse
import logging
import os
import sys
import csv
import ffmpeg  # ffmpeg-python library
from datetime import timedelta # For formatting duration

def setup_logging(log_level_str):
    """Configures logging based on the provided level string."""
    numeric_level = getattr(logging, log_level_str.upper(), None)
    if not isinstance(numeric_level, int):
        logging.warning(f"Invalid log level: {log_level_str}. Defaulting to INFO.")
        numeric_level = logging.INFO

    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stderr,
        force=True
    )
    logging.info(f"Logging level set to: {log_level_str.upper()}")

def format_duration(seconds_str):
    """Formats duration from seconds string to HH:MM:SS.ms."""
    try:
        seconds = float(seconds_str)
        td = timedelta(seconds=seconds)
        # Format timedelta manually for HH:MM:SS.ms
        total_seconds = int(td.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds_part = divmod(remainder, 60)
        milliseconds = int(td.microseconds / 1000)
        return f"{hours:02}:{minutes:02}:{seconds_part:02}.{milliseconds:03}"
    except (ValueError, TypeError):
        return seconds_str # Return original if conversion fails

def format_bitrate(bitrate_str):
    """Formats bitrate from bps to kbps."""
    try:
        bitrate = int(bitrate_str)
        return f"{bitrate // 1000} kbps"
    except (ValueError, TypeError):
        return bitrate_str # Return original if conversion fails

def extract_metadata(file_path):
    """
    Extracts metadata from a media file using ffmpeg.probe.

    Args:
        file_path (str): Path to the media file.

    Returns:
        dict: A dictionary containing extracted metadata, or None on error.
              Keys are flattened for easier CSV writing.
    """
    logging.debug(f"Probing file: {file_path}")
    try:
        probe = ffmpeg.probe(file_path)
        format_info = probe.get('format', {})
        tags = format_info.get('tags', {})

        metadata = {
            'filename': os.path.basename(file_path),
            'full_path': os.path.abspath(file_path),
            'format_name': format_info.get('format_name'),
            'duration_seconds': format_info.get('duration'),
            'duration_formatted': format_duration(format_info.get('duration')),
            'size_bytes': format_info.get('size'),
            'bit_rate_bps': format_info.get('bit_rate'),
            'bit_rate_formatted': format_bitrate(format_info.get('bit_rate')),
            'title': tags.get('title'),
            'artist': tags.get('artist'),
            'album_artist': tags.get('album_artist'),
            'album': tags.get('album'),
            'genre': tags.get('genre'),
            'date': tags.get('date'),
            'track': tags.get('track'),
            'disc': tags.get('disc'),
            'composer': tags.get('composer'),
            'publisher': tags.get('publisher'),
            'encoder': tags.get('encoder'),
            'comment': tags.get('comment'),
            # Add more tags as needed, e.g., 'copyright', 'lyrics', etc.
        }

        # Extract primary video stream info
        video_stream = next((s for s in probe.get('streams', []) if s.get('codec_type') == 'video'), None)
        if video_stream:
            metadata.update({
                'video_codec': video_stream.get('codec_name'),
                'video_resolution': f"{video_stream.get('width')}x{video_stream.get('height')}" if video_stream.get('width') else None,
                'video_frame_rate': video_stream.get('r_frame_rate'), # Rational number as string 'num/den'
                'video_bit_rate_bps': video_stream.get('bit_rate'),
                'video_bit_rate_formatted': format_bitrate(video_stream.get('bit_rate')),
            })
        else:
            metadata.update({
                'video_codec': None, 'video_resolution': None, 'video_frame_rate': None,
                'video_bit_rate_bps': None, 'video_bit_rate_formatted': None,
            })

        # Extract primary audio stream info
        audio_stream = next((s for s in probe.get('streams', []) if s.get('codec_type') == 'audio'), None)
        if audio_stream:
            metadata.update({
                'audio_codec': audio_stream.get('codec_name'),
                'audio_sample_rate': audio_stream.get('sample_rate'),
                'audio_channels': audio_stream.get('channels'),
                'audio_channel_layout': audio_stream.get('channel_layout'),
                'audio_bit_rate_bps': audio_stream.get('bit_rate'),
                'audio_bit_rate_formatted': format_bitrate(audio_stream.get('bit_rate')),
            })
        else:
            metadata.update({
                'audio_codec': None, 'audio_sample_rate': None, 'audio_channels': None,
                'audio_channel_layout': None, 'audio_bit_rate_bps': None, 'audio_bit_rate_formatted': None,
            })

        # Replace None with empty strings for CSV compatibility if preferred
        # metadata = {k: (v if v is not None else "") for k, v in metadata.items()}

        logging.debug(f"Successfully probed: {file_path}")
        return metadata

    except ffmpeg.Error as e:
        err_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e)
        logging.error(f"ffmpeg error probing {file_path}: {err_msg.strip()}")
        return None
    except Exception as e:
        logging.exception(f"Unexpected error probing {file_path}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(
        description="Extract metadata from media files and save to a CSV.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "file_paths",
        nargs='+', # Accept one or more file paths
        help="Path(s) to the input audio or video file(s)."
    )
    parser.add_argument(
        "-o", "--output",
        default="metadata_output.csv",
        metavar="CSV_FILE",
        help="Path to the output CSV file (default: metadata_output.csv)."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)."
    )

    args = parser.parse_args()
    setup_logging(args.log_level)

    all_metadata = []
    processed_count = 0
    fail_count = 0

    # Define the headers for the CSV file (ensure order and completeness)
    # It's good practice to define all expected keys here.
    csv_headers = [
        'filename', 'full_path', 'format_name', 'duration_seconds', 'duration_formatted',
        'size_bytes', 'bit_rate_bps', 'bit_rate_formatted',
        'title', 'artist', 'album_artist', 'album', 'genre', 'date', 'track', 'disc',
        'composer', 'publisher', 'encoder', 'comment',
        'video_codec', 'video_resolution', 'video_frame_rate', 'video_bit_rate_bps', 'video_bit_rate_formatted',
        'audio_codec', 'audio_sample_rate', 'audio_channels', 'audio_channel_layout', 'audio_bit_rate_bps', 'audio_bit_rate_formatted',
    ]

    logging.info(f"Starting metadata extraction for {len(args.file_paths)} file(s)...")

    for input_path in args.file_paths:
        if not os.path.isfile(input_path):
            logging.warning(f"Skipping non-existent file: {input_path}")
            fail_count += 1
            continue

        metadata = extract_metadata(input_path)
        if metadata:
            all_metadata.append(metadata)
            processed_count += 1
        else:
            fail_count += 1

    if not all_metadata:
        logging.warning("No metadata could be extracted from any input files.")
        sys.exit(1)

    logging.info(f"Writing metadata for {processed_count} file(s) to {args.output}...")

    try:
        with open(args.output, 'w', newline='', encoding='utf-8') as csvfile:
            # Use QUOTE_ALL to ensure fields containing delimiters or quotes are handled correctly
            writer = csv.DictWriter(
                csvfile,
                fieldnames=csv_headers,
                quoting=csv.QUOTE_ALL,
                extrasaction='ignore' # Ignore metadata keys not in headers
            )

            writer.writeheader()
            writer.writerows(all_metadata)

        logging.info(f"Successfully wrote metadata to {args.output}")

    except IOError as e:
        logging.critical(f"Could not write to output file {args.output}: {e}")
        sys.exit(1)
    except Exception as e:
        logging.exception(f"An unexpected error occurred during CSV writing: {e}")
        sys.exit(1)

    logging.info("--- Extraction Summary ---")
    logging.info(f"Successfully processed: {processed_count} file(s)")
    logging.info(f"Failed/Skipped:       {fail_count} file(s)")
    logging.info("--------------------------")

    if fail_count > 0:
        sys.exit(1) # Exit with error code if any files failed
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()

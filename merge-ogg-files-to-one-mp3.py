import sys
import os
from pydub import AudioSegment
import inquirer
from tqdm import tqdm
import logging
from datetime import datetime

def setup_logging():
    """Set up logging to both file and console with UTF-8 support."""
    # Ensure logs directory exists
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Create a logger
    logger = logging.getLogger('audio_merger')
    logger.setLevel(logging.DEBUG)
    
    # Create formatters with encoding specification
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    
    # File handler with UTF-8 encoding
    log_filename = os.path.join('logs', f'audio_merger_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)
    
    # Console handler with system encoding
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
    
def wait_for_key():
    """Wait for a key press."""
    input("\nPress enter/return to exit...")

def get_unique_output_path(base_path):
    """Generate a unique output path if file already exists."""
    directory = os.path.dirname(base_path)
    filename = os.path.basename(base_path)
    name, ext = os.path.splitext(filename)
    counter = 1
    
    while os.path.exists(base_path):
        base_path = os.path.join(directory, f"{name}_{counter}{ext}")
        counter += 1
    
    return base_path

def get_file_info(file_path):
    """Get detailed information about the audio file using pydub."""
    try:
        audio = AudioSegment.from_file(file_path)
        
        info = {
            'path': os.path.abspath(file_path),
            'size': os.path.getsize(file_path),
            'format': os.path.splitext(file_path)[1][1:].upper(),
            'sample_rate': audio.frame_rate,
            'channels': audio.channels,
            'bits_per_sample': audio.sample_width * 8,
            'duration': len(audio) / 1000.0,  # Convert milliseconds to seconds
            'frame_count': len(audio.get_array_of_samples()),
            'frame_width': audio.frame_width,
            'raw_data_size': len(audio.raw_data)
        }
            
        # Calculate average bitrate (bits per second)
        info['bitrate'] = int((info['raw_data_size'] * 8) / info['duration'])
        
        return info
    except Exception as e:
        return {'error': str(e)}
    
def export_with_progress(audio_segment, output_path, export_args, logger):
    """Export audio with a progress bar."""
    try:
        print("\nExporting combined file...")
        logger.info("Exporting combined file...")
        
        # Show indeterminate progress bar
        with tqdm(total=100, desc="Exporting", unit="%", bar_format='{desc}: {bar}') as pbar:
            # Update progress bar periodically
            pbar.update(50)
            # Perform the export
            audio_segment.export(output_path, **export_args)
            pbar.update(50)
        
        if os.path.exists(output_path):
            return True
        else:
            logger.error("Error: Output file was not created")
            return False
            
    except Exception as e:
        logger.error(f"Error exporting combined file: {str(e)}")
        return False
    
def get_export_settings(logger):
    """Get format-specific export settings from user."""
    format_choices = [
        ('MP3 - Most compatible lossy format', 'mp3'),
        ('WAV - Lossless audio format', 'wav'),
        ('OGG - Free lossy format', 'ogg'),
        ('FLAC - Free lossless format', 'flac'),
        ('AAC - Advanced Audio Coding', 'aac'),
        ('M4A - Apple Audio Format', 'm4a'),
    ]

    # Bitrate settings for lossy formats
    bitrates = [
        ('320k - Highest Quality', '320k'),
        ('256k - Very High Quality', '256k'),
        ('192k - High Quality', '192k'),
        ('128k - Standard Quality', '128k'),
    ]

    # Quality settings for OGG
    ogg_quality = [
        ('10 - Highest Quality', '10'),
        ('8 - Very High Quality', '8'),
        ('6 - High Quality', '6'),
        ('4 - Standard Quality', '4'),
    ]

    # Sample rate options
    sample_rates = [
        ('48000 Hz - Professional Audio', '48000'),
        ('44100 Hz - CD Quality', '44100'),
        ('32000 Hz - Broadcasting', '32000'),
        ('22050 Hz - Low Quality', '22050'),
    ]

    # Bits per sample options
    bits_per_sample = [
        ('32 bits - Studio Quality', 'f32le'),
        ('24 bits - Professional Audio', 's24le'),
        ('16 bits - CD Quality', 's16le'),
        ('8 bits - Low Quality', 'u8'),
    ]

    # Channel options
    channels = [
        ('Stereo (2 channels)', '2'),
        ('Mono (1 channel)', '1'),
    ]

    # Get basic format selection
    questions = [
        inquirer.List('format',
                     message="Select output format:",
                     choices=format_choices),
        inquirer.List('sample_rate',
                     message="Select sample rate:",
                     choices=sample_rates),
        inquirer.List('bits_per_sample',
                     message="Select bits per sample:",
                     choices=bits_per_sample),
        inquirer.List('channels',
                     message="Select channel configuration:",
                     choices=channels),
    ]

    answers = inquirer.prompt(questions)
    logger.info(f"Selected format: {answers['format']}")
    logger.info(f"Selected sample rate: {answers['sample_rate']}")
    logger.info(f"Selected bits per sample: {answers['bits_per_sample']}")
    logger.info(f"Selected channels: {answers['channels']}")

    # Base export arguments
    export_args = {
        'format': answers['format'],
        'parameters': [
            '-ar', answers['sample_rate'],
            '-ac', answers['channels']
        ]
    }

    # Add bits per sample parameter only for formats that support it
    if 'bits_per_sample' in answers and answers['format'] in ['wav', 'flac']:
        export_args['parameters'].extend(['-sample_fmt', answers['bits_per_sample']])

    # Format-specific settings
    if answers['format'] == 'ogg':
        questions = [
            inquirer.List('quality',
                         message="Select OGG quality (0-10):",
                         choices=ogg_quality),
        ]
        quality_answer = inquirer.prompt(questions)
        logger.info(f"Selected OGG quality: {quality_answer['quality']}")
        
        export_args.update({
            'codec': 'libvorbis',
            'parameters': export_args['parameters'] + ['-q:a', quality_answer['quality']]
        })

    elif answers['format'] == 'flac':
        export_args.update({
            'codec': 'flac',
            'parameters': export_args['parameters'] + ['-compression_level', '5']
        })
        logger.info("Using FLAC format with compression level 5")

    elif answers['format'] == 'aac':
        questions = [
            inquirer.List('bitrate',
                         message="Select AAC bitrate:",
                         choices=bitrates),
        ]
        bitrate_answer = inquirer.prompt(questions)
        logger.info(f"Selected bitrate: {bitrate_answer['bitrate']}")
        
        export_args.update({
            'format': 'adts',
            'codec': 'aac',
            'parameters': export_args['parameters'] + ['-b:a', bitrate_answer['bitrate']]
        })

    elif answers['format'] == 'm4a':
        questions = [
            inquirer.List('bitrate',
                         message="Select M4A bitrate:",
                         choices=bitrates),
        ]
        bitrate_answer = inquirer.prompt(questions)
        logger.info(f"Selected bitrate: {bitrate_answer['bitrate']}")
        
        export_args.update({
            'format': 'ipod',
            'codec': 'aac',
            'parameters': export_args['parameters'] + ['-b:a', bitrate_answer['bitrate']]
        })

    elif answers['format'] != 'wav':  # For MP3
        questions = [
            inquirer.List('bitrate',
                         message=f"Select {answers['format'].upper()} bitrate:",
                         choices=bitrates),
        ]
        bitrate_answer = inquirer.prompt(questions)
        logger.info(f"Selected bitrate: {bitrate_answer['bitrate']}")
        
        export_args.update({
            'parameters': export_args['parameters'] + ['-b:a', bitrate_answer['bitrate']]
        })

    return answers['format'], export_args

def convert_and_append(input_paths, logger):
    """Process audio files with UTF-8 path support."""
    if not input_paths:
        print("No input files provided.")
        logger.error("No input files provided.")
        return False

    # Normalize input paths
    input_paths = [os.path.normpath(path) for path in input_paths]
    output_dir = os.path.dirname(input_paths[0])
    supported_formats = {'.mp3', '.wav', '.ogg', '.aac', '.mp4', '.m4a', '.wma', '.flv', '.aiff'}
    
    print("\nPhase 1: Loading and analyzing audio files...")
    logger.info("Phase 1: Loading and analyzing audio files...")
    audio_segments = []
    skipped_files = []
    error_files = []
    
    with tqdm(total=len(input_paths), desc="Loading files", unit="file") as pbar:
        for input_path in input_paths:
            ext = os.path.splitext(input_path)[1].lower()
            if ext not in supported_formats:
                logger.warning(f"Skipping {input_path}: Unsupported format")
                skipped_files.append(input_path)
                pbar.update(1)
                continue
            
            try:
                audio = AudioSegment.from_file(input_path)
                audio_segments.append((input_path, audio))
                logger.info(f"Loaded {input_path} - {audio.frame_rate}Hz, {audio.channels} channels, {audio.sample_width * 8} bits")
            except Exception as e:
                print(f"Error processing {input_path}: {str(e)}")
                logger.error(f"Error processing {input_path}: {str(e)}")
                error_files.append((input_path, str(e)))
            pbar.update(1)
    
    if not audio_segments:
        print("No audio files were successfully loaded.")
        logger.error("No audio files were successfully loaded.")
        return False

    print("\nPhase 2: Getting export settings...")
    logger.info("Phase 2: Getting export settings...")
    
    # Get export settings
    format_ext, export_args = get_export_settings(logger)
    
    # Get unique output path
    base_output_path = os.path.normpath(os.path.join(output_dir, f"combined_output.{format_ext}"))
    output_path = get_unique_output_path(base_output_path)
    
    print("\nPhase 3: Converting and combining audio files...")
    logger.info("Phase 3: Converting and combining audio files...")
    combined = AudioSegment.empty()
    processed_files = []
    
    with tqdm(total=len(audio_segments), desc="Processing files", unit="file") as pbar:
        for input_path, audio in audio_segments:
            try:
                combined += audio
                processed_files.append(input_path)
                logger.info(f"Processed: {input_path}")
            except Exception as e:
                logger.error(f"Error converting {input_path}: {str(e)}")
                error_files.append((input_path, str(e)))
            pbar.update(1)
    
    if len(combined) > 0:
        # Export the combined audio
        success = export_with_progress(combined, output_path, export_args, logger)
        
        if not success:
            return False
    else:
        print("No audio files were successfully processed.")
        logger.error("No audio files were successfully processed.")
        return False

    print("\nProcessing Summary:")
    logger.info("Processing Summary:")
    print(f"Successfully processed: {len(processed_files)} files")
    logger.info(f"Successfully processed: {len(processed_files)} files")
    print(f"Skipped files: {len(skipped_files)}")
    logger.info(f"Skipped files: {len(skipped_files)}")
    print(f"Files with errors: {len(error_files)}")
    logger.info(f"Files with errors: {len(error_files)}")
    
    if error_files:
        print("\nFiles with errors:")
        logger.error("Files with errors:")
        for file, error in error_files:
            print(f"- {file}: {error}")
            logger.error(f"- {file}: {error}")
    
    if os.path.exists(output_path):
        # Get detailed file information
        file_info = get_file_info(output_path)
        
        if 'error' not in file_info:
            print("\nOutput File Information:")
            print("-" * 50)
            print(f"File Path: {file_info['path']}")
            print(f"File Size: {file_info['size'] / (1024*1024):.2f} MB")
            print(f"Format: {file_info['format']}")
            print(f"Sample Rate: {file_info['sample_rate']} Hz")
            print(f"Channels: {file_info['channels']} {'(Stereo)' if file_info['channels'] == 2 else '(Mono)'}")
            print(f"Bits per Sample: {file_info['bits_per_sample']} bit")
            print(f"Bitrate: {file_info['bitrate'] / 1000:.0f} kbps")
            print(f"Duration: {file_info['duration']:.2f} seconds")
            print(f"Frame Count: {file_info['frame_count']:,}")
            print(f"Frame Width: {file_info['frame_width']} bytes")
            
            # Print target settings if different from actual
            if 'target_sample_rate' in file_info and str(file_info['sample_rate']) != file_info['target_sample_rate']:
                print(f"Target Sample Rate: {file_info['target_sample_rate']} Hz")
            if 'target_channels' in file_info and str(file_info['channels']) != file_info['target_channels']:
                print(f"Target Channels: {file_info['target_channels']}")
            if 'target_bitrate' in file_info:
                print(f"Target Bitrate: {file_info['target_bitrate']}")
            if 'target_bits_per_sample' in file_info:
                print(f"Target Bits per Sample: {file_info['target_bits_per_sample']}")
            
            logger.info("File export successful")
            logger.info(f"File information: {file_info}")
            return True
        else:
            print(f"\nFile exported but could not read details: {file_info['error']}")
            logger.warning(f"File exported but could not read details: {file_info['error']}")
            return True
    else:
        logger.error("Error: Output file was not created")
        return False


    return True

def main():
    # Set default encoding to UTF-8
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr.reconfigure(encoding='utf-8')
    
    logger = setup_logging()
    logger.info("Starting audio merger script")
    print("Audio Merger Script")
    print("==================")
    
    if len(sys.argv) < 2:
        print("Usage: Drag and drop audio files (MP3, WAV, OGG, AAC, MP4, M4A, WMA, FLV, AIFF) onto this script")
        logger.error("No input files provided")
        wait_for_key()
        return

    success = convert_and_append(sys.argv[1:], logger)
    
    if success:
        print("\nScript execution completed successfully!")
        logger.info("Script execution completed successfully")
    else:
        print("\nScript execution completed with errors. Check the log file for details.")
        logger.info("Script execution completed with errors")
    
    wait_for_key()

if __name__ == "__main__":
    main()

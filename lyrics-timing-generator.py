import sys
import os
import logging
from pydub import AudioSegment
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, USLT, SYLT, Encoding
import numpy as np
import librosa
import warnings
import tensorflow as tf
from spleeter.separator import Separator
import soundfile as sf
import tempfile
import whisper
import shutil
from tqdm import tqdm
from dataclasses import dataclass
from typing import List
import time

# Suppress unnecessary warnings
warnings.filterwarnings('ignore')
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessingResult:
    filename: str
    success: bool
    error_message: str = None
    processing_time: float = 0
    output_mp3: str = None
    output_lrc: str = None

class LRCGenerator:
    def __init__(self, input_path):
        self.input_path = input_path
        self.separator = Separator('spleeter:2stems')
        self.whisper_model = whisper.load_model("base")
        self.pbar = None
        
    def get_output_path(self):
        """Generate output path by appending '_output' before the extension"""
        base, ext = os.path.splitext(self.input_path)
        return f"{base}_output{ext}"
        
    def extract_lyrics_from_audio(self, vocals):
        """Extract lyrics from vocals using Whisper"""
        logger.info("Extracting lyrics using Whisper")
        self.pbar.set_description("Transcribing audio")
        
        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_wav:
            sf.write(temp_wav.name, vocals, 16000)
            result = self.whisper_model.transcribe(temp_wav.name)
            os.unlink(temp_wav.name)
            
        lyrics = []
        timestamps = []
        for segment in result["segments"]:
            lyrics.append(segment["text"].strip())
            timestamps.append(segment["start"])
            
        logger.debug(f"Extracted {len(lyrics)} lyrics segments")
        return lyrics, timestamps
        
    def get_lyrics_from_tags(self):
        """Extract lyrics from MP3 tags"""
        self.pbar.set_description("Reading lyrics from tags")
        try:
            audio = MP3(self.input_path, ID3=ID3)
            if audio.tags is None:
                return None
                
            for tag in ['USLT::', 'LYRICS:', 'LYRICS']:
                if tag in audio.tags:
                    lyrics = str(audio.tags[tag].text)
                    logger.info(f"Found lyrics in tag: {tag}")
                    return lyrics
                    
            logger.info("No lyrics found in audio tags")
            return None
        except Exception as e:
            logger.warning(f"Error reading lyrics from tags: {e}")
            return None
            
    def isolate_vocals(self):
        """Extract vocals from the audio file using Spleeter"""
        self.pbar.set_description("Isolating vocals")
        with tempfile.TemporaryDirectory() as temp_dir:
            self.separator.separate_to_file(
                self.input_path,
                temp_dir,
                filename_format='{instrument}.wav'
            )
            
            vocals_path = os.path.join(temp_dir, 'vocals.wav')
            vocals, sr = librosa.load(vocals_path, sr=None, mono=True)
            
            logger.debug(f"Isolated vocals: {len(vocals)} samples at {sr}Hz")
            return vocals, sr
            
    def detect_vocal_segments(self, vocals, sr):
        """Detect segments with vocal activity"""
        self.pbar.set_description("Detecting vocal segments")
        frame_length = int(sr * 0.05)
        hop_length = int(sr * 0.025)
        
        rms = librosa.feature.rms(y=vocals, frame_length=frame_length, hop_length=hop_length)[0]
        threshold = np.mean(rms) * 1.1
        vocal_segments = rms > threshold
        
        segment_times = librosa.frames_to_time(
            np.arange(len(rms)),
            sr=sr,
            hop_length=hop_length
        )
        
        onset_frames = np.where(np.diff(vocal_segments.astype(int)) > 0)[0]
        onset_times = segment_times[onset_frames]
        
        filtered_times = self.filter_timestamps(onset_times)
        logger.debug(f"Detected {len(filtered_times)} vocal segments")
        return filtered_times
        
    def filter_timestamps(self, timestamps, min_gap=0.5):
        """Filter timestamps to remove those that are too close together"""
        if len(timestamps) == 0:
            return np.array([])
            
        filtered = [timestamps[0]]
        for t in timestamps[1:]:
            if t - filtered[-1] >= min_gap:
                filtered.append(t)
        return np.array(filtered)
        
    def generate_lrc_content(self, lyrics, timestamps):
        """Generate LRC file content"""
        self.pbar.set_description("Generating LRC content")
        lrc_lines = []
        
        audio = MP3(self.input_path, ID3=ID3)
        if audio.tags:
            if 'TIT2' in audio.tags:
                lrc_lines.append(f"[ti:{audio.tags['TIT2']}]")
            if 'TPE1' in audio.tags:
                lrc_lines.append(f"[ar:{audio.tags['TPE1']}]")
            if 'TALB' in audio.tags:
                lrc_lines.append(f"[al:{audio.tags['TALB']}]")
                
        lrc_lines.append("[by:LRCGenerator]")
        lrc_lines.append("")
        
        for timestamp, line in zip(timestamps, lyrics):
            minutes = int(timestamp // 60)
            seconds = int(timestamp % 60)
            milliseconds = int((timestamp % 1) * 100)
            time_str = f"[{minutes:02d}:{seconds:02d}.{milliseconds:02d}]"
            lrc_lines.append(f"{time_str}{line}")
            
        return "\n".join(lrc_lines)
        
    def process_file(self) -> ProcessingResult:
        """Process a single audio file"""
        start_time = time.time()
        
        try:
            logger.info(f"Processing: {self.input_path}")
            self.pbar = tqdm(total=100, desc="Starting", unit="%")
            
            # Extract vocals
            vocals, sr = self.isolate_vocals()
            self.pbar.update(20)
            
            # Get lyrics
            lyrics_text = self.get_lyrics_from_tags()
            self.pbar.update(20)
            
            if lyrics_text:
                logger.info("Using lyrics from audio file tags")
                lyrics = [line.strip() for line in lyrics_text.split('\n') if line.strip()]
                timestamps = self.detect_vocal_segments(vocals, sr)
                
                if len(timestamps) < len(lyrics):
                    total_duration = librosa.get_duration(y=vocals, sr=sr)
                    timestamps = np.linspace(0, total_duration, len(lyrics))
            else:
                logger.info("Generating lyrics from audio")
                lyrics, timestamps = self.extract_lyrics_from_audio(vocals)
            
            self.pbar.update(30)
            
            # Generate LRC content
            lrc_content = self.generate_lrc_content(lyrics, timestamps)
            
            # Create output file
            output_path = self.get_output_path()
            
            # Copy original file
            shutil.copy2(self.input_path, output_path)
            
            # Add synchronized lyrics
            audio = MP3(output_path, ID3=ID3)
            if audio.tags is None:
                audio.add_tags()
                
            # Add USLT tag
            audio.tags.add(
                USLT(encoding=3, lang='eng', desc='', text='\n'.join(lyrics))
            )
            
            # Add SYLT tag
            sylt_data = []
            for timestamp, lyric in zip(timestamps, lyrics):
                time_ms = int(timestamp * 1000)
                sylt_data.append((lyric.encode(), time_ms))
                
            audio.tags.add(
                SYLT(encoding=Encoding.UTF8, lang='eng', format=2, type=1,
                     text=sylt_data)
            )
            
            audio.save()
            
            # Save LRC file
            lrc_path = os.path.splitext(output_path)[0] + '.lrc'
            with open(lrc_path, 'w', encoding='utf-8') as f:
                f.write(lrc_content)
                
            self.pbar.update(30)
            self.pbar.close()
            
            processing_time = time.time() - start_time
            logger.info(f"Successfully processed {self.input_path}")
            
            return ProcessingResult(
                filename=os.path.basename(self.input_path),
                success=True,
                processing_time=processing_time,
                output_mp3=output_path,
                output_lrc=lrc_path
            )
            
        except Exception as e:
            if self.pbar:
                self.pbar.close()
            logger.error(f"Error processing {self.input_path}: {str(e)}")
            return ProcessingResult(
                filename=os.path.basename(self.input_path),
                success=False,
                error_message=str(e),
                processing_time=time.time() - start_time
            )

def print_summary(results: List[ProcessingResult]):
    """Print a summary of processing results"""
    print("\nProcessing Summary:")
    print("=" * 80)
    
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    
    print(f"\nProcessed {len(results)} files:")
    print(f"✓ Successful: {len(successful)}")
    print(f"✗ Failed: {len(failed)}")
    
    if successful:
        print("\nSuccessful files:")
        for result in successful:
            print(f"\n- {result.filename}")
            print(f"  Processing time: {result.processing_time:.1f} seconds")
            print(f"  Output MP3: {os.path.basename(result.output_mp3)}")
            print(f"  Output LRC: {os.path.basename(result.output_lrc)}")
    
    if failed:
        print("\nFailed files:")
        for result in failed:
            print(f"\n- {result.filename}")
            print(f"  Error: {result.error_message}")
            print(f"  Processing time: {result.processing_time:.1f} seconds")

def main():
    # Get files from command line arguments
    input_files = sys.argv[1:]
    
    if not input_files:
        logger.warning("No input files provided. Usage: python script.py file1.mp3 file2.mp3 ...")
        return
        
    results = []
    
    # Process each file
    for file_path in input_files:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            results.append(ProcessingResult(
                filename=file_path,
                success=False,
                error_message="File not found"
            ))
            continue
            
        if not file_path.lower().endswith('.mp3'):
            logger.error(f"Unsupported file format: {file_path}")
            results.append(ProcessingResult(
                filename=file_path,
                success=False,
                error_message="Unsupported file format"
            ))
            continue
            
        generator = LRCGenerator(file_path)
        results.append(generator.process_file())
    
    # Print summary
    print_summary(results)
    
    # Wait for user input before closing
    input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()

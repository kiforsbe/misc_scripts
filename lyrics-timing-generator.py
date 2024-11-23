import sys
import os
import logging
from mutagen import File
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3, USLT, SYLT, Encoding
import numpy as np
import librosa
import warnings
import torch
from demucs.pretrained import get_model
from demucs.apply import apply_model
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
    output_file: str = None
    output_lrc: str = None

class AudioProcessor:
    def __init__(self, input_path):
        self.input_path = input_path
        self.demucs_model = get_model('htdemucs')
        self.demucs_model.eval()
        if torch.cuda.is_available():
            self.demucs_model.cuda()
        self.whisper_model = whisper.load_model("base")
        self.pbar = None
        try:
            self.audio_file = EasyID3(input_path)
        except:
            self.audio_file = File(input_path)
    
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
        """Extract lyrics from audio file tags"""
        self.pbar.set_description("Reading lyrics from tags")
        try:
            # Try to get lyrics from ID3 tags
            if isinstance(self.audio_file, EasyID3):
                audio = ID3(self.input_path)
                for tag in audio.getall('USLT'):
                    if tag.text and tag.text.strip():
                        logger.info("Found lyrics in USLT tag")
                        return tag.text
            
            logger.info("No lyrics found in audio tags")
            return None
            
        except Exception as e:
            logger.warning(f"Error reading lyrics from tags: {e}")
            return None
            
    def isolate_vocals(self):
        """Extract vocals from the audio file using Demucs"""
        self.pbar.set_description("Isolating vocals")
        
        # Load audio using librosa
        audio, sr = librosa.load(self.input_path, sr=44100, mono=False)
        if audio.ndim == 1:
            audio = np.stack([audio, audio])
            
        # Convert to torch tensor
        audio_tensor = torch.tensor(audio, dtype=torch.float32)
        
        # Add batch dimension
        audio_tensor = audio_tensor.unsqueeze(0)
        
        # Move to GPU if available
        if torch.cuda.is_available():
            audio_tensor = audio_tensor.cuda()
            
        # Apply Demucs
        with torch.no_grad():
            sources = apply_model(self.demucs_model, audio_tensor, progress=True)[0]
            
        # Get vocals (sources order: drums, bass, other, vocals)
        vocals = sources[3].cpu().numpy()
        
        # Convert to mono if needed
        if vocals.ndim > 1:
            vocals = np.mean(vocals, axis=0)
            
        logger.debug(f"Isolated vocals: {len(vocals)} samples at {sr}Hz")
        return vocals, sr
            
    def detect_vocal_segments(self, vocals, sr):
        """Detect segments with vocal activity"""
        self.pbar.set_description("Detecting vocal segments")
        frame_length = int(sr * 0.05)  # 50ms frames
        hop_length = int(sr * 0.025)   # 25ms hop
        
        # Calculate RMS energy
        rms = librosa.feature.rms(y=vocals, frame_length=frame_length, hop_length=hop_length)[0]
        
        # Dynamic thresholding
        threshold = np.mean(rms) * 1.1 + np.std(rms) * 0.5
        vocal_segments = rms > threshold
        
        # Convert frames to timestamps
        segment_times = librosa.frames_to_time(
            np.arange(len(rms)),
            sr=sr,
            hop_length=hop_length
        )
        
        # Find onset frames
        onset_frames = np.where(np.diff(vocal_segments.astype(int)) > 0)[0]
        onset_times = segment_times[onset_frames]
        
        # Filter timestamps
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
        
        # Add metadata
        if isinstance(self.audio_file, EasyID3):
            title = self.audio_file.get('title', [''])[0]
            artist = self.audio_file.get('artist', [''])[0]
            album = self.audio_file.get('album', [''])[0]
            
            if title:
                lrc_lines.append(f"[ti:{title}]")
            if artist:
                lrc_lines.append(f"[ar:{artist}]")
            if album:
                lrc_lines.append(f"[al:{album}]")
                
        lrc_lines.append("[by:LRCGenerator]")
        lrc_lines.append("")
        
        # Add synchronized lyrics
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
            self.pbar = tqdm(
                total=100,
                desc="Starting",
                unit="%",
                bar_format='{l_bar}{bar}| {n:.0f}/{total_fmt} [{elapsed}<{remaining}]'
            )
            
            # Extract vocals
            vocals, sr = self.isolate_vocals()
            self.pbar.update(round(20))
            
            # Get lyrics
            lyrics_text = self.get_lyrics_from_tags()
            self.pbar.update(round(20))
            
            if lyrics_text:
                logger.info("Using lyrics from audio file tags")
                lyrics = [line.strip() for line in lyrics_text.split('\n') if line.strip()]
                timestamps = self.detect_vocal_segments(vocals, sr)
                
                # If we detect fewer vocal segments than lyrics lines,
                # distribute timestamps evenly
                if len(timestamps) < len(lyrics):
                    total_duration = librosa.get_duration(y=vocals, sr=sr)
                    timestamps = np.linspace(0, total_duration, len(lyrics))
            else:
                logger.info("Generating lyrics from audio")
                lyrics, timestamps = self.extract_lyrics_from_audio(vocals)
            
            self.pbar.update(round(30))
            
            # Generate LRC content
            lrc_content = self.generate_lrc_content(lyrics, timestamps)
            
            # Create output file
            output_path = self.get_output_path()
            
            # Copy original file
            shutil.copy2(self.input_path, output_path)
            
            # Update output file with new tags
            try:
                audio = ID3(output_path)
            except:
                audio = File(output_path)
                if audio is None:
                    raise ValueError("Unsupported audio format")
            
            # Add unsynced lyrics
            uslt = USLT(encoding=Encoding.UTF8, lang='eng', desc='', text='\n'.join(lyrics))
            audio.add(uslt)
            
            # Add synced lyrics
            # Convert timestamps to milliseconds and create tuples
            sylt_frames = []
            for timestamp, lyric in zip(timestamps, lyrics):
                time_ms = int(timestamp * 1000)  # Convert to milliseconds
                sylt_frames.append((lyric, time_ms))
            
            sylt = SYLT(encoding=Encoding.UTF8, lang='eng', format=2, type=1, desc='', text=sylt_frames)
            audio.add(sylt)
            
            # Save changes
            audio.save()
            
            # Save LRC file
            lrc_path = os.path.splitext(output_path)[0] + '.lrc'
            with open(lrc_path, 'w', encoding='utf-8') as f:
                f.write(lrc_content)
                
            self.pbar.update(round(30))
            self.pbar.close()
            
            processing_time = time.time() - start_time
            logger.info(f"Successfully processed {self.input_path}")
            
            return ProcessingResult(
                filename=os.path.basename(self.input_path),
                success=True,
                processing_time=processing_time,
                output_file=output_path,
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
            print(f"  Output file: {os.path.basename(result.output_file)}")
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
            
        try:
            processor = AudioProcessor(file_path)
            results.append(processor.process_file())
        except Exception as e:
            logger.error(f"Error initializing processor for {file_path}: {str(e)}")
            results.append(ProcessingResult(
                filename=file_path,
                success=False,
                error_message=f"Failed to initialize processor: {str(e)}"
            ))
    
    # Print summary
    print_summary(results)
    
    # Wait for user input before closing
    input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()

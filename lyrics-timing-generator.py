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
import requests
import json
import re

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

from tqdm import tqdm

class CustomTqdm(tqdm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def format_meter(self, n, total, elapsed, ncols=None, prefix='', ascii=False, unit='it', unit_scale=False, rate=None, bar_format=None, postfix=None, unit_divisor=1000, initial=0, **extra_kwargs):
        # Convert float progress to integers
        n = int(n)
        total = int(total)
        return super().format_meter(n, total, elapsed, ncols, prefix, ascii, unit, unit_scale, rate, bar_format, postfix, unit_divisor, initial, **extra_kwargs)

class LyricsCleanerOllama:
    def __init__(self, host="http://localhost:11434"):
        """Initialize Ollama client with host URL"""
        self.host = host
        self.api_generate = f"{host}/api/generate"
        
    def clean_lyrics(self, lyrics: str) -> str:
        """
        Clean lyrics using Ollama with Llama model
        Removes section markers and normalizes formatting
        """
        prompt = """
        Clean up these song lyrics by:
        1. Remove all section markers like [Verse], [Chorus], [Bridge], etc.
        2. Remove any empty lines between sections
        3. Keep only the actual lyrics
        4. Maintain line breaks between different lines
        5. Return only the cleaned lyrics, no explanations

        Lyrics to clean:
        {lyrics}
        """.format(lyrics=lyrics)

        try:
            response = requests.post(
                self.api_generate,
                json={
                    "model": "llama3.1",
                    "prompt": prompt,
                    "stream": False
                },
                timeout=30
            )
            response.raise_for_status()
            
            # Parse response and extract cleaned lyrics
            result = response.json()
            cleaned_lyrics = result.get('response', '').strip()
            
            # Additional cleaning with regex for any remaining brackets
            cleaned_lyrics = re.sub(r'\[[^\]]*\]', '', cleaned_lyrics)
            
            # Remove multiple consecutive empty lines
            cleaned_lyrics = re.sub(r'\n\s*\n', '\n', cleaned_lyrics)
            
            return cleaned_lyrics.strip()
            
        except Exception as e:
            logger.warning(f"Error cleaning lyrics with Ollama: {str(e)}")
            # If Ollama fails, fall back to basic regex cleaning
            return self.basic_cleanup(lyrics)
            
    def basic_cleanup(self, lyrics: str) -> str:
        """
        Fallback cleanup method using regex
        Used when Ollama is unavailable or fails
        """
        # Remove section markers
        cleaned = re.sub(r'\[[^\]]*\]', '', lyrics)
        # Remove empty lines
        cleaned = re.sub(r'\n\s*\n', '\n', cleaned)
        return cleaned.strip()

class AudioProcessor:
    def __init__(self, input_path):
        self.input_path = input_path
        self.demucs_model = get_model('htdemucs')
        self.demucs_model.eval()
        if torch.cuda.is_available():
            self.demucs_model.cuda()
        self.whisper_model = whisper.load_model("base")
        self.lyrics_cleaner = LyricsCleanerOllama()
        self.pbar = CustomTqdm(
            total=100,
            desc="Starting",
            unit="%",
            bar_format='{l_bar}{bar}| {n:.0f}/{total_fmt} [{elapsed}<{remaining}]'
        )
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
            
        # Clean the extracted lyrics
        cleaned_lyrics = self.lyrics_cleaner.clean_lyrics("\n".join(lyrics))
        lyrics = [line.strip() for line in cleaned_lyrics.split('\n') if line.strip()]
            
        logger.debug(f"Extracted and cleaned {len(lyrics)} lyrics segments")
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
                        # Clean the lyrics before returning
                        return self.lyrics_cleaner.clean_lyrics(tag.text)
            
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
            
    def align_lyrics_with_audio(self, lyrics, vocals, sr):
        """
        Improved alignment of existing lyrics with audio using energy-based segmentation
        """
        self.pbar.set_description("Aligning lyrics with audio")
        
        # Calculate frame and hop lengths
        hop_length = int(sr * 0.010)  # 10ms hop
        frame_length = int(2**np.ceil(np.log2(sr * 0.025)))  # Next power of 2 of 25ms frame
        
        try:
            # Get mel spectrogram with adjusted parameters
            mel_spec = librosa.feature.melspectrogram(
                y=vocals,
                sr=sr,
                n_mels=80,
                n_fft=frame_length,
                hop_length=hop_length,
                win_length=frame_length
            )
            
            # Convert to log scale
            mel_spec = librosa.power_to_db(mel_spec, ref=np.max)
            mel_energy = np.mean(mel_spec, axis=0)
            
        except Exception as e:
            logger.warning(f"Mel spectrogram calculation failed: {e}. Falling back to RMS energy.")
            mel_energy = None
        
        # Calculate RMS energy
        rms = librosa.feature.rms(
            y=vocals, 
            frame_length=frame_length,
            hop_length=hop_length,
            center=True
        )[0]
        
        # Combine mel and RMS features if available
        if mel_energy is not None:
            # Normalize both features
            mel_energy = (mel_energy - mel_energy.mean()) / mel_energy.std()
            rms_norm = (rms - rms.mean()) / rms.std()
            
            # Combine features
            energy = (mel_energy + rms_norm) / 2
        else:
            energy = rms
        
        # Smooth energy curve
        window_size = int(sr * 0.1 / hop_length)  # 100ms window
        energy_smooth = np.convolve(energy, np.ones(window_size)/window_size, mode='same')
        
        # Find potential line boundaries using peak detection
        from scipy.signal import find_peaks
        
        # Calculate dynamic prominence threshold based on energy distribution
        prominence_threshold = np.percentile(energy_smooth, 75) - np.percentile(energy_smooth, 25)
        
        # Find peaks (potential line starts)
        peaks, _ = find_peaks(
            energy_smooth,
            distance=int(sr * 0.5 / hop_length),  # Minimum 0.5s between peaks
            prominence=prominence_threshold,
            height=np.mean(energy_smooth)
        )
        
        # Convert peak frames to timestamps
        timestamps = librosa.frames_to_time(peaks, sr=sr, hop_length=hop_length)
        
        # Ensure we have reasonable number of timestamps
        if len(timestamps) < len(lyrics) * 0.5:  # Too few timestamps
            logger.warning("Too few vocal segments detected, using time-based distribution")
            # Create evenly spaced timestamps
            total_duration = librosa.get_duration(y=vocals, sr=sr)
            timestamps = np.linspace(0, total_duration * 0.95, len(lyrics))
            
        elif len(timestamps) > len(lyrics) * 2:  # Too many timestamps
            logger.info("Merging nearby timestamps")
            # Merge nearby timestamps
            merged = []
            current_group = [timestamps[0]]
            
            for t in timestamps[1:]:
                if t - current_group[-1] < 0.3:  # Merge if gap is less than 300ms
                    current_group.append(t)
                else:
                    merged.append(np.mean(current_group))
                    current_group = [t]
                    
            if current_group:
                merged.append(np.mean(current_group))
                
            timestamps = np.array(merged)
            
            # If still too many, take evenly spaced samples
            if len(timestamps) > len(lyrics):
                indices = np.round(np.linspace(0, len(timestamps) - 1, len(lyrics))).astype(int)
                timestamps = timestamps[indices]
        
        # Ensure timestamps start near the beginning of the audio
        if timestamps[0] > 1.0:  # If first timestamp is more than 1 second in
            timestamps = np.insert(timestamps, 0, 0.0)  # Add timestamp at start
            
        # Trim to match number of lyrics
        timestamps = timestamps[:len(lyrics)]
        
        # Ensure we have exactly one timestamp per lyric line
        if len(timestamps) < len(lyrics):
            # Fill remaining timestamps by interpolation
            total_duration = librosa.get_duration(y=vocals, sr=sr)
            missing_count = len(lyrics) - len(timestamps)
            additional_timestamps = np.linspace(
                timestamps[-1] if len(timestamps) > 0 else 0,
                total_duration * 0.95,
                missing_count + 1
            )[1:]
            timestamps = np.concatenate([timestamps, additional_timestamps])
        
        return timestamps
        
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
            
            # Extract vocals
            vocals, sr = self.isolate_vocals()
            self.pbar.update(round(20))
            
            # Get lyrics
            lyrics_text = self.get_lyrics_from_tags()
            self.pbar.update(round(20))
            
            if lyrics_text:
                logger.info("Using lyrics from audio file tags")
                lyrics = [line.strip() for line in lyrics_text.split('\n') if line.strip()]
                timestamps = self.align_lyrics_with_audio(lyrics, vocals, sr)
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

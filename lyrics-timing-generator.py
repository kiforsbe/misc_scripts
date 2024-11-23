from pathlib import Path
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
import whisperx
import shutil
from tqdm import tqdm
from dataclasses import dataclass
from typing import List, TextIO
import time
import requests
import json
import re

import whisperx.utils

# Suppress unnecessary warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s [%(module)s:%(funcName)s:%(lineno)d]',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

logging.getLogger('numpy').setLevel(logging.ERROR)
logging.getLogger('numba').setLevel(logging.ERROR)

whisper_model = "base"
device = "cuda" 
whisper_batch_size = 16 # reduce if low on GPU mem
whisper_compute_type = "float16" # change to "int8" if low on GPU mem (may reduce accuracy)
whisper_asr_options = {
    "hotwords": [],
    "multilingual": False,
    "word_timestamps": True,
}

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
        n = int(round(n,1))
        total = int(round(total,1))
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
        self.whisper_model = whisperx.load_model(
            "base",
            device,
            compute_type=whisper_compute_type,
            asr_options=whisper_asr_options
        )
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
    
    def extract_lyrics_from_audio(self, vocals, sr):
        """Extract lyrics from vocals using Whisper with word-level timestamps"""
        logger.info("Extracting lyrics using Whisper")
        self.pbar.set_description("Transcribing audio")

        with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_file:
            vocals_16k = librosa.resample(vocals, orig_sr=sr, target_sr=16000)
            sf.write(temp_file.name, vocals_16k, 16000)  # WhisperX expects 16kHz

            result = self.whisper_model.transcribe(
                temp_file.name,
                batch_size=whisper_batch_size
            )
        
            # 2. Align whisper output
            model_a, metadata = whisperx.load_align_model(language_code=result["language"], device=device)
            result = whisperx.align(result["segments"], model_a, metadata, temp_file.name, device, return_char_alignments=False)
            
        return result
        
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
        
        # Normalize audio
        vocals = vocals / np.max(np.abs(vocals))
        
        # Apply some basic audio preprocessing
        # High-pass filter to remove low frequency noise
        vocals = librosa.effects.preemphasis(vocals)
        
        logger.debug(f"Isolated vocals: {len(vocals)} samples at {sr}Hz")
        return vocals, sr
            
    def align_lyrics_with_audio(self, lyrics_lines, vocals, sr):
        """Align existing lyrics with audio using WhisperX's word timestamps"""
        self.pbar.set_description("Aligning lyrics with audio")
        
        # Get WhisperX transcription with word timestamps
        whisper_result = self.extract_lyrics_from_audio(vocals, sr)
        
        # Prepare lyrics for comparison by cleaning and tokenizing
        def clean_text(text):
            """Clean text for comparison by removing punctuation and converting to lowercase"""
            return re.sub(r'[^\w\s]', '', text.lower()).strip()
        
        def get_words_from_line(line):
            """Split line into words and clean each word"""
            return [clean_text(word) for word in line.split() if clean_text(word)]
        
        # Process lyrics lines into cleaned words with line indices
        lyrics_words = []
        lyrics_line_indices = []
        for i, line in enumerate(lyrics_lines):
            words = get_words_from_line(line)
            lyrics_words.extend(words)
            lyrics_line_indices.extend([i] * len(words))
        
        # Extract words and timestamps from WhisperX result
        whisper_words = []
        word_timestamps = []
        
        for segment in whisper_result["segments"]:
            if "words" in segment:
                for word_data in segment["words"]:
                    cleaned_word = clean_text(word_data["word"])
                    if cleaned_word:  # Only add non-empty words
                        whisper_words.append(cleaned_word)
                        word_timestamps.append(word_data["start"])
        
        # Sliding window matching to find best alignment for each lyrics line
        line_timestamps = []
        current_whisper_idx = 0
        
        for line_idx in range(len(lyrics_lines)):
            # Get words for current line
            line_start_idx = lyrics_line_indices.index(line_idx)
            try:
                line_end_idx = len(lyrics_line_indices) - 1 - lyrics_line_indices[::-1].index(line_idx)
            except ValueError:
                line_end_idx = line_start_idx
            
            line_words = lyrics_words[line_start_idx:line_end_idx + 1]
            if not line_words:
                continue
            
            # Find best match for current line in remaining whisper words
            best_match_score = 0
            best_match_idx = current_whisper_idx
            
            # Look ahead in whisper words to find best match
            for i in range(current_whisper_idx, len(whisper_words) - len(line_words) + 1):
                current_score = 0
                whisper_segment = whisper_words[i:i + len(line_words)]
                
                # Calculate matching score
                for w1, w2 in zip(line_words, whisper_segment):
                    if w1 == w2:
                        current_score += 1
                    elif w1 in w2 or w2 in w1:  # Partial match
                        current_score += 0.5
                
                # Normalize score by line length
                current_score /= len(line_words)
                
                if current_score > best_match_score:
                    best_match_score = current_score
                    best_match_idx = i
            
            # If we found a good match, use its timestamp
            if best_match_score > 0.3:  # Threshold for accepting a match
                line_timestamps.append(word_timestamps[best_match_idx])
                current_whisper_idx = best_match_idx + len(line_words)
            else:
                # If no good match found, interpolate timestamp
                if line_timestamps:
                    # Interpolate based on previous timestamp
                    last_timestamp = line_timestamps[-1]
                    avg_line_duration = 2.0  # Assume average line duration of 2 seconds
                    line_timestamps.append(last_timestamp + avg_line_duration)
                else:
                    # For first line with no match, use beginning of audio
                    line_timestamps.append(word_timestamps[0] if word_timestamps else 0.0)
        
        # Handle case where we have fewer timestamps than lyrics lines
        if len(line_timestamps) < len(lyrics_lines):
            missing_count = len(lyrics_lines) - len(line_timestamps)
            last_timestamp = line_timestamps[-1] if line_timestamps else 0
            total_duration = whisper_result["segments"][-1]["end"] if whisper_result["segments"] else last_timestamp + 2.0
            
            # Generate evenly spaced timestamps for remaining lines
            additional_timestamps = np.linspace(
                last_timestamp + 2.0,  # Start 2 seconds after last timestamp
                total_duration * 0.95,  # End slightly before audio end
                missing_count + 1
            )[:-1]  # Remove last point to avoid going too close to end
            
            line_timestamps.extend(additional_timestamps)
        
        # Filter timestamps to ensure minimum gap
        return self.filter_timestamps(line_timestamps, min_gap=0.5)
        
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
        
    def process_file(self):
        """Process a single audio file"""
        start_time = time.time()
        
        try:
            logger.info(f"Processing: {self.input_path}")
            
            # Extract vocals
            vocals, sr = self.isolate_vocals()
            self.pbar.update(20)
            
            # Get lyrics
            lyrics_text = self.get_lyrics_from_tags()
            self.pbar.update(20)
            
            if lyrics_text:
                logger.info("Using lyrics from audio file tags")
                lyrics = [line.strip() for line in lyrics_text.split('\n') if line.strip()]
                timestamps = self.align_lyrics_with_audio(lyrics, vocals, sr)
            else:
                logger.info("Generating lyrics from audio")
                result = self.extract_lyrics_from_audio(vocals)
                
                # Extract lyrics and timestamps from segments
                lyrics = []
                timestamps = []
                for segment in result["segments"]:
                    if segment["text"].strip():
                        lyrics.append(segment["text"].strip())
                        timestamps.append(segment["start"])
            
            self.pbar.update(30)
            
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

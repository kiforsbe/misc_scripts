from dataclasses import dataclass
from abc import ABC, abstractmethod
import logging
import eyed3
from mutagen.mp4 import MP4, MP4Cover
from typing import Optional, Union, Required

@dataclass
class AudioMetadata:
    """Container for all possible audio metadata"""
    title: Optional[str] = None
    artist: Optional[str] = None
    album: Optional[str] = None
    genre: Optional[str] = None
    year: Optional[int] = None
    lyrics: Optional[str] = None
    canonical_url: Optional[str] = None
    cover_art: Optional[bytes] = None

class AudioMetadataWriter(ABC):
    """Abstract base class for audio metadata writers"""
    @abstractmethod
    def write_metadata(self, filepath: str, metadata: AudioMetadata) -> None:
        pass

class MP3MetadataWriter(AudioMetadataWriter):
    def write_metadata(self, filepath: str, metadata: AudioMetadata) -> None:
        audio = eyed3.load(filepath)
        if audio is None:
            raise ValueError("Failed to load MP3 file")

        if audio.tag is None:
            audio.initTag(version=eyed3.id3.ID3_V2_3)

        # Set basic metadata
        if metadata.title:
            audio.tag.title = metadata.title
        if metadata.artist:
            audio.tag.artist = metadata.artist
        if metadata.album:
            audio.tag.album = metadata.album
        if metadata.genre:
            audio.tag.genre = metadata.genre

        # Set year
        if metadata.year:
            try:
                audio.tag.recording_date = eyed3.core.Date(metadata.year)
            except ValueError:
                logging.warning(f"Invalid year: {metadata.year}")

        # Set canonical URL
        if metadata.canonical_url:
            audio.tag.audio_file_url = metadata.canonical_url

        # Set lyrics
        if metadata.lyrics:
            try:
                audio.tag.lyrics.set(metadata.lyrics, description=u"", lang=b"eng")
            except Exception as e:
                logging.error(f"Error setting lyrics: {e}")

        # Set cover art
        if metadata.cover_art:
            # Remove existing covers
            existing_covers = [img for img in audio.tag.images if img.picture_type == eyed3.id3.frames.ImageFrame.FRONT_COVER]
            for cover in existing_covers:
                audio.tag.images.remove(cover.description)
            
            audio.tag.images.set(
                eyed3.id3.frames.ImageFrame.FRONT_COVER,
                metadata.cover_art,
                'image/jpeg',
                u'Cover'
            )

        # Save changes
        audio.tag.save(version=eyed3.id3.ID3_V2_3, encoding='utf-8')

class M4AMetadataWriter(AudioMetadataWriter):
    def write_metadata(self, filepath: str, metadata: AudioMetadata) -> None:
        audio = MP4(filepath)

        # Set basic metadata
        if metadata.title:
            audio['\xa9nam'] = [metadata.title]
        if metadata.artist:
            audio['\xa9ART'] = [metadata.artist]
        if metadata.album:
            audio['\xa9alb'] = [metadata.album]
        if metadata.genre:
            audio['\xa9gen'] = [metadata.genre]

        # Set year
        if metadata.year:
            audio['\xa9day'] = [str(metadata.year)]

        # Set lyrics
        if metadata.lyrics:
            audio['\xa9lyr'] = [metadata.lyrics]

        # Set canonical URL
        if metadata.canonical_url:
            audio['\xa9url'] = [metadata.canonical_url]

        # Set cover art
        if metadata.cover_art:
            # Always use JPEG for M4A
            cover = MP4Cover(metadata.cover_art, imageformat=MP4Cover.FORMAT_JPEG)
            audio['covr'] = [cover]

        audio.save()

def get_metadata_writer(file_path: str) -> AudioMetadataWriter:
    """Factory function to get the appropriate metadata writer"""
    if file_path.lower().endswith('.m4a'):
        return M4AMetadataWriter()
    elif file_path.lower().endswith('.mp3'):
        return MP3MetadataWriter()
    else:
        raise ValueError(f"Unsupported audio format: {file_path}")

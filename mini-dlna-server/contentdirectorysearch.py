import logging
import os.path
from whoosh.fields import Schema, TEXT, ID, STORED
from whoosh.analysis import StandardAnalyzer
from whoosh.index import create_in, exists_in, open_dir

class ContentDirectorySearch:
    """Handles indexed search functionality for the Content Directory Service"""
    def __init__(self, media_folders):
        self.logger = logging.getLogger('DLNAServer')
        self.media_folders = media_folders
        self.index = {}
        self.metadata_cache = {}
        self._build_index()

    def _build_index(self):
        """Build search index from media folders"""
        try:    
            # Create schema for media indexing
            self.schema = Schema(
                path=ID(stored=True),
                filename=TEXT(stored=True, analyzer=StandardAnalyzer()),
                title=TEXT(stored=True),
                artist=TEXT(stored=True),
                album=TEXT(stored=True),
                genre=TEXT(stored=True),
                type=TEXT(stored=True)
            )
            
            # Create or open index
            index_dir = os.path.join(os.path.dirname(__file__), 'search_index')
            if not os.path.exists(index_dir):
                os.makedirs(index_dir)
                
            if exists_in(index_dir):
                self.ix = open_dir(index_dir)
            else:
                self.ix = create_in(index_dir, self.schema)
            
            # Index all media files
            writer = self.ix.writer()
            
            for folder in self.media_folders:
                for root, _, files in os.walk(folder):
                    for file in files:
                        try:
                            ext = os.path.splitext(file)[1].lower()
                            if ext in VIDEO_EXTENSIONS or ext in AUDIO_EXTENSIONS or ext in IMAGE_EXTENSIONS:
                                full_path = os.path.join(root, file)
                                metadata = self._extract_metadata(full_path)
                                
                                # Store in index
                                writer.add_document(
                                    path=full_path,
                                    filename=file,
                                    title=metadata.get('title', file),
                                    artist=metadata.get('artist', ''),
                                    album=metadata.get('album', ''),
                                    genre=metadata.get('genre', ''),
                                    type=self._get_media_type(ext)
                                )
                                
                                # Cache metadata
                                self.metadata_cache[full_path] = metadata
                                
                        except Exception as e:
                            self.logger.warning(f"Error indexing {file}: {e}")
                            
            writer.commit()
            self.logger.info("Search index built successfully")
            
        except ImportError:
            self.logger.warning("Whoosh not installed, falling back to basic search")
            self._build_basic_index()

    def _build_basic_index(self):
        """Build a simple in-memory index when Whoosh is not available"""
        for folder in self.media_folders:
            for root, _, files in os.walk(folder):
                for file in files:
                    ext = os.path.splitext(file)[1].lower()
                    if ext in VIDEO_EXTENSIONS or ext in AUDIO_EXTENSIONS or ext in IMAGE_EXTENSIONS:
                        full_path = os.path.join(root, file)
                        self.index[full_path] = {
                            'filename': file,
                            'type': self._get_media_type(ext),
                            'metadata': self._extract_metadata(full_path)
                        }

    def search(self, query, media_type=None, limit=50):
        """Search for media items matching query"""
        try:
            from whoosh.qparser import MultifieldParser
            from whoosh.query import Term
            
            with self.ix.searcher() as searcher:
                query_fields = ['filename', 'title', 'artist', 'album', 'genre']
                parser = MultifieldParser(query_fields, self.schema)
                q = parser.parse(query)
                
                # Add media type filter if specified
                if media_type:
                    q = q & Term('type', media_type)
                
                results = searcher.search(q, limit=limit)
                return [(hit['path'], hit.score, hit['title']) for hit in results]
                
        except ImportError:
            return self._basic_search(query, media_type, limit)

    def _basic_search(self, query, media_type=None, limit=50):
        """Basic search implementation without Whoosh"""
        query = query.lower()
        results = []
        
        for path, info in self.index.items():
            if media_type and info['type'] != media_type:
                continue
                
            score = 0
            metadata = info['metadata']
            
            # Check filename
            if query in info['filename'].lower():
                score += 1
                
            # Check metadata
            for field in ['title', 'artist', 'album', 'genre']:
                if field in metadata and query in metadata[field].lower():
                    score += 1
                    
            if score > 0:
                results.append((path, score, metadata.get('title', info['filename'])))
                
        # Sort by score and limit results
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:limit]

    def _extract_metadata(self, file_path):
        """Extract metadata from media file"""
        try:
            from mutagen import File
            metadata = {}
            media_file = File(file_path)
            
            if media_file is not None:
                if hasattr(media_file, 'tags'):
                    tags = media_file.tags
                    if tags:
                        metadata['title'] = str(tags.get('title', [''])[0])
                        metadata['artist'] = str(tags.get('artist', [''])[0])
                        metadata['album'] = str(tags.get('album', [''])[0])
                        metadata['genre'] = str(tags.get('genre', [''])[0])
                        
                if hasattr(media_file.info, 'length'):
                    metadata['duration'] = int(media_file.info.length)
                    
            return metadata
            
        except Exception as e:
            self.logger.debug(f"Error extracting metadata from {file_path}: {e}")
            return {}

    def _get_media_type(self, ext):
        """Get media type from file extension"""
        if ext in VIDEO_EXTENSIONS:
            return 'video'
        elif ext in AUDIO_EXTENSIONS:
            return 'audio'
        elif ext in IMAGE_EXTENSIONS:
            return 'image'
        return 'unknown'

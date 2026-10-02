from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING

from .asr import separate_vocals, transcribe_vocals
from .audio import assemble_transcript
from .metadata import fingerprint, has_meaningful_title, read_metadata
from .lyrics import format_lrc, format_srt, format_vtt
from .titles import format_lyrics as format_lyrics_with_model, prepare_lyrics, suggest_titles
from .types import Analysis, Chunk, Settings, TimedWord

if TYPE_CHECKING:
    from .worker_runtime import ModelRuntime

CACHE_SCHEMA = 1


def _version(name):
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return 'uninstalled'


def _key(data):
    return hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()


def _timed_cache_path(root: Path, source: Path, settings: Settings) -> Path:
    key = _key({'source': str(Path(source).resolve()).casefold(), 'backend': settings.asr,
                'checkpoint': settings.checkpoint, 'timing': 'qwen-align-nemotron-duration-v1', 'format': 2})
    return root / 'timed-lyrics' / (key + '.manifest.json')


def _timed_cache_paths(manifest: Path) -> dict[str, Path]:
    base = manifest.name.removesuffix('.manifest.json')
    return {'manifest': manifest, 'words': manifest.with_name(base + '.words.json'),
            'lrc': manifest.with_name(base + '.lrc'), 'srt': manifest.with_name(base + '.srt'),
            'vtt': manifest.with_name(base + '.vtt')}


def _load_timed_cache(path: Path, source_digest: str):
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
        if value.get('schema') != CACHE_SCHEMA or value.get('source_digest') != source_digest:
            return None
        paths = _timed_cache_paths(path)
        words_payload = json.loads(paths['words'].read_text(encoding='utf-8'))
        words = [TimedWord(**item) for item in words_payload['words']]
        formats = {name: paths[name].read_text(encoding='utf-8') for name in ('lrc', 'srt', 'vtt')}
        if (value.get('format_version') != 2 or not words
                or any(not word.text.strip() or not math.isfinite(word.start) or not math.isfinite(word.end)
                       or word.start < 0 or word.end < word.start for word in words)
                or any(current.start < previous.start for previous, current in zip(words, words[1:]))
                or not all(isinstance(v, str) and v.strip() for v in formats.values())):
            return None
        return value, words, formats
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return None


def _save_timed_cache(path: Path, source_digest: str, lyric_text: str, formatted: str,
                      words: list[TimedWord], formats: dict[str, str]):
    paths = _timed_cache_paths(path)
    paths['manifest'].parent.mkdir(parents=True, exist_ok=True)
    atomic_json(paths['words'], {'words': [asdict(word) for word in words]})
    for name, text in formats.items():
        _atomic_text(paths[name], text)
    atomic_json(paths['manifest'], {'schema': CACHE_SCHEMA, 'source_digest': source_digest,
                                    'lyrics': lyric_text, 'formatted_lyrics': formatted,
                                    'format_version': 2})


def _atomic_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as stream:
            stream.write(text.rstrip() + '\n')
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def refresh_timed_cache_source(analysis: Analysis, source: Path, settings: Settings):
    cache_path = Path(analysis.timed_cache_path) if analysis.timed_cache_path else None
    if cache_path is not None and cache_path.is_file():
        try:
            value = json.loads(cache_path.read_text(encoding='utf-8'))
            source_digest = fingerprint(source)
            value['source_digest'] = source_digest
            atomic_json(cache_path, value)
            analysis.digest = source_digest
        except (OSError, ValueError, TypeError):
            pass
    if cache_path is not None and Path(source).resolve() != Path(analysis.source).resolve():
        new_path = _timed_cache_path(settings.cache_dir.resolve(), Path(source), settings)
        old_paths, new_paths = _timed_cache_paths(cache_path), _timed_cache_paths(new_path)
        if cache_path.is_file():
            new_path.parent.mkdir(parents=True, exist_ok=True)
            for name, old_file in old_paths.items():
                if old_file.is_file():
                    os.replace(old_file, new_paths[name])
        analysis.timed_cache_path = str(new_path)


def atomic_json(path: Path, data: dict):
    path.parent.mkdir(parents=True,exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent,suffix='.tmp')
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as stream:
            json.dump(data,stream,indent=2,ensure_ascii=False,default=str)
        os.replace(name,path)
    finally:
        Path(name).unlink(missing_ok=True)


def _read_cache(path):
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        return data if data.get('schema') == CACHE_SCHEMA else None
    except (OSError,ValueError,AttributeError):
        return None


def save_report(analysis: Analysis, settings: Settings, outcome='analyzed', backup=None):
    path = analysis.report_path
    if path is None:
        return
    data = asdict(analysis)
    data.update(settings=asdict(settings),checkpoint=settings.checkpoint,outcome=outcome,backup=backup,
                versions={name:_version(name) for name in ('torch','transformers','demucs')})
    atomic_json(path,data)
    lyrics_text = analysis.formatted_lyrics or analysis.lyrics
    text = f'{analysis.source}\n\n{lyrics_text}\n\nTitle suggestions:\n'
    for number,candidate in enumerate(analysis.candidates,1):
        text += f'{number}. {candidate.title}\n   {candidate.rationale}\n'
    text += f'\nSelected title: {analysis.selected_title or "(none)"}\nOutcome: {outcome}\n'
    if backup:
        text += f'Backup: {backup}\n'
    if analysis.notes:
        text += '\nNotes:\n' + '\n'.join(analysis.notes) + '\n'
    path.with_suffix('.txt').write_text(text,encoding='utf-8')


def analyze_file(path: Path, settings: Settings, progress=print, runtime: ModelRuntime | None = None,
                 force_lyrics: bool = False, ensure_timed_lyrics: bool = False) -> Analysis:
    path = path.resolve()
    digest = fingerprint(path)
    metadata = read_metadata(path)
    root = settings.cache_dir.resolve()
    report_key = _key({'source':str(path),'digest':digest,'backend':settings.asr,'checkpoint':settings.checkpoint,
                       'separator':settings.separator_model,'seconds':settings.chunk_seconds,'overlap':settings.overlap_seconds})
    analysis = Analysis(path,digest,metadata,[], '', [],report_path=root/'reports'/(report_key+'.json'))
    save_report(analysis,settings,'processing')
    try:
        _analyze(analysis, settings, progress, runtime, force_lyrics, ensure_timed_lyrics)
    except Exception as exc:
        analysis.notes.append(str(exc))
        try:
            save_report(analysis,settings,'failed')
        except OSError:
            pass
        raise
    save_report(analysis,settings)
    return analysis


def vocal_duration(path):
    import soundfile as sf
    return float(sf.info(path).duration)


def valid_chunks(chunks, duration):
    if not chunks:
        return duration == 0
    if abs(chunks[0].start) > .05 or abs(chunks[-1].end-duration) > .05:
        return False
    previous = None
    for chunk in chunks:
        if not isinstance(chunk.text,str) or not all(math.isfinite(v) for v in (chunk.start,chunk.end)) or not 0 <= chunk.start < chunk.end <= duration + .05:
            return False
        last_timed_start = -1.0
        for word in chunk.aligned_words:
            if (not word.text.strip() or not all(math.isfinite(v) for v in (word.start, word.end))
                    or not chunk.start - .05 <= word.start <= word.end <= chunk.end + .05
                    or word.start < last_timed_start):
                return False
            last_timed_start = word.start
        if previous and not (previous.start < chunk.start <= previous.end + .05 and previous.end < chunk.end):
            return False
        previous = chunk
    return True


def _runtime(path):
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
        return value if isinstance(value,dict) else {}
    except (OSError,ValueError):
        return {}


def _analyze(analysis: Analysis, settings: Settings, progress, runtime: ModelRuntime | None = None,
             force_lyrics: bool = False, ensure_timed_lyrics: bool = False):
    path, digest, metadata = analysis.source, analysis.digest, analysis.metadata
    metadata_title = str(metadata.get('title') or '').strip()
    title = metadata_title if has_meaningful_title(metadata_title) else ''
    if metadata_title and not title:
        analysis.notes.append(f'Metadata title {metadata_title!r} looks like a generic track number; title suggestions will be generated.')
    embedded_lyrics = str(metadata.get('lyrics') or '').strip()
    timed_path = _timed_cache_path(settings.cache_dir.resolve(), path, settings)
    analysis.timed_cache_path = str(timed_path)
    timed_cache = _load_timed_cache(timed_path, digest)
    if ensure_timed_lyrics and timed_cache is not None and not force_lyrics:
        cache_info, analysis.timed_lyrics, analysis.timed_lyrics_formats = timed_cache
        analysis.lyrics = embedded_lyrics or str(cache_info.get('lyrics') or '')
        analysis.formatted_lyrics = embedded_lyrics or str(cache_info.get('formatted_lyrics') or analysis.lyrics)
        if title:
            analysis.notes.append('Title retained from metadata; matching timed-lyrics cache reused without transcription.')
        else:
            progress(f'Generating title suggestions with {settings.ollama_model}…')
            suggestions = suggest_titles(analysis.lyrics, metadata, settings, runtime)
            analysis.candidates = list(suggestions)
        return
    if title and embedded_lyrics and not force_lyrics:
        analysis.lyrics = embedded_lyrics
        analysis.formatted_lyrics = embedded_lyrics
        if timed_cache is not None:
            _, analysis.timed_lyrics, analysis.timed_lyrics_formats = timed_cache
        analysis.notes.append('Title retained from metadata; embedded lyrics already exist, so model inference was skipped.')
        if metadata.get('metadata_warning'):
            analysis.notes.append('Metadata read warning: ' + metadata['metadata_warning'])
        progress('Title and embedded lyrics already exist; skipping model inference.')
        return
    if title:
        analysis.notes.append('Title retained from metadata; title suggestions are skipped.')
        if embedded_lyrics and force_lyrics:
            analysis.notes.append('Lyrics will be regenerated because --force-lyrics was supplied.')
    root = settings.cache_dir.resolve()
    stem_key = _key({'schema':CACHE_SCHEMA,'source':digest,'separator':settings.separator_model,'demucs':_version('demucs'),'decode':'stereo-44100-f32'})
    folder = root/'vocals'/stem_key
    folder.mkdir(parents=True,exist_ok=True)
    vocals = folder/'vocals.wav'
    manifest = folder/'manifest.json'
    cached = None if settings.refresh else _read_cache(manifest)
    if not cached or not vocals.is_file() or cached.get('digest') != fingerprint(vocals):
        progress('Separating vocals with Demucs…')
        manifest.unlink(missing_ok=True)
        if runtime is None:
            separate_vocals(path,vocals,settings)
        else:
            runtime.separate(path, vocals)
        if fingerprint(path) != digest:
            raise ValueError('Source changed during separation')
        stem_digest = fingerprint(vocals)
        cached = {'schema':CACHE_SCHEMA,'digest':stem_digest,'runtime':_runtime(vocals.with_suffix('.wav.runtime.json'))}
        atomic_json(manifest,cached)
    else:
        progress('Using cached vocal stem.')
        stem_digest = cached['digest']
    analysis.runtime['separation'] = cached.get('runtime',{})
    duration = vocal_duration(vocals)
    strategy = 'nemotron-stream-v1' if settings.asr == 'nemotron' else 'qwen-overlap-align-v1'
    transcript_key = _key({'schema':CACHE_SCHEMA,'stem':stem_digest,'backend':settings.asr,'checkpoint':settings.checkpoint,
                           'strategy':strategy,
                           'transformers':_version('transformers'),'torch':_version('torch'),'seconds':settings.chunk_seconds,'overlap':settings.overlap_seconds,'tokens':512})
    transcript_path = root/'transcripts'/(transcript_key+'.json')
    cached = None if settings.refresh else _read_cache(transcript_path)
    chunks = None
    if cached:
        try:
            chunks = [Chunk(**item) for item in cached['chunks']]
            if not valid_chunks(chunks,duration):
                chunks = None
        except (TypeError,ValueError,KeyError):
            chunks = None
    if chunks is None:
        progress(f'Transcribing isolated vocals with {settings.asr}…')
        chunks = transcribe_vocals(vocals, settings) if runtime is None else runtime.transcribe(vocals)
        if not valid_chunks(chunks,duration):
            raise ValueError('ASR chunks do not cover the complete vocal stem')
        cached = {'schema':CACHE_SCHEMA,'chunks':[asdict(c) for c in chunks],
                  'runtime':_runtime(vocals.parent/'transcript-worker.json.runtime.json')}
        atomic_json(transcript_path,cached)
    else:
        progress('Using cached transcript.')
        if settings.asr == 'qwen' and any(chunk.text.strip() and not chunk.aligned_words for chunk in chunks):
            progress('Aligning cached Qwen transcript words…')
            chunks = (transcribe_vocals(vocals, settings, chunks) if runtime is None
                      else runtime.transcribe(vocals, chunks))
            if not valid_chunks(chunks, duration):
                raise ValueError('Aligned cached transcript chunks are invalid')
            cached = {'schema': CACHE_SCHEMA, 'chunks': [asdict(c) for c in chunks],
                      'runtime': cached.get('runtime', {})}
            atomic_json(transcript_path, cached)
    analysis.runtime['transcription'] = cached.get('runtime',{})
    lyrics = assemble_transcript(chunks)
    notes = analysis.notes
    alignment_errors = [chunk.alignment_error for chunk in chunks if chunk.alignment_error]
    if alignment_errors:
        notes.append('Word alignment failed for this track; conservative overlap text stitching was used: ' + alignment_errors[0])
    elif settings.asr == 'qwen' and settings.overlap_seconds:
        notes.append('Matching phrases are removed across overlapping chunks; ambiguous whole-chunk matches and non-overlapping repeats are preserved.')
    if not lyrics.strip():
        notes.append('No lyrics transcribed. Review the vocal stem or enter a manual title.')
    if prepare_lyrics(lyrics)[1]:
        notes.append('Lyrics were reduced to fit the title model context.')
    if metadata.get('metadata_warning'):
        notes.append('Metadata read warning: ' + metadata['metadata_warning'])
    analysis.chunks, analysis.lyrics = chunks, lyrics
    save_report(analysis,settings)
    if title:
        if lyrics.strip():
            progress(f'Formatting lyrics with {settings.ollama_model}…')
            try:
                analysis.formatted_lyrics = format_lyrics_with_model(lyrics, metadata, settings, runtime)
            except RuntimeError as exc:
                analysis.notes.append(f'Lyric formatting failed; conservative local formatting will be used: {exc}')
    else:
        progress(f'Generating title suggestions with {settings.ollama_model}…')
        suggestions = suggest_titles(lyrics, metadata, settings, runtime)
        analysis.candidates = list(suggestions)
        analysis.formatted_lyrics = getattr(suggestions, 'formatted_lyrics', '')
    formatted_timed_lyrics = analysis.formatted_lyrics.strip() or lyrics
    timed_words = [word for chunk in chunks for word in chunk.aligned_words]
    if timed_words:
        try:
            formats = {'lrc': format_lrc(formatted_timed_lyrics, timed_words),
                       'srt': format_srt(formatted_timed_lyrics, timed_words),
                       'vtt': format_vtt(formatted_timed_lyrics, timed_words)}
            analysis.timed_lyrics = timed_words
            analysis.timed_lyrics_formats = formats
            _save_timed_cache(timed_path, digest, lyrics, formatted_timed_lyrics, timed_words, formats)
        except ValueError as exc:
            notes.append(f'Timed lyric formatting failed; untimed lyrics are retained: {exc}')
    elif ensure_timed_lyrics:
        notes.append('Timed lyrics were requested, but this ASR result did not contain usable word timestamps.')

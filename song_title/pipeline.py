from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import tempfile
from dataclasses import asdict
from pathlib import Path

from .asr import separate_vocals, transcribe_vocals
from .audio import assemble_transcript
from .metadata import fingerprint, read_metadata
from .titles import prepare_lyrics, suggest_titles
from .types import Analysis, Chunk, Settings

CACHE_SCHEMA = 1


def _version(name):
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return 'uninstalled'


def _key(data):
    return hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()


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
    text = f'{analysis.source}\n\n{analysis.lyrics}\n\nTitle suggestions:\n'
    for number,candidate in enumerate(analysis.candidates,1):
        text += f'{number}. {candidate.title}\n   {candidate.rationale}\n'
    text += f'\nSelected title: {analysis.selected_title or "(none)"}\nOutcome: {outcome}\n'
    if backup:
        text += f'Backup: {backup}\n'
    if analysis.notes:
        text += '\nNotes:\n' + '\n'.join(analysis.notes) + '\n'
    path.with_suffix('.txt').write_text(text,encoding='utf-8')


def analyze_file(path: Path, settings: Settings, progress=print) -> Analysis:
    path = path.resolve()
    digest = fingerprint(path)
    metadata = read_metadata(path)
    root = settings.cache_dir.resolve()
    report_key = _key({'source':str(path),'digest':digest,'backend':settings.asr,'checkpoint':settings.checkpoint,
                       'separator':settings.separator_model,'seconds':settings.chunk_seconds,'overlap':settings.overlap_seconds})
    analysis = Analysis(path,digest,metadata,[], '', [],report_path=root/'reports'/(report_key+'.json'))
    save_report(analysis,settings,'processing')
    try:
        _analyze(analysis,settings,progress)
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


def _analyze(analysis: Analysis, settings: Settings, progress):
    path, digest, metadata = analysis.source, analysis.digest, analysis.metadata
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
        separate_vocals(path,vocals,settings)
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
    transcript_key = _key({'schema':CACHE_SCHEMA,'stem':stem_digest,'backend':settings.asr,'checkpoint':settings.checkpoint,
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
        chunks = transcribe_vocals(vocals,settings)
        if not valid_chunks(chunks,duration):
            raise ValueError('ASR chunks do not cover the complete vocal stem')
        cached = {'schema':CACHE_SCHEMA,'chunks':[asdict(c) for c in chunks],
                  'runtime':_runtime(vocals.parent/'transcript-worker.json.runtime.json')}
        atomic_json(transcript_path,cached)
    else:
        progress('Using cached transcript.')
    analysis.runtime['transcription'] = cached.get('runtime',{})
    lyrics = assemble_transcript(chunks)
    notes = analysis.notes
    if settings.overlap_seconds:
        notes.append('Overlapping chunk text may repeat; phrases are preserved because exact word timings are unavailable.')
    if not lyrics.strip():
        notes.append('No lyrics transcribed. Review the vocal stem or enter a manual title.')
    if prepare_lyrics(lyrics)[1]:
        notes.append('Lyrics were reduced to fit the title model context.')
    if metadata.get('metadata_warning'):
        notes.append('Metadata read warning: ' + metadata['metadata_warning'])
    analysis.chunks, analysis.lyrics = chunks, lyrics
    save_report(analysis,settings)
    progress(f'Generating title suggestions with {settings.ollama_model}…')
    analysis.candidates = suggest_titles(lyrics,metadata,settings)

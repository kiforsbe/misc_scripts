import json
from pathlib import Path

import pytest


def test_vocals_only_pipeline_and_backend_cache(tmp_path, monkeypatch):
    from song_title import pipeline
    from song_title.types import Settings, Chunk, Candidate
    source = tmp_path / 'song.mp3'
    source.write_bytes(b'original mix')
    stem = tmp_path / 'vocals.wav'
    events = []
    def separate(source_arg, output, settings):
        events.append('separate')
        output.write_bytes(b'vocals only')
        return output
    def transcribe(input_path, settings):
        assert input_path != source and input_path.read_bytes() == b'vocals only'
        events.append('asr:' + settings.asr)
        return [Chunk(0,30,'after the last train')]
    monkeypatch.setattr(pipeline, 'separate_vocals', separate)
    monkeypatch.setattr(pipeline, 'transcribe_vocals', transcribe)
    monkeypatch.setattr(pipeline, 'read_metadata', lambda p: {'title':'Old'})
    monkeypatch.setattr(pipeline, 'vocal_duration', lambda p:30, raising=False)
    monkeypatch.setattr(pipeline, 'suggest_titles', lambda *args: [Candidate('Last Train','Chorus',['last train'])])
    settings = Settings(asr='qwen',cache_dir=tmp_path / 'cache')
    first = pipeline.analyze_file(source, settings)
    second = pipeline.analyze_file(source, settings)
    third = pipeline.analyze_file(source, Settings(asr='nemotron',cache_dir=settings.cache_dir))
    assert events == ['separate','asr:qwen','asr:nemotron']
    assert first.lyrics == second.lyrics == third.lyrics == 'after the last train'
    assert json.loads(first.report_path.read_text())['candidates'][0]['title'] == 'Last Train'


def test_separation_failure_prevents_asr(tmp_path, monkeypatch):
    from song_title import pipeline
    from song_title.types import Settings
    source = tmp_path / 'song.mp3'
    source.write_bytes(b'mix')
    monkeypatch.setattr(pipeline, 'read_metadata', lambda p: {})
    monkeypatch.setattr(pipeline, 'separate_vocals', lambda *args: (_ for _ in ()).throw(RuntimeError('no stem')))
    monkeypatch.setattr(pipeline, 'transcribe_vocals', lambda *args: pytest.fail('ASR called without vocals'))
    with pytest.raises(RuntimeError, match='no stem'):
        pipeline.analyze_file(source, Settings(cache_dir=tmp_path/'cache'))


def test_cached_stem_corruption_is_regenerated(tmp_path, monkeypatch):
    from song_title import pipeline
    from song_title.types import Settings, Chunk
    source = tmp_path/'song.wav'
    source.write_bytes(b'mix')
    calls = []
    def separate(source, output, settings):
        calls.append(output)
        output.write_bytes(b'clean stem')
        return output
    monkeypatch.setattr(pipeline,'separate_vocals',separate)
    monkeypatch.setattr(pipeline,'transcribe_vocals',lambda *a:[Chunk(0,1,'train')])
    monkeypatch.setattr(pipeline,'suggest_titles',lambda *a:[])
    monkeypatch.setattr(pipeline,'read_metadata',lambda *a:{})
    monkeypatch.setattr(pipeline,'vocal_duration',lambda p:1, raising=False)
    settings = Settings(cache_dir=tmp_path/'cache')
    pipeline.analyze_file(source, settings)
    calls[0].write_bytes(b'corrupted')
    pipeline.analyze_file(source, settings)
    assert len(calls) == 2


@pytest.mark.parametrize('bad_chunks',[[],[{'start':0,'end':1,'text':'partial'}]])
def test_incomplete_transcript_cache_is_regenerated(tmp_path, monkeypatch, bad_chunks):
    from song_title import pipeline
    from song_title.types import Settings, Chunk
    source = tmp_path/'song.wav'
    source.write_bytes(b'mix')
    calls = []
    def separate(source,output,settings):
        output.write_bytes(b'vocal stem')
    def transcribe(*a):
        calls.append(1)
        return [Chunk(0,30,'complete lyrics')]
    monkeypatch.setattr(pipeline,'separate_vocals',separate)
    monkeypatch.setattr(pipeline,'transcribe_vocals',transcribe)
    monkeypatch.setattr(pipeline,'vocal_duration',lambda p:30, raising=False)
    monkeypatch.setattr(pipeline,'read_metadata',lambda p:{})
    monkeypatch.setattr(pipeline,'suggest_titles',lambda *a:[])
    settings = Settings(cache_dir=tmp_path/'cache')
    pipeline.analyze_file(source,settings)
    cached = next((settings.cache_dir/'transcripts').glob('*.json'))
    cached.write_text(json.dumps({'schema':pipeline.CACHE_SCHEMA,'chunks':bad_chunks}))
    assert pipeline.analyze_file(source,settings).lyrics == 'complete lyrics'
    assert len(calls) == 2


def test_asr_failure_writes_error_report(tmp_path, monkeypatch):
    from song_title import pipeline
    from song_title.types import Settings
    source = tmp_path/'song.wav'
    source.write_bytes(b'mix')
    monkeypatch.setattr(pipeline,'read_metadata',lambda p:{})
    monkeypatch.setattr(pipeline,'vocal_duration',lambda p:30, raising=False)
    monkeypatch.setattr(pipeline,'separate_vocals',lambda source,output,settings:output.write_bytes(b'vocals'))
    monkeypatch.setattr(pipeline,'transcribe_vocals',lambda *a: (_ for _ in ()).throw(RuntimeError('model failure')))
    settings = Settings(cache_dir=tmp_path/'cache')
    with pytest.raises(RuntimeError,match='model failure'):
        pipeline.analyze_file(source,settings)
    reports = list((settings.cache_dir/'reports').glob('*.json'))
    assert reports and 'model failure' in reports[0].read_text()

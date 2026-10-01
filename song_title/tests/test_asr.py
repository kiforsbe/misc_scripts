import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest


class InputDict(dict):
    def to(self, *args, **kwargs):
        return self


def test_qwen_adapter_forces_english_and_removes_prompt_tokens():
    from song_title.asr import decode_qwen
    class Processor:
        def apply_transcription_request(self, **kwargs):
            assert kwargs['language'] == 'English'
            assert isinstance(kwargs['audio'], np.ndarray)
            assert kwargs['audio_kwargs']['sampling_rate'] == 16000
            return InputDict(input_ids=np.array([[1,2]]))
        def decode(self, ids, **kwargs):
            assert ids.tolist() == [[3,4]]
            return [{'language':'English','transcription':'last train'}]
    class Model:
        device = 'cpu'
        dtype = None
        def generate(self, **kwargs):
            return np.array([[1,2,3,4]])
    text, truncated = decode_qwen(Model(), Processor(), np.zeros(100), 16000, 512)
    assert text == 'last train' and not truncated


def test_nemotron_adapter_decodes_returned_sequences():
    from song_title.asr import decode_nemotron
    class Processor:
        def __call__(self, audio, **kwargs):
            assert kwargs['sampling_rate'] == 16000
            return InputDict(input_features=np.array([[0.0]]))
        def decode(self, ids, **kwargs):
            assert ids.tolist() == [[7,8]] and kwargs['skip_special_tokens']
            return ['last train']
    class Model:
        device = 'cpu'
        dtype = None
        def generate(self, **kwargs):
            return SimpleNamespace(sequences=np.array([[7,8]]))
    assert decode_nemotron(Model(), Processor(), np.zeros(100), 16000) == 'last train'


def test_worker_failure_contains_reason_and_never_returns_output(tmp_path, monkeypatch):
    from song_title.asr import run_worker
    from song_title.types import Settings
    def run(command, **kwargs):
        assert command[:3][1:] == ['-m','song_title.worker']
        assert isinstance(command, list)
        return SimpleNamespace(returncode=1, stdout='', stderr='separation failed')
    monkeypatch.setattr('subprocess.run', run)
    with pytest.raises(RuntimeError, match='separation failed'):
        run_worker('separate', tmp_path / 'input [1].mp3', tmp_path / 'vocals.wav', Settings())


def test_transcription_worker_uses_vocal_input_and_offsets(tmp_path, monkeypatch):
    from song_title import asr
    from song_title.types import Settings
    import soundfile as sf
    path = tmp_path / 'vocals.wav'
    sf.write(path, np.ones(6500) * .1, 100)
    class Model:
        device = 'cpu'
        dtype = None
    processor = SimpleNamespace(feature_extractor=SimpleNamespace(sampling_rate=100))
    monkeypatch.setattr(asr, 'load_backend', lambda settings, device: (Model(), processor))
    monkeypatch.setattr(asr, 'decode_nemotron', lambda *args: 'last train')
    result = asr.transcribe_in_process(path, Settings(asr='nemotron',device='cpu'))
    assert [(c.start,c.end,c.text) for c in result] == [(0,30,'last train'),(26,56,'last train'),(52,65,'last train')]


@pytest.mark.parametrize('seconds,overlap,reduced,expected',[(30,20,False,[(0,30),(10,40)]),(2,0.5,True,[(0,1),(0.5,1.5),(1,2)])])
def test_requested_overlap_and_reduced_chunk_size(tmp_path,monkeypatch,seconds,overlap,reduced,expected):
    from song_title import asr
    from song_title.types import Settings
    import soundfile as sf
    duration = 40 if not reduced else 2
    path = tmp_path/'vocals.wav'
    sf.write(path,np.ones(duration*100)*.1,100)
    model = SimpleNamespace(device='cpu',dtype=None)
    processor = SimpleNamespace(feature_extractor=SimpleNamespace(sampling_rate=100))
    monkeypatch.setattr(asr,'load_backend',lambda *a:(model,processor))
    monkeypatch.setattr(asr,'decode_nemotron',lambda *a:'lyrics')
    chunks = asr.transcribe_in_process(path,Settings(asr='nemotron',device='cpu',chunk_seconds=seconds,overlap_seconds=overlap),reduced=reduced)
    assert [(c.start,c.end) for c in chunks] == expected


def test_auto_oom_retry_reduces_segments_then_uses_cpu(monkeypatch):
    from song_title.asr import with_device_retry
    attempts = []
    def operation(device, reduced):
        attempts.append((device, reduced))
        if device == 'cuda':
            raise RuntimeError('CUDA out of memory')
        return 'done'
    assert with_device_retry(operation, 'auto', cuda_available=True) == 'done'
    assert attempts == [('cuda',False),('cuda',True),('cpu',True)]


def test_explicit_cuda_failure_does_not_silently_use_cpu():
    from song_title.asr import with_device_retry
    with pytest.raises(RuntimeError, match='memory'):
        with_device_retry(lambda *a: (_ for _ in ()).throw(RuntimeError('CUDA out of memory')), 'cuda', True)

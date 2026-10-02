"""Long-lived, one-phase model worker used by a single song-title run."""
from __future__ import annotations

import gc
import json
import subprocess
import sys
import tempfile
from contextlib import redirect_stdout
from dataclasses import asdict
from pathlib import Path

from .types import Settings


def _settings(data):
    values = dict(data)
    values['cache_dir'] = Path(values['cache_dir'])
    return Settings(**values)


def _release_cuda(torch):
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()


def _serve(phase: str):
    import torch
    from .asr import load_backend, transcribe_in_process, with_device_retry

    backend = None
    backend_device = None
    separator = None
    separator_device = None
    preferred_configuration = None

    def release_backend():
        nonlocal backend, backend_device
        backend = None
        backend_device = None
        _release_cuda(torch)

    def release_separator():
        nonlocal separator, separator_device
        separator = None
        separator_device = None
        _release_cuda(torch)

    def run(request):
        nonlocal backend, backend_device, separator, separator_device, preferred_configuration
        settings = _settings(request['settings'])
        if phase == 'separate':
            requested = settings.separator_device or settings.device
        elif phase == 'asr':
            requested = settings.device
        else:
            raise ValueError(f'Unknown worker phase: {phase}')
        used = {}

        def operation(device, reduced):
            nonlocal backend, backend_device, separator, separator_device
            used.update(device=device, reduced_segments=reduced)
            if device == 'cuda':
                torch.cuda.reset_peak_memory_stats()
            try:
                if phase == 'asr':
                    if backend is None or backend_device != device:
                        release_backend()
                        backend = load_backend(settings, device)
                        backend_device = device
                    chunks = transcribe_in_process(
                        Path(request['source']), settings, device, reduced, backend=backend)
                    result = {'chunks': [asdict(chunk) for chunk in chunks]}
                else:
                    if separator is None or separator_device != device:
                        release_separator()
                        from demucs.api import Separator
                        separator = Separator(
                            model=settings.separator_model, device=device, shifts=1,
                            split=True, overlap=0.25, segment=3 if reduced else 6,
                            jobs=0, progress=True,
                        )
                        # Demucs loads checkpoints on CPU, then apply_model moves each
                        # submodel back to its original device after separation. Put the
                        # model on the selected device once so it stays resident there.
                        separator._model.to(device)
                        separator_device = device
                        print(f'Demucs model {settings.separator_model} loaded on {device} and kept resident.',
                              flush=True)
                    else:
                        separator.update_parameter(device=device, segment=3 if reduced else 6)
                    result = self_separate(Path(request['source']), Path(request['output']), separator,
                                           settings.worker_timeout)
                return result
            except RuntimeError as exc:
                if device == 'cuda' and 'out of memory' in str(exc).casefold():
                    if phase == 'asr':
                        release_backend()
                    else:
                        release_separator()
                raise

        if requested == 'auto' and preferred_configuration is not None:
            try:
                result = operation(*preferred_configuration)
            except RuntimeError as exc:
                if 'out of memory' not in str(exc).casefold():
                    raise
                result = with_device_retry(operation, requested, torch.cuda.is_available())
        else:
            result = with_device_retry(operation, requested, torch.cuda.is_available())
        if requested == 'auto':
            preferred_configuration = (used['device'], used['reduced_segments'])
        result['runtime'] = dict(used)
        if used.get('device') == 'cuda':
            result['runtime']['peak_vram_bytes'] = torch.cuda.max_memory_allocated()
        return result

    try:
        for line in sys.stdin:
            try:
                request = json.loads(line)
                if not isinstance(request, dict):
                    raise ValueError('request must be a JSON object')
                with redirect_stdout(sys.stderr):
                    result = run(request)
                response = {'ok': True, 'result': result}
            except Exception as exc:
                response = {'ok': False, 'error': f'{type(exc).__name__}: {exc}'}
            sys.stdout.write(json.dumps(response, ensure_ascii=False, default=str) + '\n')
            sys.stdout.flush()
    finally:
        release_backend()
        release_separator()


def self_separate(source: Path, output: Path, separator, worker_timeout: float):
    """Decode consistently with the previous CLI path and save its vocal stem."""
    with tempfile.TemporaryDirectory(prefix='song-title-separate-') as folder:
        decoded = Path(folder) / 'decoded.wav'
        subprocess.run([
            'ffmpeg', '-nostdin', '-v', 'error', '-y', '-i', str(source),
            '-ac', '2', '-ar', '44100', '-c:a', 'pcm_f32le', str(decoded),
        ], check=True, capture_output=True, timeout=max(1, worker_timeout - 15))
        _, stems = separator.separate_audio_file(decoded)
        vocals = stems.get('vocals')
        if vocals is None:
            raise RuntimeError('Demucs did not output a vocals stem')
        output.parent.mkdir(parents=True, exist_ok=True)
        from demucs.separate import save_audio
        save_audio(vocals, output, separator.samplerate, bits_per_sample=32, as_float=True)
    return {}


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        print('usage: python -m song_title.worker separate|asr', file=sys.stderr)
        return 2
    _serve(argv[0])
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(f'{type(exc).__name__}: {exc}', file=sys.stderr)
        raise SystemExit(1)

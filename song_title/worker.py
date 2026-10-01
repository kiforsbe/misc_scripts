"""Short-lived inference workers release models before the next phase starts."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path

from .types import Settings


def _separate(source: Path, output: Path, settings: Settings, device: str, reduced: bool):
    with tempfile.TemporaryDirectory(prefix='song-title-separate-') as folder:
        decoded = Path(folder)/'decoded.wav'
        subprocess.run(['ffmpeg','-nostdin','-v','error','-y','-i',str(source),'-ac','2','-ar','44100','-c:a','pcm_f32le',str(decoded)], check=True)
        # Demucs is in this short-lived worker rather than a persistent model
        # process; failure cannot leave another inference subprocess running.
        from demucs.separate import main as demucs_main
        demucs_main(['--two-stems','vocals','--float32','-n',settings.separator_model,'-d',device,'--segment','3' if reduced else '6','--out',str(Path(folder)/'separated'),str(decoded)])
        stem = Path(folder)/'separated'/settings.separator_model/'decoded'/'vocals.wav'
        if not stem.is_file():
            raise RuntimeError('Demucs did not output vocals.wav')
        output.parent.mkdir(parents=True,exist_ok=True)
        import shutil
        shutil.copyfile(stem, output)


def main(argv=None):
    from .asr import transcribe_in_process, with_device_retry
    import torch
    argv = sys.argv[1:] if argv is None else argv
    phase, request_path, result_path = argv
    request = json.loads(Path(request_path).read_text(encoding='utf-8'))
    settings_dict = request['settings']
    settings_dict['cache_dir'] = Path(settings_dict['cache_dir'])
    settings = Settings(**settings_dict)
    source, output = Path(request['source']), Path(request['output'])
    requested = (settings.separator_device or settings.device) if phase == 'separate' else settings.device
    used = {}
    def operation(device,reduced):
        used.update(device=device, reduced_segments=reduced)
        if device == 'cuda':
            torch.cuda.reset_peak_memory_stats()
        if phase == 'separate':
            _separate(source,output,settings,device,reduced)
            return {}
        if phase == 'asr':
            return {'chunks':[asdict(c) for c in transcribe_in_process(source,settings,device,reduced)]}
        raise ValueError('Unknown worker phase')
    result = with_device_retry(operation,requested,torch.cuda.is_available())
    result['runtime'] = used
    if used.get('device') == 'cuda':
        result['runtime']['peak_vram_bytes'] = torch.cuda.max_memory_allocated()
    Path(result_path).write_text(json.dumps(result),encoding='utf-8')
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f'{type(exc).__name__}: {exc}',file=sys.stderr)
        raise SystemExit(1)

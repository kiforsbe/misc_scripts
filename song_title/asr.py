from __future__ import annotations

import gc
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path

from common.presentation import Colors

from .audio import chunk_ranges
from .types import Chunk, Settings


def decode_qwen(model, processor, samples, sample_rate, max_tokens):
    inputs = processor.apply_transcription_request(audio=samples, language='English', audio_kwargs={'sampling_rate':sample_rate})
    inputs = inputs.to(model.device, model.dtype)
    ids = model.generate(**inputs, max_new_tokens=max_tokens, do_sample=False)
    generated = ids[:, inputs['input_ids'].shape[1]:]
    parsed = processor.decode(generated, return_format='parsed')
    if isinstance(parsed, list):
        parsed = parsed[0]
    return parsed['transcription'].strip(), generated.shape[-1] >= max_tokens


def decode_nemotron(model, processor, samples, sample_rate):
    inputs = processor(samples, sampling_rate=sample_rate, return_tensors='pt')
    inputs = inputs.to(model.device, dtype=model.dtype)
    result = model.generate(**inputs, return_dict_in_generate=True)
    text = processor.decode(result.sequences, skip_special_tokens=True)
    return (text[0] if isinstance(text, list) else text).strip()


def load_backend(settings: Settings, device: str):
    import torch
    try:
        from transformers import AutoProcessor, AutoModelForMultimodalLM, AutoModelForRNNT
    except ImportError as exc:
        raise RuntimeError('Install song_title/requirements.txt; native ASR requires transformers>=5.13') from exc
    processor = AutoProcessor.from_pretrained(settings.checkpoint)
    factory = AutoModelForMultimodalLM if settings.asr == 'qwen' else AutoModelForRNNT
    dtype = torch.float32 if device == 'cpu' else (torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float16)
    model = factory.from_pretrained(settings.checkpoint, dtype=dtype, attn_implementation='sdpa').to(device).eval()
    return model, processor


def with_device_retry(operation, requested: str, cuda_available: bool):
    if requested == 'cuda' and not cuda_available:
        raise RuntimeError('CUDA requested but unavailable; use --device cpu')
    device = 'cuda' if requested != 'cpu' and cuda_available else 'cpu'
    for current, reduced in [(device,False)] + ([('cuda',True)] + ([('cpu',True)] if requested == 'auto' else []) if device == 'cuda' else []):
        try:
            return operation(current, reduced)
        except RuntimeError as exc:
            if 'out of memory' not in str(exc).lower() or current == 'cpu':
                raise
            print(f'{current} memory exhausted; retrying with smaller segments or CPU.', flush=True)
    raise RuntimeError('CUDA out of memory after reduced-segment retry; use --device cpu or a smaller Qwen checkpoint')


def transcribe_in_process(vocals: Path, settings: Settings, device: str | None = None, reduced: bool = False) -> list[Chunk]:
    import numpy as np
    import soundfile as sf
    import torch
    model = processor = None
    try:
        model, processor = load_backend(settings, device or settings.device)
        sample_rate = int(processor.feature_extractor.sampling_rate)
        with tempfile.TemporaryDirectory(prefix='song-title-asr-') as folder:
            normalized = Path(folder)/'vocals-mono.wav'
            subprocess.run(['ffmpeg','-nostdin','-v','error','-y','-i',str(vocals),'-ac','1','-ar',str(sample_rate),'-c:a','pcm_f32le',str(normalized)], check=True, capture_output=True)
            samples, rate = sf.read(normalized, dtype='float32')
        duration = len(samples)/rate
        seconds = settings.chunk_seconds / 2 if reduced else settings.chunk_seconds
        overlap = min(settings.overlap_seconds, seconds/2) if reduced else settings.overlap_seconds
        ranges = chunk_ranges(duration, seconds, overlap)
        chunks = []
        with torch.inference_mode():
            for number, (start,end) in enumerate(ranges, 1):
                data = samples[round(start*rate):round(end*rate)]
                print(f'{settings.asr}: chunk {number}/{len(ranges)} ({start:.1f}–{end:.1f}s)', flush=True)
                if not data.size or np.max(np.abs(data)) < 1e-7:
                    text = ''
                elif settings.asr == 'qwen':
                    text, truncated = decode_qwen(model, processor, data, rate, 512)
                    if truncated:
                        text, truncated = decode_qwen(model, processor, data, rate, 1024)
                    if truncated:
                        raise RuntimeError(f'Qwen output truncated at {start:.1f}s; reduce --chunk-seconds')
                else:
                    text = decode_nemotron(model, processor, data, rate)
                chunks.append(Chunk(start,end,text))
        return chunks
    finally:
        del model, processor
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()


def run_worker(phase: str, source: Path, output: Path, settings: Settings):
    with tempfile.TemporaryDirectory(prefix='song-title-worker-') as folder:
        request = Path(folder)/'request.json'
        response = Path(folder)/'response.json'
        request.write_text(json.dumps({'source':str(source.resolve()),'output':str(output.resolve()),'settings':asdict(settings)}, default=str), encoding='utf-8')
        environment = os.environ.copy()
        environment['PYTHONIOENCODING'] = 'utf-8'
        # Resolve the package even when invoked outside the repository root.
        environment['PYTHONPATH'] = str(Path(__file__).resolve().parent.parent) + os.pathsep + environment.get('PYTHONPATH','')
        try:
            result = subprocess.run([sys.executable,'-m','song_title.worker',phase,str(request),str(response)], capture_output=True, text=True, encoding='utf-8', errors='replace', env=environment, timeout=settings.worker_timeout)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f'{phase} exceeded {settings.worker_timeout:g}s; increase --worker-timeout') from exc
        if result.returncode or not response.exists():
            detail = result.stderr[-4000:] or result.stdout[-4000:] or 'worker produced no response'
            raise RuntimeError(f'{phase} failed: {detail.strip()}')
        if result.stdout.strip():
            use_color = Colors.should_use()
            for line in result.stdout.splitlines():
                lowered = line.casefold()
                color = (Colors.YELLOW if 'memory exhausted' in lowered else
                         Colors.CYAN if 'chunk ' in lowered else Colors.DIM)
                print(Colors.wrap(line, color, use_color), flush=True)
        data = json.loads(response.read_text(encoding='utf-8'))
        runtime = data.get('runtime', {})
        print(f'{phase}: device={runtime.get("device", "unknown")}, reduced segments={runtime.get("reduced_segments", False)}', flush=True)
        if 'peak_vram_bytes' in runtime:
            print(f'{phase}: peak PyTorch GPU allocation {runtime["peak_vram_bytes"] / 2**20:.0f} MiB', flush=True)
        output.with_suffix(output.suffix + '.runtime.json').write_text(json.dumps(runtime), encoding='utf-8')
        return data


def separate_vocals(source: Path, output: Path, settings: Settings) -> Path:
    run_worker('separate', source, output, settings)
    if not output.is_file():
        raise RuntimeError('Separator did not produce a vocal stem')
    return output


def transcribe_vocals(vocals: Path, settings: Settings) -> list[Chunk]:
    result = run_worker('asr', vocals, vocals.parent/'transcript-worker.json', settings)
    return [Chunk(**item) for item in result['chunks']]

from __future__ import annotations

import gc
import json
import math
import subprocess
import tempfile
from dataclasses import asdict
from pathlib import Path

from .audio import chunk_ranges
from .types import Chunk, Settings, TimedWord


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


def _nemotron_stream_features(samples, processor, sample_rate, first_features=None):
    import numpy as np

    first_size = processor.num_samples_first_audio_chunk
    if first_features is None:
        first_audio = samples[:first_size]
        if len(first_audio) < first_size:
            first_audio = np.pad(first_audio, (0, first_size - len(first_audio)))
        first = processor(first_audio, sampling_rate=sample_rate, is_streaming=True,
                          is_first_audio_chunk=True, return_tensors='pt')
        first_features = first.input_features
    yield first_features[:, :processor.num_mel_frames_first_audio_chunk, :]

    mel_frame = processor.num_mel_frames_first_audio_chunk
    hop = processor.feature_extractor.hop_length
    n_fft = processor.feature_extractor.n_fft
    chunk_size = processor.num_samples_per_audio_chunk
    start = mel_frame * hop - n_fft // 2
    while start < len(samples):
        end = start + chunk_size
        audio = samples[max(0, start):min(end, len(samples))]
        if len(audio) < chunk_size:
            audio = np.pad(audio, (0, chunk_size - len(audio)))
        chunk = processor(audio, sampling_rate=sample_rate, is_streaming=True,
                          is_first_audio_chunk=False, return_tensors='pt')
        yield chunk.input_features[:, :processor.num_mel_frames_per_audio_chunk, :]
        mel_frame += processor.num_mel_frames_per_audio_chunk
        start = mel_frame * hop - n_fft // 2


def _tokens_to_words(text: str, token_times: list[dict]) -> list[TimedWord]:
    import re

    pattern = re.compile(r"[^\W_]+(?:['’][^\W_]+)*", re.UNICODE)
    mapped = []
    cursor = 0
    for item in token_times:
        token = str(item.get('token', ''))
        mapped.append((cursor, cursor + len(token), float(item['start']), float(item['end'])))
        cursor += len(token)
    if cursor != len(text):
        # Decoder pieces should round-trip exactly; fail closed if tokenizer offsets differ.
        normalized = ''.join(str(item.get('token', '')) for item in token_times).strip()
        if normalized.casefold() != text.strip().casefold():
            raise ValueError('Nemotron token timestamps do not match decoded transcript')
        leading = len(''.join(str(item.get('token', '')) for item in token_times)) - len(normalized)
        mapped = [(start - leading, end - leading, begin, finish)
                  for start, end, begin, finish in mapped]
    words = []
    for match in pattern.finditer(text):
        spans = [(start, end) for left, right, start, end in mapped
                 if right > match.start() and left < match.end()]
        if spans:
            words.append(TimedWord(match.group(), min(start for start, _ in spans),
                                   max(end for _, end in spans)))
    return words


def decode_nemotron_streaming(model, processor, samples, sample_rate):
    processor.set_num_lookahead_tokens(processor.default_num_lookahead_tokens)
    first_size = processor.num_samples_first_audio_chunk
    first_audio = samples[:first_size]
    if len(first_audio) < first_size:
        import numpy as np
        first_audio = np.pad(first_audio, (0, first_size - len(first_audio)))
    first = processor(first_audio, sampling_rate=sample_rate, is_streaming=True,
                      is_first_audio_chunk=True, return_tensors='pt')
    features = _nemotron_stream_features(samples, processor, sample_rate, first.input_features)
    inputs = dict(first)
    inputs['input_features'] = features
    result = model.generate(**inputs, return_dict_in_generate=True)
    decoded, token_times = processor.decode(result.sequences, durations=result.durations, skip_special_tokens=True)
    text = decoded[0] if isinstance(decoded, list) else decoded
    timed = token_times[0] if token_times else []
    return text.strip(), _tokens_to_words(text, timed)


def load_qwen_aligner(settings: Settings, device: str):
    import torch
    try:
        from transformers import AutoProcessor, AutoModelForTokenClassification
    except ImportError as exc:
        raise RuntimeError('Transformers Qwen forced alignment requires transformers>=5.13') from exc
    checkpoint = 'Qwen/Qwen3-ForcedAligner-0.6B-hf'
    processor = AutoProcessor.from_pretrained(checkpoint)
    dtype = torch.float32 if device == 'cpu' else (torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float16)
    model = AutoModelForTokenClassification.from_pretrained(checkpoint, dtype=dtype, attn_implementation='sdpa').to(device).eval()
    return model, processor


def align_qwen(model, processor, samples, sample_rate, transcript, offset=0.0):
    import torch
    inputs, word_lists = processor.prepare_forced_aligner_inputs(
        audio=(samples, sample_rate), transcript=transcript, language='English')
    inputs = inputs.to(model.device, model.dtype)
    with torch.inference_mode():
        outputs = model(**inputs)
    results = processor.decode_forced_alignment(
        logits=outputs.logits, input_ids=inputs['input_ids'], word_lists=word_lists,
        timestamp_token_id=model.config.timestamp_token_id)[0]
    words = []
    previous_start = -1.0
    duration = len(samples) / sample_rate
    for item in results:
        text = str(item['text']).strip()
        start, end = float(item['start_time']), float(item['end_time'])
        if (not text or not math.isfinite(start) or not math.isfinite(end) or start < 0
                or start > end or end > duration + .05 or start < previous_start):
            raise ValueError('Qwen forced aligner returned invalid or non-monotonic timestamps')
        words.append(TimedWord(text, offset + start, offset + end))
        previous_start = start
    return words


def align_cached_chunks(vocals: Path, settings: Settings, chunks: list[Chunk],
                        device: str | None = None, aligner: tuple | None = None) -> list[Chunk]:
    import numpy as np
    import soundfile as sf
    import torch

    owns_aligner = aligner is None
    model, processor = aligner or (None, None)
    try:
        if owns_aligner:
            model, processor = load_qwen_aligner(settings, device or settings.device)
        sample_rate = int(processor.feature_extractor.sampling_rate)
        with tempfile.TemporaryDirectory(prefix='song-title-align-') as folder:
            normalized = Path(folder) / 'vocals-mono.wav'
            subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-y', '-i', str(vocals), '-ac', '1',
                            '-ar', str(sample_rate), '-c:a', 'pcm_f32le', str(normalized)], check=True,
                           capture_output=True, timeout=max(1, settings.worker_timeout - 15))
            samples, rate = sf.read(normalized, dtype='float32')
        centers = [(chunk.start + chunk.end) / 2 for chunk in chunks]
        result = []
        with torch.inference_mode():
            for index, chunk in enumerate(chunks):
                data = samples[round(chunk.start * rate):round(chunk.end * rate)]
                if not chunk.text.strip() or not data.size or np.max(np.abs(data)) < 1e-7:
                    result.append(Chunk(chunk.start, chunk.end, chunk.text, [], ''))
                    continue
                try:
                    words = align_qwen(model, processor, data, rate, chunk.text, chunk.start)
                    lower = 0.0 if index == 0 else (centers[index - 1] + centers[index]) / 2
                    upper = chunk.end if index == len(chunks) - 1 else (centers[index] + centers[index + 1]) / 2
                    words = [word for word in words if lower <= (word.start + word.end) / 2 < upper]
                    result.append(Chunk(chunk.start, chunk.end, chunk.text, words))
                except RuntimeError as exc:
                    if 'out of memory' in str(exc).casefold():
                        raise
                    result.append(Chunk(chunk.start, chunk.end, chunk.text, [], str(exc)))
                except (KeyError, TypeError, ValueError, IndexError) as exc:
                    result.append(Chunk(chunk.start, chunk.end, chunk.text, [], str(exc)))
        return result
    finally:
        if owns_aligner:
            del model, processor
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()


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


def with_device_retry(operation, requested: str, cuda_available: bool, *, verbose: bool = False):
    if requested == 'cuda' and not cuda_available:
        raise RuntimeError('CUDA requested but unavailable; use --device cpu')
    device = 'cuda' if requested != 'cpu' and cuda_available else 'cpu'
    for current, reduced in [(device,False)] + ([('cuda',True)] + ([('cpu',True)] if requested == 'auto' else []) if device == 'cuda' else []):
        try:
            return operation(current, reduced)
        except RuntimeError as exc:
            if 'out of memory' not in str(exc).lower() or current == 'cpu':
                raise
            if verbose:
                print(f'{current} memory exhausted; retrying with smaller segments or CPU.', flush=True)
    raise RuntimeError('CUDA out of memory after reduced-segment retry; use --device cpu or a smaller Qwen checkpoint')


def transcribe_in_process(vocals: Path, settings: Settings, device: str | None = None, reduced: bool = False,
                          backend: tuple | None = None, aligner: tuple | None = None) -> list[Chunk]:
    import numpy as np
    import soundfile as sf
    import torch
    owns_backend = backend is None
    owns_aligner = aligner is None and settings.asr == 'qwen'
    model, processor = backend or (None, None)
    aligner_model, aligner_processor = aligner or (None, None)
    try:
        if owns_backend:
            model, processor = load_backend(settings, device or settings.device)
        if owns_aligner:
            try:
                aligner_model, aligner_processor = load_qwen_aligner(settings, device or settings.device)
            except RuntimeError as exc:
                if 'out of memory' in str(exc).casefold():
                    raise
                aligner_model, aligner_processor = None, None
        sample_rate = int(processor.feature_extractor.sampling_rate)
        with tempfile.TemporaryDirectory(prefix='song-title-asr-') as folder:
            normalized = Path(folder)/'vocals-mono.wav'
            subprocess.run(['ffmpeg','-nostdin','-v','error','-y','-i',str(vocals),'-ac','1','-ar',str(sample_rate),'-c:a','pcm_f32le',str(normalized)], check=True, capture_output=True,
                           timeout=max(1, settings.worker_timeout - 15))
            samples, rate = sf.read(normalized, dtype='float32')
        duration = len(samples)/rate
        if settings.asr == 'nemotron':
            with torch.inference_mode():
                text, timed_words = decode_nemotron_streaming(model, processor, samples, rate)
            return [Chunk(0, duration, text, timed_words)]

        seconds = settings.chunk_seconds / 2 if reduced else settings.chunk_seconds
        overlap = min(settings.overlap_seconds, seconds/2) if reduced else settings.overlap_seconds
        ranges = chunk_ranges(duration, seconds, overlap)
        centers = [(start + end) / 2 for start, end in ranges]
        chunks = []
        with torch.inference_mode():
            for number, (start,end) in enumerate(ranges, 1):
                data = samples[round(start*rate):round(end*rate)]
                if settings.log_level == 'debug':
                    print(f'{settings.asr}: chunk {number}/{len(ranges)} ({start:.1f}–{end:.1f}s)', flush=True)
                if not data.size or np.max(np.abs(data)) < 1e-7:
                    text = ''
                    timed_words = []
                    alignment_error = ''
                elif settings.asr == 'qwen':
                    text, truncated = decode_qwen(model, processor, data, rate, 512)
                    if truncated:
                        text, truncated = decode_qwen(model, processor, data, rate, 1024)
                    if truncated:
                        raise RuntimeError(f'Qwen output truncated at {start:.1f}s; reduce --chunk-seconds')
                    timed_words = []
                    alignment_error = ''
                    if aligner_model is None:
                        alignment_error = 'Qwen forced aligner could not be loaded.'
                    else:
                        try:
                            timed_words = align_qwen(aligner_model, aligner_processor, data, rate, text, start)
                            lower = 0.0 if number == 1 else (centers[number - 2] + centers[number - 1]) / 2
                            upper = duration if number == len(ranges) else (centers[number - 1] + centers[number]) / 2
                            timed_words = [word for word in timed_words
                                           if lower <= (word.start + word.end) / 2 < upper]
                        except RuntimeError as exc:
                            if 'out of memory' in str(exc).casefold():
                                raise
                            alignment_error = str(exc)
                        except (KeyError, TypeError, ValueError, IndexError) as exc:
                            alignment_error = str(exc)
                else:
                    text = decode_nemotron(model, processor, data, rate)
                    timed_words = []
                    alignment_error = ''
                chunks.append(Chunk(start,end,text,timed_words,alignment_error))
        return chunks
    finally:
        if owns_backend:
            del model, processor
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        if owns_aligner:
            del aligner_model, aligner_processor
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()


def run_worker(phase: str, source: Path, output: Path, settings: Settings,
               cached_chunks: list[Chunk] | None = None):
    from .worker_runtime import ModelRuntime
    with ModelRuntime(settings) as runtime:
        if phase == 'separate':
            runtime.separate(source, output)
            return {'runtime': json.loads(output.with_suffix(output.suffix + '.runtime.json').read_text(encoding='utf-8'))}
        if phase == 'asr':
            chunks = runtime.transcribe(source, cached_chunks)
            sidecar = Path(source).parent/'transcript-worker.json.runtime.json'
            return {'chunks': [asdict(chunk) for chunk in chunks],
                    'runtime': json.loads(sidecar.read_text(encoding='utf-8'))}
        raise ValueError(f'Unknown worker phase: {phase}')


def separate_vocals(source: Path, output: Path, settings: Settings) -> Path:
    run_worker('separate', source, output, settings)
    if not output.is_file():
        raise RuntimeError('Separator did not produce a vocal stem')
    return output


def transcribe_vocals(vocals: Path, settings: Settings, cached_chunks: list[Chunk] | None = None) -> list[Chunk]:
    result = run_worker('asr', vocals, vocals.parent/'transcript-worker.json', settings, cached_chunks)
    return [Chunk(**item) for item in result['chunks']]

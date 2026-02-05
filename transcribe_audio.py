from __future__ import annotations

import argparse
import json
import logging
import os
import inspect
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import torch


def _ensure_torchaudio_compat() -> None:
    try:
        import torchaudio
    except Exception:
        return

    if not hasattr(torchaudio, "list_audio_backends"):
        torchaudio.list_audio_backends = lambda: ["soundfile"]
    if not hasattr(torchaudio, "set_audio_backend"):
        torchaudio.set_audio_backend = lambda _backend: None


_ensure_torchaudio_compat()

import whisperx
try:
    import whisperx.vad as whisperx_vad
except Exception:
    try:
        from whisperx import vad as whisperx_vad
    except Exception:
        whisperx_vad = None
from faster_whisper import WhisperModel
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score
from speechbrain.pretrained import EncoderClassifier


LOGGER = logging.getLogger("transcribe_audio")


@dataclass(frozen=True)
class TranscriptionConfig:
    model_size: str
    language: Optional[str]
    device: str
    compute_type: str
    batch_size: int
    beam_size: int
    cpu_threads: int
    num_workers: int
    asr_vad_filter: bool
    asr_vad_min_silence_ms: int
    asr_vad_max_speech_s: float


@dataclass(frozen=True)
class DiarizationConfig:
    enabled: bool
    min_speakers: int
    max_speakers: int
    num_speakers: Optional[int]
    vad_onset: float
    vad_offset: float
    vad_min_speech_s: float
    vad_max_speech_s: float
    max_gap_s: float


@dataclass(frozen=True)
class Segment:
    start: float
    end: float
    text: str
    speaker: Optional[str] = None


@dataclass(frozen=True)
class Word:
    start: float
    end: float
    text: str


class WhisperTranscriber:
    def __init__(self, config: TranscriptionConfig) -> None:
        self._config = config
        self._model = WhisperModel(
            config.model_size,
            device=config.device,
            compute_type=config.compute_type,
            cpu_threads=config.cpu_threads,
            num_workers=config.num_workers,
        )

    def transcribe(self, audio_input: str | np.ndarray, duration_s: float) -> tuple[list[dict], str]:
        start_time = time.perf_counter()
        kwargs = {
            "language": self._config.language,
            "task": "transcribe",
            "beam_size": self._config.beam_size,
            "vad_filter": self._config.asr_vad_filter,
            "vad_parameters": {
                "min_silence_duration_ms": self._config.asr_vad_min_silence_ms,
                "max_speech_duration_s": self._config.asr_vad_max_speech_s,
            },
        }
        if "batch_size" in inspect.signature(self._model.transcribe).parameters:
            kwargs["batch_size"] = self._config.batch_size

        segments, info = self._model.transcribe(audio_input, **kwargs)
        segment_list = []
        next_log = _progress_interval(duration_s)
        for segment in segments:
            segment_list.append({"start": segment.start, "end": segment.end, "text": segment.text})
            if next_log and segment.end >= next_log:
                elapsed = time.perf_counter() - start_time
                percent = min(100.0, (segment.end / duration_s) * 100.0)
                LOGGER.info(
                    "ASR progress: %.1f%% (%s / %s), elapsed %s",
                    percent,
                    format_duration(segment.end),
                    format_duration(duration_s),
                    format_duration(elapsed),
                )
                next_log += _progress_interval(duration_s)
        return segment_list, info.language


class WhisperAligner:
    def __init__(self, device: str) -> None:
        self._device = device

    def align(self, segments: list[dict], audio: np.ndarray, language: str) -> list[dict]:
        align_model, metadata = whisperx.load_align_model(language_code=language, device=self._device)
        aligned = whisperx.align(
            segments,
            align_model,
            metadata,
            audio,
            self._device,
            return_char_alignments=False,
        )
        return aligned.get("segments", [])


class WhisperXVadSegmenter:
    def __init__(self, config: DiarizationConfig, device: str) -> None:
        self._config = config
        self._device = device

    def segment(self, audio: np.ndarray, sample_rate: int) -> list[Segment]:
        if whisperx_vad is None:
            raise RuntimeError(
                "WhisperX VAD module not found. Ensure whisperx==3.7.6 is installed."
            )

        vad_model = whisperx_vad.load_vad_model(device=self._device)
        vad_options = whisperx_vad.VadOptions(
            onset=self._config.vad_onset,
            offset=self._config.vad_offset,
            min_speech_duration=self._config.vad_min_speech_s,
            max_speech_duration=self._config.vad_max_speech_s,
        )
        speech_timestamps = whisperx_vad.get_speech_timestamps(
            audio, vad_model, sample_rate=sample_rate, vad_options=vad_options
        )

        segments = self._normalize_segments(speech_timestamps, audio, sample_rate)
        return segments

    def _energy_vad(self, audio: np.ndarray, sample_rate: int) -> list[dict]:
        if audio.size == 0:
            return []

        frame_ms = 30
        frame_size = int(sample_rate * frame_ms / 1000)
        hop_size = frame_size // 2
        if frame_size <= 0 or hop_size <= 0:
            return []

        frames = []
        for start in range(0, len(audio) - frame_size + 1, hop_size):
            frame = audio[start : start + frame_size]
            energy = float(np.mean(frame * frame))
            frames.append((start, start + frame_size, energy))

        if not frames:
            return []

        energies = np.array([e for _, _, e in frames])
        threshold = max(1e-7, float(np.percentile(energies, 60)))

        speech_timestamps: list[dict] = []
        in_speech = False
        seg_start = 0
        for start, end, energy in frames:
            is_speech = energy >= threshold
            if is_speech and not in_speech:
                in_speech = True
                seg_start = start
            elif not is_speech and in_speech:
                in_speech = False
                speech_timestamps.append({"start": seg_start, "end": end})

        if in_speech:
            speech_timestamps.append({"start": seg_start, "end": frames[-1][1]})

        min_len = int(self._config.vad_min_speech_s * sample_rate)
        max_len = int(self._config.vad_max_speech_s * sample_rate)
        filtered: list[dict] = []
        for ts in speech_timestamps:
            length = ts["end"] - ts["start"]
            if length < min_len:
                continue
            if max_len > 0 and length > max_len:
                segments = self._split_segment(ts, max_len)
                filtered.extend(segments)
            else:
                filtered.append(ts)
        return filtered

    def _split_segment(self, ts: dict, max_len: int) -> list[dict]:
        segments: list[dict] = []
        start = ts["start"]
        end = ts["end"]
        while start < end:
            split_end = min(start + max_len, end)
            segments.append({"start": start, "end": split_end})
            start = split_end
        return segments

    def _normalize_segments(
        self,
        speech_timestamps: list[dict],
        audio: np.ndarray,
        sample_rate: int,
    ) -> list[Segment]:
        if not speech_timestamps:
            return []

        max_ts = max(ts["end"] for ts in speech_timestamps)
        audio_duration_s = audio.shape[0] / sample_rate
        scale = 1.0
        if max_ts > audio_duration_s * 2:
            scale = 1.0 / sample_rate

        return [
            Segment(start=ts["start"] * scale, end=ts["end"] * scale, text="")
            for ts in speech_timestamps
        ]


class SpeakerEmbedder:
    def __init__(self, device: str) -> None:
        self._device = device
        self._classifier = EncoderClassifier.from_hparams(
            source="speechbrain/spkrec-ecapa-voxceleb",
            run_opts={"device": device},
        )

    def embed(self, audio: np.ndarray, sample_rate: int, segments: list[Segment]) -> np.ndarray:
        embeddings: list[np.ndarray] = []
        for segment in segments:
            start = int(segment.start * sample_rate)
            end = int(segment.end * sample_rate)
            chunk = audio[start:end]
            if chunk.size == 0:
                embeddings.append(np.zeros((192,), dtype=np.float32))
                continue
            waveform = torch.tensor(chunk, dtype=torch.float32).unsqueeze(0)
            with torch.no_grad():
                embedding = self._classifier.encode_batch(waveform).squeeze(0).cpu().numpy()
            embeddings.append(embedding)
        return np.vstack(embeddings) if embeddings else np.empty((0, 192), dtype=np.float32)


class SpeakerClusterer:
    def __init__(self, config: DiarizationConfig) -> None:
        self._config = config

    def cluster(self, embeddings: np.ndarray) -> list[int]:
        if embeddings.size == 0:
            return []
        n_segments = embeddings.shape[0]
        if n_segments == 1:
            return [0]
        num_speakers = self._config.num_speakers
        if num_speakers is None:
            num_speakers = self._infer_speaker_count(embeddings)
        num_speakers = max(1, min(num_speakers, n_segments))
        clustering = AgglomerativeClustering(
            n_clusters=num_speakers,
            metric="cosine",
            linkage="average",
        )
        return clustering.fit_predict(embeddings).tolist()

    def _infer_speaker_count(self, embeddings: np.ndarray) -> int:
        n_segments = embeddings.shape[0]
        max_k = min(self._config.max_speakers, n_segments)
        min_k = min(self._config.min_speakers, max_k)
        if max_k <= 1:
            return 1

        best_k = min_k
        best_score = -1.0
        for k in range(min_k, max_k + 1):
            if k <= 1:
                continue
            labels = AgglomerativeClustering(
                n_clusters=k,
                metric="cosine",
                linkage="average",
            ).fit_predict(embeddings)
            if len(set(labels)) < 2:
                continue
            score = silhouette_score(embeddings, labels, metric="cosine")
            if score > best_score:
                best_score = score
                best_k = k
        return best_k


class SegmentAssembler:
    def __init__(self, config: DiarizationConfig) -> None:
        self._config = config

    def assemble(
        self,
        aligned_segments: list[dict],
        diarization_segments: list[Segment],
        speaker_labels: list[int],
    ) -> list[Segment]:
        speaker_map = self._assign_speakers(diarization_segments, speaker_labels)
        words = self._extract_words(aligned_segments)
        if not words:
            return self._fallback_segments(aligned_segments, speaker_map)
        word_segments = [
            Segment(
                start=word.start,
                end=word.end,
                text=word.text,
                speaker=self._speaker_for_word(word, speaker_map),
            )
            for word in words
        ]
        return self._merge_segments(word_segments)

    def _assign_speakers(
        self, segments: list[Segment], labels: list[int]
    ) -> list[Segment]:
        updated: list[Segment] = []
        for segment, label in zip(segments, labels):
            updated.append(
                Segment(
                    start=segment.start,
                    end=segment.end,
                    text=segment.text,
                    speaker=f"Speaker {label + 1}",
                )
            )
        return updated

    def _extract_words(self, aligned_segments: list[dict]) -> list[Word]:
        words: list[Word] = []
        for segment in aligned_segments:
            for word in segment.get("words", []):
                if "start" not in word or "end" not in word:
                    continue
                text = word.get("word", "").strip()
                if not text:
                    continue
                words.append(Word(start=word["start"], end=word["end"], text=text))
        return words

    def _speaker_for_word(self, word: Word, diarization_segments: list[Segment]) -> Optional[str]:
        best_speaker = None
        best_overlap = 0.0
        for segment in diarization_segments:
            overlap = max(0.0, min(word.end, segment.end) - max(word.start, segment.start))
            if overlap > best_overlap:
                best_overlap = overlap
                best_speaker = segment.speaker
        return best_speaker

    def _merge_segments(self, segments: list[Segment]) -> list[Segment]:
        merged: list[Segment] = []
        for segment in segments:
            if not merged:
                merged.append(segment)
                continue
            last = merged[-1]
            gap = segment.start - last.end
            if segment.speaker == last.speaker and gap <= self._config.max_gap_s:
                merged[-1] = Segment(
                    start=last.start,
                    end=segment.end,
                    text=f"{last.text} {segment.text}",
                    speaker=last.speaker,
                )
            else:
                merged.append(segment)
        return merged

    def _fallback_segments(
        self, aligned_segments: list[dict], diarization_segments: list[Segment]
    ) -> list[Segment]:
        fallback: list[Segment] = []
        for segment in aligned_segments:
            speaker = None
            if diarization_segments:
                word = Word(
                    start=segment.get("start", 0.0),
                    end=segment.get("end", 0.0),
                    text="",
                )
                speaker = self._speaker_for_word(word, diarization_segments)
            fallback.append(
                Segment(
                    start=segment.get("start", 0.0),
                    end=segment.get("end", 0.0),
                    text=segment.get("text", "").strip(),
                    speaker=speaker,
                )
            )
        return fallback


class OutputWriter:
    def write(self, segments: list[Segment], output_path: Path) -> None:
        raise NotImplementedError


class SrtWriter(OutputWriter):
    def __init__(self, include_speaker_tags: bool) -> None:
        self._include_speaker_tags = include_speaker_tags

    def write(self, segments: list[Segment], output_path: Path) -> None:
        lines: list[str] = []
        for index, segment in enumerate(segments, start=1):
            lines.append(str(index))
            lines.append(f"{format_srt_time(segment.start)} --> {format_srt_time(segment.end)}")
            text = segment.text
            if self._include_speaker_tags and segment.speaker:
                text = f"[{segment.speaker}] {text}"
            lines.append(text)
            lines.append("")
        output_path.write_text("\n".join(lines), encoding="utf-8")


class VttWriter(OutputWriter):
    def write(self, segments: list[Segment], output_path: Path) -> None:
        lines = ["WEBVTT", ""]
        for segment in segments:
            lines.append(f"{format_vtt_time(segment.start)} --> {format_vtt_time(segment.end)}")
            text = segment.text
            if segment.speaker:
                text = f"[{segment.speaker}] {text}"
            lines.append(text)
            lines.append("")
        output_path.write_text("\n".join(lines), encoding="utf-8")


class JsonWriter(OutputWriter):
    def write(self, segments: list[Segment], output_path: Path) -> None:
        payload = [
            {
                "start": segment.start,
                "end": segment.end,
                "speaker": segment.speaker,
                "text": segment.text,
            }
            for segment in segments
        ]
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class TextWriter(OutputWriter):
    def write(self, segments: list[Segment], output_path: Path) -> None:
        lines = []
        for segment in segments:
            if segment.speaker:
                lines.append(f"{segment.speaker}: {segment.text}")
            else:
                lines.append(segment.text)
        output_path.write_text("\n".join(lines), encoding="utf-8")


class AudioTranscriber:
    def __init__(
        self,
        transcription_config: TranscriptionConfig,
        diarization_config: DiarizationConfig,
    ) -> None:
        self._transcriber = WhisperTranscriber(transcription_config)
        self._language = transcription_config.language
        self._diarization_enabled = diarization_config.enabled
        self._aligner = WhisperAligner(transcription_config.device)
        if self._diarization_enabled:
            self._segmenter = WhisperXVadSegmenter(
                diarization_config, transcription_config.device
            )
            self._embedder = SpeakerEmbedder(transcription_config.device)
            self._clusterer = SpeakerClusterer(diarization_config)
            self._assembler = SegmentAssembler(diarization_config)
        else:
            self._segmenter = None
            self._embedder = None
            self._clusterer = None
            self._assembler = None

    def transcribe(self, audio_path: Path) -> list[Segment]:
        audio = whisperx.load_audio(str(audio_path))
        duration_s = audio.shape[0] / 16000
        LOGGER.info("Loaded audio: %s (%s)", audio_path.name, format_duration(duration_s))
        segments, detected_language = self._transcriber.transcribe(audio, duration_s)
        language = detected_language or self._language or "en"
        aligned_segments = self._aligner.align(segments, audio, language)

        if not self._diarization_enabled:
            return [
                Segment(
                    start=segment.get("start", 0.0),
                    end=segment.get("end", 0.0),
                    text=segment.get("text", "").strip(),
                    speaker=None,
                )
                for segment in aligned_segments
            ]

        sample_rate = 16000
        diarization_segments = self._segmenter.segment(audio, sample_rate)
        embeddings = self._embedder.embed(audio, sample_rate, diarization_segments)
        labels = self._clusterer.cluster(embeddings)

        return self._assembler.assemble(aligned_segments, diarization_segments, labels)


def format_srt_time(seconds: float) -> str:
    milliseconds = int(round(seconds * 1000.0))
    hours = milliseconds // 3_600_000
    minutes = (milliseconds % 3_600_000) // 60_000
    seconds = (milliseconds % 60_000) // 1000
    millis = milliseconds % 1000
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{millis:03d}"


def format_vtt_time(seconds: float) -> str:
    milliseconds = int(round(seconds * 1000.0))
    hours = milliseconds // 3_600_000
    minutes = (milliseconds % 3_600_000) // 60_000
    seconds = (milliseconds % 60_000) // 1000
    millis = milliseconds % 1000
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}"


def format_duration(seconds: float) -> str:
    total_seconds = int(round(seconds))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    secs = total_seconds % 60
    if hours > 0:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:d}:{secs:02d}"


def _progress_interval(duration_s: float) -> float:
    if duration_s <= 0:
        return 0.0
    return max(10.0, min(60.0, duration_s * 0.05))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transcribe meeting audio with WhisperX alignment and ECAPA diarization."
    )
    parser.add_argument("input", type=Path, help="Path to an audio or video file.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output directory.")
    parser.add_argument(
        "--outputs",
        nargs="+",
        choices=["srt", "vtt", "json", "txt"],
        default=["srt", "json", "txt"],
        help="Output formats to write.",
    )
    parser.add_argument(
        "--srt-speaker-tags",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable speaker tags in SRT output.",
    )
    parser.add_argument(
        "--diarize",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable speaker diarization.",
    )
    parser.add_argument("--model", default="medium", help="Faster-Whisper model size.")
    parser.add_argument("--language", default=None, help="Language code, e.g. en.")
    parser.add_argument("--device", default="cpu", help="Device for inference.")
    parser.add_argument("--compute-type", default="int8", help="Compute type for faster-whisper.")
    parser.add_argument("--batch-size", type=int, default=8, help="Transcription batch size.")
    parser.add_argument("--beam-size", type=int, default=5, help="Beam size for decoding.")
    parser.add_argument(
        "--cpu-threads",
        type=int,
        default=0,
        help="CPU threads to use (0 = auto).",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=0,
        help="Worker threads for decoding (0 = auto).",
    )
    parser.add_argument(
        "--asr-vad",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable faster-whisper VAD for ASR.",
    )
    parser.add_argument("--asr-vad-min-silence-ms", type=int, default=300)
    parser.add_argument("--asr-vad-max-speech-s", type=float, default=30.0)
    parser.add_argument("--min-speakers", type=int, default=2)
    parser.add_argument("--max-speakers", type=int, default=8)
    parser.add_argument("--num-speakers", type=int, default=None)
    parser.add_argument("--vad-onset", type=float, default=0.5)
    parser.add_argument("--vad-offset", type=float, default=0.363)
    parser.add_argument("--vad-min-speech-s", type=float, default=0.25)
    parser.add_argument("--vad-max-speech-s", type=float, default=30.0)
    parser.add_argument("--max-gap-s", type=float, default=0.8)
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def build_transcriber(args: argparse.Namespace) -> AudioTranscriber:
    transcription_config = TranscriptionConfig(
        model_size=args.model,
        language=args.language,
        device=args.device,
        compute_type=args.compute_type,
        batch_size=args.batch_size,
        beam_size=args.beam_size,
        cpu_threads=args.cpu_threads,
        num_workers=args.num_workers,
        asr_vad_filter=args.asr_vad,
        asr_vad_min_silence_ms=args.asr_vad_min_silence_ms,
        asr_vad_max_speech_s=args.asr_vad_max_speech_s,
    )
    diarization_config = DiarizationConfig(
        enabled=args.diarize,
        min_speakers=args.min_speakers,
        max_speakers=args.max_speakers,
        num_speakers=args.num_speakers,
        vad_onset=args.vad_onset,
        vad_offset=args.vad_offset,
        vad_min_speech_s=args.vad_min_speech_s,
        vad_max_speech_s=args.vad_max_speech_s,
        max_gap_s=args.max_gap_s,
    )
    return AudioTranscriber(transcription_config, diarization_config)


def resolve_cpu_threads(value: int) -> int:
    if value > 0:
        return value
    cpu_count = os.cpu_count() or 4
    return max(1, min(cpu_count, 8))


def resolve_num_workers(value: int, cpu_threads: int) -> int:
    if value > 0:
        return value
    return max(1, min(cpu_threads, 2))


def write_outputs(
    segments: list[Segment],
    output_dir: Path,
    stem: str,
    outputs: Iterable[str],
    srt_speaker_tags: bool,
) -> None:
    writers: dict[str, OutputWriter] = {
        "srt": SrtWriter(include_speaker_tags=srt_speaker_tags),
        "vtt": VttWriter(),
        "json": JsonWriter(),
        "txt": TextWriter(),
    }
    for output_format in outputs:
        writer = writers[output_format]
        output_path = output_dir / f"{stem}.{output_format}"
        writer.write(segments, output_path)
        LOGGER.info("Wrote %s", output_path)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(levelname)s: %(message)s")

    start_time = time.perf_counter()

    args.cpu_threads = resolve_cpu_threads(args.cpu_threads)
    args.num_workers = resolve_num_workers(args.num_workers, args.cpu_threads)

    audio_path = args.input
    if not audio_path.exists():
        raise FileNotFoundError(f"Input not found: {audio_path}")

    if args.output_dir is None:
        output_dir = audio_path.parent
    else:
        output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    torch.set_num_threads(args.cpu_threads)
    torch.set_num_interop_threads(min(4, args.cpu_threads))

    transcriber = build_transcriber(args)
    segments = transcriber.transcribe(audio_path)

    write_outputs(
        segments,
        output_dir,
        audio_path.stem,
        args.outputs,
        srt_speaker_tags=args.srt_speaker_tags,
    )

    elapsed = time.perf_counter() - start_time
    LOGGER.info("Elapsed time: %s", format_duration(elapsed))


if __name__ == "__main__":
    main()

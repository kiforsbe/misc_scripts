from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    asr: str = 'nemotron'
    asr_model: str | None = None
    device: str = 'auto'
    separator_device: str | None = None
    separator_model: str = 'htdemucs'
    chunk_seconds: float = 30
    overlap_seconds: float = 4
    ollama_model: str = 'qwen3.5:4b'
    ollama_host: str = 'http://localhost:11434'
    connect_timeout: float = 10
    inference_timeout: float = 300
    worker_timeout: float = 3600
    log_level: str = 'info'
    cache_dir: Path = field(default_factory=lambda: Path('.song-title-cache'))
    refresh: bool = False

    def __post_init__(self):
        if self.asr not in {'qwen', 'nemotron'}:
            raise ValueError('ASR must be qwen or nemotron')
        if self.device not in {'auto', 'cpu', 'cuda'} or self.separator_device not in {None, 'auto', 'cpu', 'cuda'}:
            raise ValueError('Device must be auto, cpu or cuda')
        if self.log_level not in {'info', 'debug'}:
            raise ValueError('Log level must be info or debug')
        values = (self.chunk_seconds, self.overlap_seconds, self.connect_timeout, self.inference_timeout, self.worker_timeout)
        if not all(math.isfinite(v) for v in values):
            raise ValueError('Durations must be finite')
        if self.chunk_seconds <= 0 or not 0 <= self.overlap_seconds < self.chunk_seconds:
            raise ValueError('Require chunk duration > 0 and 0 <= overlap < chunk duration')
        if min(self.connect_timeout, self.inference_timeout, self.worker_timeout) <= 0:
            raise ValueError('Timeouts must be positive')

    @property
    def checkpoint(self) -> str:
        return self.asr_model or ({'qwen':'Qwen/Qwen3-ASR-1.7B-hf', 'nemotron':'nvidia/nemotron-speech-streaming-en-0.6b'}[self.asr])


@dataclass
class Chunk:
    start: float
    end: float
    text: str


@dataclass
class Candidate:
    title: str
    rationale: str
    evidence: list[str]
    metadata_support: str = ''


@dataclass
class Analysis:
    source: Path
    digest: str
    metadata: dict
    chunks: list[Chunk]
    lyrics: str
    candidates: list[Candidate]
    notes: list[str] = field(default_factory=list)
    report_path: Path | None = None
    selected_title: str | None = None
    runtime: dict = field(default_factory=dict)
    formatted_lyrics: str = ''

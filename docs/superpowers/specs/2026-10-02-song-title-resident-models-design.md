# Song title resident models design

## Goal

Keep the song-title tool's Demucs separator, selected ASR model, and configured Ollama model loaded across tracks processed by one CLI invocation. The next track should reuse those models when its vocal stem or transcript is not already cached. Keep the existing track-by-track analysis and review flow.

## Current behavior

`cli.main` calls `pipeline.analyze_file` for each input. For each file, the pipeline may start one short-lived subprocess to separate vocals, a second short-lived subprocess to transcribe them, and then make title and optional filename-shortening requests to Ollama. The worker subprocesses exit after their single job, and the Ollama requests set `keep_alive` to `0`. This reliably releases memory between phases, but reloads models for subsequent tracks.

## Design

### Resident local workers

Create one lazily started, long-lived worker for Demucs and one for ASR per CLI invocation. Keep the existing JSON request/result boundaries but make each worker accept multiple jobs over a newline-delimited JSON protocol. Load each worker's model on its first cache miss and retain the loaded model and processor for later jobs. Do not start a worker for a phase whose work is entirely satisfied by cache.

The Demucs worker must reuse one loaded separator model instead of invoking a fresh CLI model load for each track. The ASR worker must reuse the selected checkpoint's model and processor. Requests remain sequential within each worker. The parent CLI keeps its current per-file order and review behavior.

Retain the existing device selection and CUDA retry intent. A retry must release any failed phase model before reloading it with reduced segment settings or on CPU. If a retained model in another phase prevents an operation from fitting, report an actionable memory error for that track; do not silently change the selected ASR or separator model.

### Ollama lifecycle

Set `keep_alive: -1` on title suggestion and filename-shortening requests, so the configured Ollama model remains loaded across requests during the invocation. At CLI shutdown, send an empty request with `keep_alive: 0` for only the configured model to release the retention requested by this run. Ignore cleanup failures after preserving the primary result or error. Do not unload other Ollama models.

### Shutdown and failures

Manage both local workers from a run-scoped context in the CLI. Close their input streams and wait for clean exits in `finally`; if a worker hangs or the user interrupts the run, terminate it and reap the process. A failed job is returned to the existing per-track exception handling, while the worker remains usable when its protocol and process are healthy. A crashed or protocol-desynchronized worker is reported for that track and is restarted lazily for a later track.

### Compatibility and documentation

No new command-line option is needed: resident model reuse is the default behavior requested for a run. Preserve the existing CLI arguments, per-track order, output format, cache keys, and report content. Update the memory section of `song_title/README.md` to describe simultaneous model residency during a run, shutdown cleanup, and the increased sustained RAM/VRAM requirement.

## Validation

Review the lifecycle paths to verify each local model is loaded once across multiple jobs, cache hits do not start workers, device retries release and reload the relevant model, worker shutdown runs on normal completion and exceptions, and Ollama requests retain then unload only the configured model. Review the existing `song_title/tests` suite for compatibility coverage; real model accuracy and hardware-specific memory fit are outside this design's validation scope.

## Risks

All three models can be resident at once, so the sustained RAM/VRAM requirement is higher than the current phase-isolated design. On devices that cannot hold them together, the tool can use existing CPU/reduced-segment paths where possible, but a track may still fail with an actionable memory error. The Ollama cleanup request targets the configured model on the shared Ollama server, so it may also unload that same model if another client was using it.

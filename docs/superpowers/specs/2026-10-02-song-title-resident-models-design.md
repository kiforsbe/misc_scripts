# Song title resident models design

## Goal

Keep the song-title tool's Demucs separator, selected ASR model, and configured Ollama model loaded across tracks processed by one CLI invocation. The next track should reuse those models when its vocal stem or transcript is not already cached. Only suggest a new title when the audio metadata has no nonblank title. For a track with an existing title, run transcription and lyric formatting only when embedded lyrics are missing or `--force-lyrics` requests replacement. Use its existing title with the normal rename function. Keep the existing track-by-track analysis and review flow for tracks that need a title suggestion.

## Current behavior

`cli.main` calls `pipeline.analyze_file` for each input. For each file, the pipeline may start one short-lived subprocess to separate vocals, a second short-lived subprocess to transcribe them, and then make title and optional filename-shortening requests to Ollama. The worker subprocesses exit after their single job, and the Ollama requests set `keep_alive` to `0`. This reliably releases memory between phases, but reloads models for subsequent tracks.

## Design

### Resident local workers

Create one lazily started, long-lived worker for Demucs and one for ASR per CLI invocation. Keep the existing JSON request/result boundaries but make each worker accept multiple jobs over a newline-delimited JSON protocol. Load each worker's model on its first cache miss and retain the loaded model and processor for later jobs. Do not start a worker for a phase whose work is entirely satisfied by cache.

The Demucs worker must reuse one loaded separator model instead of invoking a fresh CLI model load for each track. The ASR worker must reuse the selected checkpoint's model and processor. Requests remain sequential within each worker. The parent CLI keeps its current per-file processing order and its review behavior for tracks that need a title suggestion.

### Existing-title tracks

Treat a title as present when the metadata title is a nonblank string after trimming whitespace. Treat embedded lyrics as present when the metadata lyrics value is a nonblank string after trimming whitespace. For a track with an existing title and existing lyrics, skip Demucs, ASR, title suggestions, and lyric formatting unless `--force-lyrics` is set. If embedded lyrics are missing or `--force-lyrics` is set, run separation and ASR; call Ollama for lyric formatting only, never for title suggestions. Never display title candidates, prompt for a replacement title, or update the existing title tag for a titled track. Use the existing metadata title for the usual filename template and call the existing `output_path` and no-clobber rename flow automatically for every titled track, subject to normal `--dry-run` behavior. Tracks with a missing or blank title retain the current suggestion, confirmation, and rename behavior.

Keep title suggestion and lyric formatting as separate operations so a titled track with missing lyrics can format lyrics without soliciting candidates. Both requests use the resident configured Ollama model. When an existing-title track already has lyrics and forced replacement is not requested, avoid both operations.

Retain the existing device selection and CUDA retry intent. A retry must release any failed phase model before reloading it with reduced segment settings or on CPU. If a retained model in another phase prevents an operation from fitting, report an actionable memory error for that track; do not silently change the selected ASR or separator model.

### Ollama lifecycle

Set `keep_alive: -1` on title suggestion and filename-shortening requests, so the configured Ollama model remains loaded across requests during the invocation. At CLI shutdown, send an empty request with `keep_alive: 0` for only the configured model to release the retention requested by this run. Ignore cleanup failures after preserving the primary result or error. Do not unload other Ollama models.

### Shutdown and failures

Manage both local workers from a run-scoped context in the CLI. Close their input streams and wait for clean exits in `finally`; if a worker hangs or the user interrupts the run, terminate it and reap the process. A failed job is returned to the existing per-track exception handling, while the worker remains usable when its protocol and process are healthy. A crashed or protocol-desynchronized worker is reported for that track and is restarted lazily for a later track.

### Compatibility and documentation

No new command-line option is needed: resident model reuse and existing-title handling are default behavior. Preserve the existing CLI arguments, per-track order, cache keys, and report format; reports must make clear when a title was retained from metadata rather than suggested and when inference was skipped because lyrics already existed. Update the memory and usage sections of `song_title/README.md` to describe simultaneous model residency during a run, shutdown cleanup, the increased sustained RAM/VRAM requirement, and title suggestions being limited to tracks without a metadata title. Explain that titled tracks are transcribed only when lyrics are missing or forced replacement is requested.

## Validation

Review the lifecycle paths to verify each local model is loaded once across multiple jobs, cache hits do not start workers, device retries release and reload the relevant model, worker shutdown runs on normal completion and exceptions, and Ollama requests retain then unload only the configured model. Review the CLI flow to verify titled tracks with existing lyrics skip inference by default, titled tracks with missing or forced lyrics receive formatting without title suggestions, preserve their title tags, and rename through the normal function, while untitled tracks retain the existing suggestion flow. Review the existing `song_title/tests` suite for compatibility coverage; real model accuracy and hardware-specific memory fit are outside this design's validation scope.

## Risks

All three models can be resident at once, so the sustained RAM/VRAM requirement is higher than the current phase-isolated design. On devices that cannot hold them together, the tool can use existing CPU/reduced-segment paths where possible, but a track may still fail with an actionable memory error. The Ollama cleanup request targets the configured model on the shared Ollama server, so it may also unload that same model if another client was using it.

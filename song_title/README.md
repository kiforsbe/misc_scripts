# Song title suggestions

Suggest English titles for original songs from their lyrics and existing audio metadata. Supports local Qwen and Nemotron transcription and uses Ollama `qwen3.5:4b` by default for title suggestions.

The processing order is always **decode → Demucs vocal stem → overlapping vocal chunks → ASR → Ollama → review → lyrics/tag updates**. Vocal separation is mandatory. A separation error stops that file; the mixed song is never passed to ASR.

## Installation (Python 3.13)

Use the Python environment already configured for this repository. Activate it before running these commands, and use the same `python` executable for installing dependencies and running the tool. This project does not need a separate virtual environment.

```powershell
python --version
python -m pip --version
```

Install a PyTorch build appropriate for your device before the tool requirements. For CPU:

```powershell
python -m pip install torch --index-url https://download.pytorch.org/whl/cpu
```

For NVIDIA GPUs, select the matching Windows/Python/CUDA command from [PyTorch's installer](https://pytorch.org/get-started/locally/). Then:

```powershell
python -m pip install -r song_title/requirements.txt
```

FFmpeg and FFprobe must be on PATH. Install [Ollama](https://ollama.com/download), start its server/app, and obtain the default title model:

```powershell
ollama pull qwen3.5:4b
```

Demucs and ASR weights download on their first use. Later runs reuse downloaded weights and analysis caches. No audio is sent to a transcription service. Lyrics and metadata are sent to the configured Ollama host, which defaults to localhost. Cached inference can run without internet access once all weights are present.

## Usage

Run these commands with the same `python` executable where you installed the dependencies.

```powershell
python -m song_title "song1.mp3" "song2.flac"
python -m song_title "D:\Original Songs" --recursive --asr nemotron
python -m song_title "song.mp3" --asr qwen --asr-model Qwen/Qwen3-ASR-0.6B-hf
python -m song_title "song.mp3" --device cpu --dry-run
python -m song_title "song.mp3" --auto --backup
python -m song_title "song.mp3" --auto --filename-template '%album% - %artist% - $num(%tracknumber%,2) - %title%'
```

The CLI defaults to Nemotron with the English-specific [nvidia/nemotron-speech-streaming-en-0.6b](https://huggingface.co/nvidia/nemotron-speech-streaming-en-0.6b). `--asr qwen` uses [Qwen/Qwen3-ASR-1.7B-hf](https://huggingface.co/Qwen/Qwen3-ASR-1.7B-hf). Both use native Transformers; `transformers>=5.13` is required. Only the selected backend runs. To compare, rerun with the other backend; the isolated vocal stem is reused.

The program offers up to three suggestions with reasons and verified lyric excerpts. Choose a number, enter your own title (`e`), skip (`s`), or quit (`q`). A separate confirmation, defaulting to no, is required to write a selected title. Ollama formats the transcript into phrase-based lyric lines and stanzas in the same request as title suggestions. The formatter may adjust layout, capitalization, and punctuation, but its word sequence is checked against the transcript; if it changes or omits words, conservative local formatting is used instead. Lyrics are written beside the audio as `song.lyrics.txt` and embedded when the file has no existing lyrics tag. Existing lyrics and sidecar files are kept unless `--force-lyrics` is supplied. `--dry-run` generates reports without interactive prompts or source updates.

`--auto` accepts the first title suggestion, writes it to the tags, and renames the file without prompting. When a title is confirmed interactively, the file is renamed with the same default template. The default naming template is `%album% - %artist% - $num(%tracknumber%,2) - %title%`, which produces names such as `Album Name - Artist Name - 01 - Track Title.mp3`; if album, artist, or track metadata is missing, its empty segment and separators are omitted. Filenames are limited to 90 characters by default. If the rendered filename exceeds the limit, Ollama shortens the populated metadata fields used by the template and the filename is rendered again. The formatter preserves original words and spacing, removing complete words or trailing descriptive phrases when needed. Shortened fields are cached in `.song-title-cache/filename-shortening.json` by model, field, original value, and limit, so repeated album or artist metadata reuses the cached wording. Cached entries that alter or merge source words are ignored. If a rendered name still cannot fit, the rename is skipped. Use `--max-filename-length` to change the limit; filename shortening does not change audio metadata. Filesystem-invalid characters are stripped from filename components. A custom `--filename-template` uses MusicBrainz Picard-style `%field%` variables and `$num(%tracknumber%,2)`; template slashes create subfolders under the source folder. The current implementation supports `%title%`, `%artist%`, `%albumartist%`, `%album%`, `%tracknumber%`, `%discnumber%`, `%date%`, `%genre%`, and `%_extension%` variables plus `$num()`. Generated path components are sanitized and existing destinations are never overwritten.

## Files and reports

Tag updates support MP3, FLAC, M4A/M4B/MP4 audio, Ogg Vorbis, and Opus. Other supported decoding inputs (including WAV) can receive suggestions, reports, and lyrics sidecars. Tag edits happen in place by default: the tool edits a temporary copy, verifies it, checks that the source has not changed, and replaces the original without re-encoding audio. Pass `--backup` to preserve the original as `song.mp3.bak` (numbered `.bak.1`, `.bak.2`, etc. if needed). Only title and lyrics tags selected for update are changed; supported metadata and artwork are retained.

Reports and caches default to `.song-title-cache/`:

```text
vocals/<content-key>/vocals.wav      # Reusable isolated vocal stem
vocals/<content-key>/manifest.json
transcripts/<configuration-key>.json
reports/<source-and-backend-key>.json
filename-shortening.json
reports/<source-and-backend-key>.txt
```

JSON records raw chunk texts and timing offsets, assembled lyrics, metadata context, suggestions, the selected title, configuration, runtime device/peak PyTorch GPU allocation, errors, and save outcome. Timing offsets describe chunks rather than exact word timings. The source-content hash includes tags, so changing tags can invalidate the vocal cache. Corrupt or incomplete cached entries are regenerated. Reports remain local and contain transcribed lyrics. Errors before a source can be read or the report directory can be written appear on stderr.

## Memory and quality

Separation and ASR run in separate, short-lived worker processes; they finish before Ollama starts. The title request uses `keep_alive: 0`, unloading that requested model afterward. The tool does not unload unrelated Ollama models.

Default ASR segments are 30 seconds with four seconds of overlap, processed one at a time. Demucs uses six-second internal segments. Automatic device mode retries CUDA memory errors with smaller segments, then CPU. Explicit `--device cuda` reports an error after its smaller-segment retry; `--device cpu` also requests CPU inference from Ollama. A 16 GB GPU is an upper testing target, not a minimum; actual memory needs depend on the checkpoint, free memory, and context size.

Both vocal separation and ASR can make mistakes. All chunk text is preserved, including repeated choruses; overlapping chunks can introduce duplicate phrases because these adapters do not provide exact word timestamps. Review the raw transcript/vocal stem when suggestions seem wrong. The tool abstains from lyric-based suggestions for empty transcripts and accepts manual titles.

## Options and troubleshooting

Run `python -m song_title --help` for all options:

- `--asr qwen|nemotron`, `--asr-model CHECKPOINT`
- `--device auto|cuda|cpu`, `--separator-device auto|cuda|cpu`
- `--separator-model htdemucs`
- `--chunk-seconds 30`, `--overlap-seconds 4`
- `--ollama-model qwen3.5:4b`, `--ollama-host http://localhost:11434`
- `--output-dir .song-title-cache`, `--refresh`
- `--connect-timeout 10`, `--inference-timeout 300`, `--worker-timeout 3600`
- `--recursive`, `--dry-run`, `--auto`
- `--backup`, `--force-lyrics`
- `--filename-template TEMPLATE` (Picard-style syntax; default `%album% - %artist% - $num(%tracknumber%,2) - %title%`)
- `--max-filename-length 90` (Ollama shortens template fields above this limit)
- `--color` / `--no-color` (default: detect terminal support)

Model downloads or CPU processing can take longer than the default worker timeout; increase it when needed. For Qwen output truncation reduce `--chunk-seconds`. Ollama connection errors require starting its server; missing models require `ollama pull`. Transcripts and vocal stems are cached even if title generation fails, so rerunning resumes the expensive work.

Run tests from the repository root with `python -m pytest song_title/tests -q`. Tests include real encoded audio/tag fixtures and mocked external model/HTTP boundaries; they do not establish singing accuracy. Compare representative songs with reference lyrics to evaluate that.

The feature lives entirely in `song_title/`: `cli.py` handles review, `pipeline.py` orchestrates caching/reports, `audio.py` handles discovery/chunk joining, `asr.py` and `worker.py` run the local models, `titles.py` calls Ollama, and `metadata.py` updates tags. There is no extra helper package. Code shared with other tools belongs in `common/`; none of these components currently has another consumer.

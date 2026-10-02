from __future__ import annotations

import argparse
import importlib.metadata
import os
import shutil
import sys
from pathlib import Path

from common.presentation import Colors

from .audio import discover_inputs
from .lyrics import write_lyrics_file, write_timed_lyrics_file
from .metadata import can_write, has_meaningful_title, update_metadata
from .naming import DEFAULT_FILENAME_TEMPLATE, DEFAULT_MAX_FILENAME_LENGTH, FilenameTooLongError, output_path
from .pipeline import analyze_file, refresh_timed_cache_source, save_report
from .title_history import load_title_history, save_title_history, similar_title
from .types import Settings
from .worker_runtime import ModelRuntime

_USE_COLOR = False


def _c(text, color):
    return Colors.wrap(str(text), color, _USE_COLOR)


def parser():
    result = argparse.ArgumentParser(description='Suggest titles for original songs from isolated vocals, preserve lyrics, and optionally update tags.')
    result.add_argument('inputs', nargs='+', type=Path, help='One or more files or directories')
    result.add_argument('--recursive', action='store_true')
    result.add_argument('--asr', choices=['qwen', 'nemotron'], default='nemotron', help='Automatic speech recognition model for English lyrics')
    result.add_argument('--asr-model', help='Compatible Hugging Face checkpoint or local model directory')
    result.add_argument('--device', choices=['auto', 'cuda', 'cpu'], default='auto', help='Device for ASR and separation; "auto" uses GPU if available')
    result.add_argument('--separator-device', choices=['auto', 'cuda', 'cpu'], help='Device for vocal separation; "auto" uses GPU if available')
    result.add_argument('--separator-model', default='htdemucs', help='Demucs model for vocal separation; see https://github.com/htdemucs/demucs')
    result.add_argument('--chunk-seconds', type=float, default=30, help='Maximum seconds of audio to process at once; shorter chunks reduce memory usage but may reduce accuracy')
    result.add_argument('--overlap-seconds', type=float, default=4, help='Seconds of overlap between audio chunks')
    result.add_argument('--ollama-model', default='qwen3.5:4b', help='Ollama model for title suggestion; see https://ollama.com/models/qwen3.5')
    result.add_argument('--ollama-host', default='http://localhost:11434', help='Ollama host URL')
    result.add_argument('--connect-timeout', type=float, default=10, help='Seconds to wait for a connection to Ollama')
    result.add_argument('--inference-timeout', type=float, default=300, help='Seconds to wait for Ollama to return a title suggestion')
    result.add_argument('--worker-timeout', type=float, default=3600, help='Seconds allowed for each separation or transcription worker')
    result.add_argument('--log-level', choices=['info', 'debug'], default='info',
                        help='Output detail; debug shows model, device, and per-chunk progress')
    result.add_argument('--output-dir', type=Path, default=Path('.song-title-cache'), help='Vocal/transcript cache and JSON/TXT reports')
    result.add_argument('--refresh', action='store_true', help='Regenerate vocals and transcript')
    result.add_argument('--dry-run', action='store_true', help='Analyze and report without writing lyrics, tags, or filenames')
    result.add_argument('--backup', action='store_true', help='Save the original audio as .bak before changing its metadata')
    result.add_argument('--force-lyrics', action='store_true', help='Overwrite existing embedded lyrics and, with --lyrics-file, the lyrics sidecar')
    result.add_argument('--lyrics-file', action='store_true', help='Also write generated lyrics beside the audio as .lyrics.txt')
    result.add_argument('--timed-lyrics-file', action='store_true', help='Write aligned lyrics beside the audio as .lyrics.lrc')
    result.add_argument('--ensure-timed-lyrics', action='store_true', help='Generate timed lyrics only when a matching timed-lyrics cache entry is missing')
    result.add_argument('--auto', action='store_true', help='Use the first title suggestion, write tags, and rename without prompting')
    template_help = DEFAULT_FILENAME_TEMPLATE.replace('%', '%%')
    result.add_argument('--filename-template', help=f'MusicBrainz Picard-style rename template; default: {template_help}')
    result.add_argument('--max-filename-length', type=int, default=DEFAULT_MAX_FILENAME_LENGTH,
                        help=f'Maximum filename length before Ollama shortens template metadata parts (default: {DEFAULT_MAX_FILENAME_LENGTH})')
    result.add_argument('--color', action=argparse.BooleanOptionalAction, default=None, help='Force ANSI colors on or off (default: detect terminal support)')
    return result


def check_prerequisites(settings):
    for executable in ('ffmpeg', 'ffprobe'):
        if not shutil.which(executable):
            raise RuntimeError(f'{executable} is required on PATH')
    for name in ('demucs', 'torch', 'transformers', 'soundfile', 'numpy', 'librosa', 'mutagen', 'requests', 'pydantic'):
        try:
            version = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError as exc:
            raise RuntimeError(f'{name} missing. Install: python -m pip install -r song_title/requirements.txt') from exc
        if name == 'transformers' and tuple(int(x) for x in version.split('.')[:2]) < (5, 13):
            raise RuntimeError('Native Qwen/Nemotron ASR requires transformers>=5.13. Install song_title/requirements.txt')


def write_title(path, title, expected_digest, *, make_backup=False, lyrics=None):
    return update_metadata(path, expected_digest, title=title, lyrics=lyrics, make_backup=make_backup)


def _review(analysis):
    print(f'{_c("Current title:", Colors.BOLD)} {analysis.metadata.get("title", "(missing)")}')
    for note in analysis.notes:
        print(f'{_c("Note:", Colors.YELLOW)} {note}')
    for number, candidate in enumerate(analysis.candidates, 1):
        print(f'{_c(f"{number}.", Colors.CYAN)} {_c(candidate.title, Colors.GREEN + Colors.BOLD)}\n   {candidate.rationale}\n   {_c("Evidence:", Colors.DIM)} ' + ' / '.join(candidate.evidence))
    if analysis.report_path:
        print(f'{_c("Report:", Colors.DIM)} {analysis.report_path}')


def _progress(message):
    lower = message.casefold()
    color = Colors.CYAN if any(word in lower for word in ('separat', 'transcrib')) else Colors.MAGENTA
    print(_c(message, color), flush=True)


def _input_display_base(source: Path, input_paths) -> Path:
    source = source.resolve()
    matches = []
    for value in input_paths:
        root = Path(value).resolve()
        if root.is_dir() and source.is_relative_to(root):
            matches.append((len(root.parts), root))
        elif source == root:
            matches.append((len(root.parts), root.parent))
    return max(matches, key=lambda match: match[0])[1] if matches else source.parent


def _display_path(path: Path, base: Path) -> str:
    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(base))
    except ValueError:
        return resolved.name


def _choose(analysis, previous_titles=()):
    while True:
        answer = input(_c('Choose a number, [e] enter a title, [s] skip, [q] quit: ', Colors.CYAN)).strip().lower()
        if answer in {'', 's', 'q'}:
            return None, answer == 'q'
        if answer == 'e':
            title = input(_c('Title: ', Colors.CYAN)).strip()
            if title and len(title) <= 250 and not any(ord(c) < 32 for c in title):
                duplicate = similar_title(title, list(previous_titles))
                if duplicate:
                    print(_c(f'Title is too similar to a previously selected title: {duplicate}', Colors.YELLOW))
                else:
                    return title, False
            else:
                print(_c('Enter a title of 1–250 characters without control characters.', Colors.YELLOW))
        elif answer.isdigit() and 1 <= int(answer) <= len(analysis.candidates):
            return analysis.candidates[int(answer) - 1].title, False
        else:
            print(_c('Choose an available suggestion, enter a title, skip, or quit.', Colors.YELLOW))


def _rename_no_clobber(source: Path, destination: Path) -> Path:
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if os.path.normcase(str(source)) == os.path.normcase(str(destination)):
        return source
    if destination.exists():
        raise FileExistsError(f'Rename target already exists: {destination}')
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(source, destination)
        source.unlink()
    except FileExistsError:
        raise FileExistsError(f'Rename target already exists: {destination}')
    except OSError:
        if destination.exists():
            raise FileExistsError(f'Rename target already exists: {destination}')
        os.rename(source, destination)
    return destination


def _store_lyrics_file(path: Path, analysis, arguments) -> Path | None:
    lyrics = analysis.formatted_lyrics.strip() or analysis.lyrics.strip()
    if not lyrics:
        return None
    result = write_lyrics_file(path, lyrics, overwrite=arguments.force_lyrics)
    if result is None:
        analysis.notes.append(f'Existing lyrics file kept: {path.with_name(path.stem + ".lyrics.txt")}')
    else:
        print(f'{_c("Lyrics file:", Colors.GREEN)} {result}')
    return result


def _store_timed_lyrics_file(path: Path, analysis, arguments) -> Path | None:
    lyrics = analysis.timed_lyrics_formats.get('lrc', '')
    if not lyrics:
        if arguments.timed_lyrics_file:
            analysis.notes.append('Timed lyrics sidecar was requested, but no matching word timestamps are available.')
        return None
    result = write_timed_lyrics_file(path, lyrics, overwrite=arguments.force_lyrics)
    if result is None:
        analysis.notes.append(f'Existing timed lyrics file kept: {path.with_name(path.stem + ".lyrics.lrc")}')
    else:
        print(f'{_c("Timed lyrics file:", Colors.GREEN)} {result}')
    return result


def _tag_lyrics(analysis, path: Path, arguments) -> str | None:
    lyrics = analysis.formatted_lyrics.strip() or analysis.lyrics.strip()
    if not lyrics:
        return None
    existing = str(analysis.metadata.get('lyrics', '')).strip()
    if existing and not arguments.force_lyrics:
        analysis.notes.append('Existing embedded lyrics kept; use --force-lyrics to replace them.')
        return None
    if not can_write(path):
        extra = '; use --lyrics-file to export a .lyrics.txt sidecar' if not arguments.lyrics_file else '; lyrics can be exported to a .lyrics.txt sidecar'
        analysis.notes.append(f'Embedded lyrics are unsupported for {path.suffix}{extra}.')
        return None
    return lyrics


def main(argv=None):
    global _USE_COLOR
    arguments = parser().parse_args(argv)
    if arguments.max_filename_length < 1:
        parser().error('--max-filename-length must be a positive integer')
    force_color = arguments.color
    if force_color is None and 'NO_COLOR' in os.environ:
        force_color = False
    _USE_COLOR = Colors.should_use(force=force_color)
    try:
        settings = Settings(asr=arguments.asr, asr_model=arguments.asr_model, device=arguments.device, separator_device=arguments.separator_device,
                            separator_model=arguments.separator_model, chunk_seconds=arguments.chunk_seconds, overlap_seconds=arguments.overlap_seconds,
                            ollama_model=arguments.ollama_model, ollama_host=arguments.ollama_host, connect_timeout=arguments.connect_timeout,
                            inference_timeout=arguments.inference_timeout, worker_timeout=arguments.worker_timeout, cache_dir=arguments.output_dir,
                            refresh=arguments.refresh, log_level=arguments.log_level)
        inputs = discover_inputs(arguments.inputs, arguments.recursive, [settings.cache_dir])
        if not inputs:
            raise ValueError('No supported audio files found')
        display_bases = {source: _input_display_base(source, arguments.inputs) for source in inputs}
        check_prerequisites(settings)
        selected_titles_path = settings.cache_dir / 'selected-titles.json'
        selected_titles = load_title_history(selected_titles_path)
    except (ValueError, RuntimeError) as exc:
        print(_c(f'Error: {exc}', Colors.RED), file=sys.stderr)
        return 2

    counts = {'analyzed': 0, 'saved': 0, 'lyrics_tags': 0, 'lyrics_files': 0, 'renamed': 0, 'skipped': 0, 'failed': 0}
    try:
        with ModelRuntime(settings) as runtime:
            for number, source in enumerate(inputs, 1):
                display_base = display_bases[source]
                display_source = _display_path(source, display_base)
                print(f'\n{_c(f"[{number}/{len(inputs)}]", Colors.CYAN + Colors.BOLD)} {display_source}', flush=True)
                analysis = None
                try:
                    analysis = analyze_file(source, settings, progress=_progress, runtime=runtime,
                                            force_lyrics=arguments.force_lyrics,
                                            ensure_timed_lyrics=arguments.ensure_timed_lyrics or arguments.timed_lyrics_file)
                    counts['analyzed'] += 1
                    duplicate_titles = []
                    unique_candidates = []
                    for candidate in analysis.candidates:
                        duplicate = similar_title(candidate.title, selected_titles)
                        if duplicate:
                            duplicate_titles.append(candidate.title)
                        else:
                            unique_candidates.append(candidate)
                    if duplicate_titles:
                        analysis.candidates = unique_candidates
                        analysis.notes.append(f'Filtered {len(duplicate_titles)} title suggestion(s) too similar to previously selected titles.')
                    _review(analysis)
                    if arguments.dry_run:
                        continue

                    metadata_title = str(analysis.metadata.get('title') or '').strip()
                    existing_title = metadata_title if has_meaningful_title(metadata_title) else ''
                    chosen_title = None
                    quit_requested = False
                    title_confirmed = False
                    if existing_title:
                        print(f'{_c("Keeping metadata title:", Colors.DIM)} {existing_title}')
                    elif arguments.auto:
                        if analysis.candidates and can_write(source):
                            chosen_title = analysis.candidates[0].title
                            analysis.selected_title = chosen_title
                            title_confirmed = True
                        else:
                            analysis.notes.append('Auto mode found no writable title suggestion; title and filename were left unchanged.')
                    elif sys.stdin.isatty():
                        chosen_title, quit_requested = _choose(analysis, selected_titles)
                        analysis.selected_title = chosen_title
                        if chosen_title and not can_write(source):
                            analysis.notes.append(f'Title writing is unsupported for {source.suffix}; suggestion retained in report.')
                            chosen_title = None
                        if chosen_title:
                            print(f'{_c("Title change:", Colors.YELLOW)} {analysis.metadata.get("title", "(missing)")} → {_c(chosen_title, Colors.GREEN)}')
                            title_confirmed = input(_c('Save this title? [y/N]: ', Colors.CYAN)).strip().lower() in {'y', 'yes'}
                    else:
                        analysis.notes.append('Noninteractive input: title selection requires --auto or an interactive terminal.')

                    title_to_write = chosen_title if title_confirmed else None
                    title_for_rename = title_to_write or existing_title
                    lyrics_to_write = _tag_lyrics(analysis, source, arguments)
                    timed_lyrics_to_write = analysis.timed_lyrics if source.suffix.lower() == '.mp3' else None
                    template = arguments.filename_template or DEFAULT_FILENAME_TEMPLATE
                    destination = source
                    if title_for_rename and template:
                        naming_metadata = dict(analysis.metadata, title=title_for_rename)
                        try:
                            destination = output_path(source, naming_metadata, template, settings=settings,
                                                      runtime=runtime, max_length=arguments.max_filename_length)
                            if os.path.normcase(str(destination.resolve())) != os.path.normcase(str(source.resolve())) and destination.exists():
                                raise FileExistsError(f'Rename target already exists: {destination}')
                        except FilenameTooLongError as exc:
                            analysis.notes.append(f'Rename skipped: {exc}')
                            print(_c(f'Rename skipped: {exc}', Colors.YELLOW))

                    backup = None
                    if can_write(source) and (title_to_write or lyrics_to_write or timed_lyrics_to_write):
                        backup = update_metadata(source, analysis.digest, title=title_to_write,
                                                 lyrics=lyrics_to_write,
                                                 synchronized_lyrics=timed_lyrics_to_write,
                                                 make_backup=arguments.backup)
                        if lyrics_to_write:
                            analysis.metadata['lyrics'] = lyrics_to_write
                            counts['lyrics_tags'] += 1
                            print(_c('Embedded lyrics updated.', Colors.GREEN))
                        if title_to_write or lyrics_to_write or timed_lyrics_to_write:
                            refresh_timed_cache_source(analysis, source, settings)
                        if title_to_write:
                            analysis.metadata['title'] = title_to_write
                            counts['saved'] += 1
                            print(f'{_c("Saved title:", Colors.GREEN)} {title_to_write}')
                            if not similar_title(title_to_write, selected_titles):
                                selected_titles.append(title_to_write)
                                try:
                                    save_title_history(selected_titles_path, selected_titles)
                                except OSError as exc:
                                    analysis.notes.append(f'Could not save selected-title cache: {exc}')
                                    print(_c(f'Could not save selected-title cache: {exc}', Colors.YELLOW))
                        if backup:
                            print(f'{_c("Backup:", Colors.DIM)} {backup}')
                    elif title_to_write:
                        analysis.notes.append('Title was not written because this audio format is unsupported.')
                        counts['skipped'] += 1

                    if title_for_rename and destination != source:
                        destination = _rename_no_clobber(source, destination)
                        refresh_timed_cache_source(analysis, destination, settings)
                        analysis.metadata['output_path'] = str(destination)
                        counts['renamed'] += 1
                        print(f'{_c("Renamed:", Colors.GREEN)} {_display_path(destination, display_base)}')

                    lyrics_file = _store_lyrics_file(destination, analysis, arguments) if arguments.lyrics_file else None
                    if lyrics_file:
                        counts['lyrics_files'] += 1
                    timed_lyrics_file = (_store_timed_lyrics_file(destination, analysis, arguments)
                                         if arguments.timed_lyrics_file else None)
                    if not title_to_write and destination == source:
                        counts['skipped'] += 1
                    outcome = ('saved' if title_to_write else
                               'lyrics-saved' if lyrics_to_write or lyrics_file or timed_lyrics_file else
                               'renamed' if destination != source else 'skipped')
                    if quit_requested:
                        outcome = 'quit'
                    save_report(analysis, settings, outcome, str(backup) if backup else None)
                    if quit_requested:
                        break
                except EOFError:
                    counts['skipped'] += 1
                    if analysis is not None:
                        try:
                            lyrics_to_write = _tag_lyrics(analysis, source, arguments)
                            timed_lyrics_to_write = analysis.timed_lyrics if source.suffix.lower() == '.mp3' else None
                            backup = update_metadata(source, analysis.digest, lyrics=lyrics_to_write,
                                                     synchronized_lyrics=timed_lyrics_to_write,
                                                     make_backup=arguments.backup) if lyrics_to_write or timed_lyrics_to_write else None
                            if lyrics_to_write:
                                analysis.metadata['lyrics'] = lyrics_to_write
                                counts['lyrics_tags'] += 1
                            if backup or lyrics_to_write or timed_lyrics_to_write:
                                refresh_timed_cache_source(analysis, source, settings)
                            if arguments.lyrics_file:
                                _store_lyrics_file(source, analysis, arguments)
                            if arguments.timed_lyrics_file:
                                _store_timed_lyrics_file(source, analysis, arguments)
                            save_report(analysis, settings, 'input-ended', str(backup) if backup else None)
                        except Exception as exc:
                            analysis.notes.append(str(exc))
                            save_report(analysis, settings, 'failed')
                    print(_c('Input ended; title was not saved.', Colors.YELLOW))
                    break
                except Exception as exc:
                    counts['failed'] += 1
                    if analysis is not None:
                        analysis.notes.append(str(exc))
                        try:
                            save_report(analysis, settings, 'failed')
                        except OSError:
                            pass
                    print(_c(f'Failed: {exc}', Colors.RED), file=sys.stderr)
    except KeyboardInterrupt:
        print(_c('\nStopped; no further files processed.', Colors.YELLOW))
        return 130
    color_for = {'analyzed': Colors.CYAN, 'saved': Colors.GREEN, 'lyrics_tags': Colors.GREEN,
                 'lyrics_files': Colors.GREEN, 'renamed': Colors.GREEN, 'skipped': Colors.YELLOW, 'failed': Colors.RED}
    summary = ', '.join(_c(f'{name}: {count}', color_for[name]) for name, count in counts.items())
    print('\n' + summary)
    return 1 if counts['failed'] else 0

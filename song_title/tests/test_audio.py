from pathlib import Path

import pytest


def test_chunk_coverage_and_overlap():
    from song_title.audio import chunk_ranges
    assert chunk_ranges(65, 30, 4) == [(0, 30), (26, 56), (52, 65)]
    assert chunk_ranges(0, 30, 4) == []
    assert chunk_ranges(5, 30, 4) == [(0, 5)]


@pytest.mark.parametrize('seconds,overlap', [(0, 0), (30, 30), (30, -1)])
def test_invalid_segments_rejected(seconds, overlap):
    from song_title.audio import chunk_ranges
    with pytest.raises(ValueError):
        chunk_ranges(60, seconds, overlap)


def test_boundary_merge_preserves_later_chorus():
    from song_title.audio import assemble_transcript
    from song_title.types import Chunk
    chunks = [Chunk(0, 30, 'We wait after the last train'),
              Chunk(26, 56, 'after the last train until dawn'),
              Chunk(52, 80, 'We wait after the last train')]
    assert assemble_transcript(chunks) == 'We wait after the last train\nafter the last train until dawn\nWe wait after the last train'


def test_adjacent_identical_choruses_are_preserved_without_word_timestamps():
    from song_title.audio import assemble_transcript
    from song_title.types import Chunk
    phrase = 'We will find our way'
    assert assemble_transcript([Chunk(0,30,phrase),Chunk(26,56,phrase)]) == phrase + '\n' + phrase


def test_short_repeated_words_are_not_removed():
    from song_title.audio import assemble_transcript
    from song_title.types import Chunk
    assert assemble_transcript([Chunk(0, 30, 'go go'), Chunk(26, 50, 'go go')]) == 'go go\ngo go'


def test_nonoverlapping_chunks_keep_repeated_phrase():
    from song_title.audio import assemble_transcript
    from song_title.types import Chunk
    assert assemble_transcript([Chunk(0, 5, 'after the last train'), Chunk(5, 10, 'after the last train')]).count('after the last train') == 2


def test_discovery_deduplicates_and_excludes_outputs(tmp_path):
    from song_title.audio import discover_inputs
    song = tmp_path / 'song $ [1].mp3'
    song.write_bytes(b'x')
    cache = tmp_path / 'cache'
    cache.mkdir()
    (cache / 'vocals.wav').write_bytes(b'x')
    nested = tmp_path / 'album'
    nested.mkdir()
    (nested / 'second.FLAC').write_bytes(b'x')
    result = discover_inputs([tmp_path, song], True, [cache])
    assert len(result) == 2 and set(result) == {song.resolve(), (nested / 'second.FLAC').resolve()}


def test_invalid_configuration_rejected():
    from song_title.types import Settings
    with pytest.raises(ValueError):
        Settings(chunk_seconds=4, overlap_seconds=4)

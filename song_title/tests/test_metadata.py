import subprocess

import pytest


@pytest.fixture
def flac(tmp_path):
    import numpy as np
    import soundfile as sf
    from mutagen.flac import FLAC
    path = tmp_path / 'song.flac'
    sf.write(path, np.sin(np.arange(8000) / 10) * .05, 8000)
    tags = FLAC(path)
    tags['title'] = 'Untitled'
    tags['artist'] = 'My Artist'
    tags['comment'] = 'Night train demo'
    tags.save()
    return path


def test_title_transaction_preserves_audio_and_other_tags(flac):
    from song_title.metadata import fingerprint, read_metadata, write_title
    import soundfile as sf
    original = flac.read_bytes()
    audio, sr = sf.read(flac)
    backup = write_title(flac, 'After the Last Train', fingerprint(flac))
    assert backup.read_bytes() == original
    assert read_metadata(flac)['title'] == 'After the Last Train'
    assert read_metadata(flac)['artist'] == 'My Artist'
    after, new_sr = sf.read(flac)
    assert sr == new_sr and (audio == after).all()
    second = write_title(flac, 'Dawn', fingerprint(flac))
    assert second != backup and backup.read_bytes() == original


def test_changed_source_is_not_overwritten(flac):
    from song_title.metadata import fingerprint, write_title
    digest = fingerprint(flac)
    flac.write_bytes(flac.read_bytes() + b'changed')
    changed = flac.read_bytes()
    with pytest.raises(ValueError, match='changed'):
        write_title(flac, 'New title', digest)
    assert flac.read_bytes() == changed


def test_failed_tag_verification_keeps_original(flac, monkeypatch):
    from song_title import metadata
    original = flac.read_bytes()
    monkeypatch.setattr(metadata, '_verify_copy', lambda *args: (_ for _ in ()).throw(ValueError('verify failed')))
    with pytest.raises(ValueError):
        metadata.write_title(flac, 'New title', metadata.fingerprint(flac))
    assert flac.read_bytes() == original


def test_mp3_retains_id3_version_and_artwork(tmp_path):
    from mutagen.id3 import ID3, TIT2, TPE1, APIC
    from song_title.metadata import fingerprint, write_title
    path = tmp_path / 'tags.mp3'
    tags = ID3()
    tags.add(TIT2(encoding=3, text=['Old']))
    tags.add(TPE1(encoding=3, text=['Artist']))
    tags.add(APIC(encoding=3, mime='image/png', type=3, desc='cover', data=b'picture'))
    tags.save(path, v2_version=3)
    write_title(path, 'New', fingerprint(path))
    result = ID3(path)
    assert result.version[1] == 3
    assert str(result['TIT2']) == 'New'
    assert result.getall('APIC')[0].data == b'picture'


@pytest.mark.parametrize('ext,codec', [('m4a','aac'), ('ogg','libvorbis'), ('opus','libopus'), ('mp3','libmp3lame')])
def test_real_encoded_formats_title_only(tmp_path, ext, codec):
    import shutil
    if not shutil.which('ffmpeg'):
        pytest.skip('FFmpeg not installed')
    from song_title.metadata import fingerprint, read_metadata, write_title
    path = tmp_path / ('song.' + ext)
    subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','sine=frequency=440:duration=0.1','-c:a',codec,'-metadata','artist=Artist','-metadata','title=Old',str(path)], check=True)
    def pcm():
        return subprocess.run(['ffmpeg','-v','error','-i',str(path),'-f','s16le','-'], check=True, capture_output=True).stdout
    before = pcm()
    write_title(path, 'New', fingerprint(path))
    assert read_metadata(path)['title'] == 'New'
    assert read_metadata(path)['artist'] == 'Artist'
    assert pcm() == before

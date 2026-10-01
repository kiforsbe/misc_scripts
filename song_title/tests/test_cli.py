from pathlib import Path


def make_analysis(path):
    from song_title.types import Analysis, Candidate
    return Analysis(path, 'digest', {'title':'Old'}, [], 'last train', [Candidate('Last Train','Chorus',['last train'])])


def test_dry_run_never_prompts_or_writes(tmp_path, monkeypatch):
    from song_title import cli
    source = tmp_path/'song.mp3'
    source.write_bytes(b'mix')
    monkeypatch.setattr(cli,'analyze_file',lambda path,*a,**k:make_analysis(path))
    monkeypatch.setattr(cli,'check_prerequisites',lambda settings:None)
    monkeypatch.setattr('builtins.input',lambda *a: (_ for _ in ()).throw(AssertionError('prompt in dry run')))
    monkeypatch.setattr(cli,'write_title',lambda *a: (_ for _ in ()).throw(AssertionError('write in dry run')))
    assert cli.main([str(source),'--dry-run']) == 0


def test_eof_cannot_save(tmp_path, monkeypatch):
    from song_title import cli
    source = tmp_path/'song.mp3'
    source.write_bytes(b'mix')
    monkeypatch.setattr(cli,'analyze_file',lambda path,*a,**k:make_analysis(path))
    monkeypatch.setattr(cli,'check_prerequisites',lambda settings:None)
    monkeypatch.setattr(cli.sys.stdin,'isatty',lambda:True)
    monkeypatch.setattr('builtins.input',lambda *a: (_ for _ in ()).throw(EOFError()))
    monkeypatch.setattr(cli,'write_title',lambda *a: (_ for _ in ()).throw(AssertionError('write after EOF')))
    assert cli.main([str(source)]) == 0


def test_manual_title_requires_confirmation(tmp_path, monkeypatch):
    from song_title import cli
    source = tmp_path/'song.mp3'
    source.write_bytes(b'mix')
    monkeypatch.setattr(cli,'analyze_file',lambda path,*a,**k:make_analysis(path))
    monkeypatch.setattr(cli,'check_prerequisites',lambda settings:None)
    monkeypatch.setattr(cli.sys.stdin,'isatty',lambda:True)
    answers = iter(['e','My Own Title','yes'])
    monkeypatch.setattr('builtins.input',lambda *a:next(answers))
    saved = []
    def write(path,title,digest):
        saved.append((path,title,digest))
        return path.with_suffix('.bak')
    monkeypatch.setattr(cli,'write_title',write)
    assert cli.main([str(source)]) == 0
    assert saved == [(source.resolve(),'My Own Title','digest')]


def test_noninteractive_input_never_prompts_or_writes(tmp_path, monkeypatch):
    import io
    from song_title import cli
    source = tmp_path/'song.mp3'
    source.write_bytes(b'mix')
    monkeypatch.setattr(cli,'analyze_file',lambda path,*a,**k:make_analysis(path))
    monkeypatch.setattr(cli,'check_prerequisites',lambda settings:None)
    monkeypatch.setattr(cli.sys,'stdin',io.StringIO('e\nPiped Title\nyes\n'))
    monkeypatch.setattr(cli,'write_title',lambda *a: (_ for _ in ()).throw(AssertionError('noninteractive write')))
    assert cli.main([str(source)]) == 0
    assert cli.sys.stdin.tell() == 0


def test_manual_selection_is_recorded_even_for_unsupported_format(tmp_path, monkeypatch):
    import json
    from song_title import cli
    source = tmp_path/'song.wav'
    source.write_bytes(b'mix')
    analysis = make_analysis(source)
    analysis.report_path = tmp_path/'report.json'
    monkeypatch.setattr(cli,'analyze_file',lambda *a:analysis)
    monkeypatch.setattr(cli,'check_prerequisites',lambda settings:None)
    monkeypatch.setattr(cli.sys.stdin,'isatty',lambda:True)
    answers = iter(['e','My Own Title'])
    monkeypatch.setattr('builtins.input',lambda *a:next(answers))
    assert cli.main([str(source)]) == 0
    assert json.loads(analysis.report_path.read_text())['selected_title'] == 'My Own Title'


def test_batch_continues_after_failed_file(tmp_path, monkeypatch):
    from song_title import cli
    paths = [tmp_path/'a.mp3',tmp_path/'b.mp3']
    for path in paths:
        path.write_bytes(b'mix')
    processed = []
    def analyze(path,*a,**k):
        processed.append(path.name)
        if path.name == 'a.mp3':
            raise RuntimeError('bad file')
        return make_analysis(path)
    monkeypatch.setattr(cli,'analyze_file',analyze)
    monkeypatch.setattr(cli,'check_prerequisites',lambda settings:None)
    assert cli.main([str(p) for p in paths] + ['--dry-run']) == 1
    assert processed == ['a.mp3','b.mp3']


def test_cli_and_settings_share_the_default_backend():
    from song_title.cli import parser
    from song_title.types import Settings
    assert Settings().asr == parser().parse_args(['song.wav']).asr


def test_failed_save_is_recorded_with_selected_title(tmp_path,monkeypatch):
    import json
    from song_title import cli
    source = tmp_path/'song.mp3'
    source.write_bytes(b'mix')
    analysis = make_analysis(source)
    analysis.report_path = tmp_path/'report.json'
    monkeypatch.setattr(cli,'analyze_file',lambda *a:analysis)
    monkeypatch.setattr(cli,'check_prerequisites',lambda settings:None)
    monkeypatch.setattr(cli.sys.stdin,'isatty',lambda:True)
    answers = iter(['1','yes'])
    monkeypatch.setattr('builtins.input',lambda *a:next(answers))
    monkeypatch.setattr(cli,'write_title',lambda *a: (_ for _ in ()).throw(ValueError('Source changed')))
    assert cli.main([str(source)]) == 1
    report = json.loads(analysis.report_path.read_text())
    assert report['outcome'] == 'failed' and report['selected_title'] == 'Last Train'
    assert 'Source changed' in report['notes']

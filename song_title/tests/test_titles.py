import json

import pytest


def test_evidence_must_exist_and_titles_are_unique():
    from song_title.titles import validate_candidates
    data = {'candidates': [
        {'title':'Last Train','rationale':'Chorus','evidence':['after the last train']},
        {'title':'last train','rationale':'Duplicate','evidence':['after the last train']},
        {'title':'Invented','rationale':'Wrong','evidence':['ocean paradise']}]}
    result = validate_candidates(data, 'We wait after the last train until dawn')
    assert [c.title for c in result] == ['Last Train']


def test_ollama_payload_and_response_validation(monkeypatch):
    from song_title.titles import suggest_titles
    from song_title.types import Settings
    import requests
    def post(url, **kwargs):
        assert url == 'http://localhost:11434/api/chat'
        body = kwargs['json']
        assert body['model'] == 'qwen3.5:4b'
        assert body['keep_alive'] == 0 and body['stream'] is False
        assert body['format']['type'] == 'object'
        assert body['options']['num_ctx'] == 8192
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({'message': {'content': json.dumps({'candidates':[{'title':'Last Train','rationale':'Chorus phrase','evidence':['last train']}]})}}).encode()
        return response
    monkeypatch.setattr(requests, 'post', post)
    result = suggest_titles('We wait for the last train', {'genre':'folk'}, Settings())
    assert result[0].title == 'Last Train'


def test_empty_lyrics_returns_no_candidates_without_http(monkeypatch):
    from song_title.titles import suggest_titles
    from song_title.types import Settings
    monkeypatch.setattr('requests.post', lambda *a, **k: pytest.fail('HTTP called for empty lyrics'))
    assert suggest_titles('', {}, Settings()) == []


def test_context_reduction_is_bounded_and_preserves_recurring_phrase():
    from song_title.titles import prepare_lyrics
    lyrics = ('after the last train\n' + 'a different verse\n' * 20) * 1000
    text, reduced = prepare_lyrics(lyrics, 1000)
    assert reduced and len(text) <= 1000 and 'after the last train' in text


def test_missing_ollama_model_is_actionable(monkeypatch):
    import requests
    from song_title.titles import suggest_titles
    from song_title.types import Settings
    response = requests.Response()
    response.status_code = 404
    response._content = b'{"error":"model not found"}'
    monkeypatch.setattr(requests, 'post', lambda *a, **k: response)
    with pytest.raises(RuntimeError, match='ollama pull qwen3.5:4b'):
        suggest_titles('last train', {}, Settings())

"""Run-scoped clients for resident inference workers and Ollama."""
from __future__ import annotations

import json
import os
import queue
import signal
import subprocess
import sys
import threading
from collections import deque
from dataclasses import asdict
from pathlib import Path

import requests

from .types import Chunk, Settings


class _WorkerJobError(RuntimeError):
    """A reported operation failure that left the JSON-lines worker healthy."""


class _ResidentWorker:
    """One lazily started worker with a single in-flight JSON-lines request."""

    def __init__(self, phase: str, settings: Settings):
        self.phase = phase
        self.settings = settings
        self.process: subprocess.Popen | None = None
        self.responses: queue.Queue = queue.Queue()
        self.stderr_tail: deque[str] = deque(maxlen=80)
        self.reader_threads: list[threading.Thread] = []

    def _start(self):
        environment = os.environ.copy()
        environment['PYTHONIOENCODING'] = 'utf-8'
        package_root = str(Path(__file__).resolve().parent.parent)
        environment['PYTHONPATH'] = package_root + os.pathsep + environment.get('PYTHONPATH', '')
        self.responses = queue.Queue()
        self.stderr_tail = deque(maxlen=80)
        process = subprocess.Popen(
            [sys.executable, '-m', 'song_title.worker', self.phase],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding='utf-8', errors='replace', bufsize=1, env=environment,
            start_new_session=(os.name != 'nt'),
            creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0),
        )
        self.process = process
        responses = self.responses
        stderr_tail = self.stderr_tail

        def read_responses():
            assert process.stdout is not None
            try:
                for line in process.stdout:
                    responses.put(line)
            except (OSError, ValueError):
                pass
            finally:
                responses.put(None)

        def read_errors():
            assert process.stderr is not None
            try:
                for line in process.stderr:
                    stderr_tail.append(line.rstrip())
                    print(line, end='', file=sys.stderr, flush=True)
            except (OSError, ValueError):
                pass

        self.reader_threads = [
            threading.Thread(target=read_responses, name=f'song-title-{self.phase}-stdout', daemon=True),
            threading.Thread(target=read_errors, name=f'song-title-{self.phase}-stderr', daemon=True),
        ]
        for thread in self.reader_threads:
            thread.start()

    @staticmethod
    def _terminate_tree(process):
        if process.poll() is not None:
            return
        if os.name == 'nt':
            try:
                subprocess.run(
                    ['taskkill', '/PID', str(process.pid), '/T', '/F'],
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    check=False, timeout=10,
                )
            except (OSError, subprocess.TimeoutExpired):
                pass
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except OSError:
                pass
        if process.poll() is None:
            try:
                process.terminate()
            except OSError:
                pass

    def _stop(self, *, force=False):
        process, self.process = self.process, None
        if process is None:
            return
        if process.stdin is not None:
            try:
                process.stdin.close()
            except (OSError, ValueError):
                pass
        if process.poll() is None:
            if force:
                self._terminate_tree(process)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._terminate_tree(process)
                process.wait()
        for thread in self.reader_threads:
            thread.join(timeout=2)
        for stream, thread in zip((process.stdout, process.stderr), self.reader_threads):
            if stream is not None and not thread.is_alive():
                stream.close()
        self.reader_threads = []

    def request(self, request: dict) -> dict:
        if self.process is None or self.process.poll() is not None:
            self._stop(force=True)
            self._start()
        process = self.process
        try:
            assert process is not None and process.stdin is not None
            process.stdin.write(json.dumps(request, ensure_ascii=False, default=str) + '\n')
            process.stdin.flush()
            line = self.responses.get(timeout=self.settings.worker_timeout)
            if line is None:
                detail = '\n'.join(self.stderr_tail) or 'worker exited without a response'
                raise RuntimeError(detail)
            response = json.loads(line)
            if not isinstance(response, dict) or not isinstance(response.get('ok'), bool):
                raise ValueError('worker returned an invalid response object')
            if not response['ok']:
                raise _WorkerJobError(f'{self.phase} failed: {response.get("error", "worker operation failed")}')
            result = response.get('result')
            if not isinstance(result, dict):
                raise ValueError('worker returned an invalid result object')
            return result
        except _WorkerJobError:
            raise
        except (OSError, ValueError, queue.Empty, RuntimeError) as exc:
            detail = '\n'.join(self.stderr_tail)
            self._stop(force=True)
            if isinstance(exc, queue.Empty):
                reason = f'{self.phase} exceeded {self.settings.worker_timeout:g}s'
            else:
                reason = str(exc)
            if detail and detail not in reason:
                reason += f'\n{detail}'
            raise RuntimeError(f'{self.phase} worker failed: {reason}') from exc

    def close(self):
        self._stop()


class ModelRuntime:
    """Own resident local workers and the configured Ollama model for one run."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._workers: dict[str, _ResidentWorker] = {}
        self._ollama_used = False
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False

    def _worker(self, phase: str) -> _ResidentWorker:
        if self._closed:
            raise RuntimeError('model runtime is already closed')
        if phase not in self._workers:
            self._workers[phase] = _ResidentWorker(phase, self.settings)
        return self._workers[phase]

    @staticmethod
    def _show_runtime(phase: str, runtime: dict, log_level: str):
        if log_level != 'debug':
            return
        print(f'{phase}: device={runtime.get("device", "unknown")}, reduced segments={runtime.get("reduced_segments", False)}', flush=True)
        if 'peak_vram_bytes' in runtime:
            print(f'{phase}: peak PyTorch GPU allocation {runtime["peak_vram_bytes"] / 2**20:.0f} MiB', flush=True)

    def separate(self, source: Path, output: Path) -> Path:
        result = self._worker('separate').request({
            'source': str(Path(source).resolve()), 'output': str(Path(output).resolve()),
            'settings': asdict(self.settings),
        })
        runtime = result.pop('runtime', {})
        self._show_runtime('separate', runtime, self.settings.log_level)
        output = Path(output)
        output.with_suffix(output.suffix + '.runtime.json').write_text(json.dumps(runtime), encoding='utf-8')
        if not output.is_file():
            raise RuntimeError('Separator did not produce a vocal stem')
        return output

    def transcribe(self, vocals: Path) -> list[Chunk]:
        result = self._worker('asr').request({
            'source': str(Path(vocals).resolve()),
            'settings': asdict(self.settings),
        })
        runtime = result.pop('runtime', {})
        self._show_runtime('asr', runtime, self.settings.log_level)
        Path(vocals).parent.joinpath('transcript-worker.json.runtime.json').write_text(
            json.dumps(runtime), encoding='utf-8')
        return [Chunk(**item) for item in result['chunks']]

    def ollama_request(self, payload: dict) -> dict:
        if self._closed:
            raise RuntimeError('model runtime is already closed')
        self._ollama_used = True
        payload = dict(payload)
        payload['keep_alive'] = -1
        response = requests.post(
            self.settings.ollama_host.rstrip('/') + '/api/chat', json=payload,
            timeout=(self.settings.connect_timeout, self.settings.inference_timeout),
        )
        if response.status_code == 404:
            raise RuntimeError(f'Ollama model unavailable. Run: ollama pull {self.settings.ollama_model}')
        response.raise_for_status()
        return response.json()

    def _unload_ollama(self):
        if not self._ollama_used:
            return
        try:
            response = requests.post(
                self.settings.ollama_host.rstrip('/') + '/api/generate',
                json={'model': self.settings.ollama_model, 'prompt': '', 'keep_alive': 0},
                timeout=(self.settings.connect_timeout, self.settings.inference_timeout),
            )
            response.raise_for_status()
        except requests.RequestException:
            pass

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            for worker in self._workers.values():
                try:
                    worker.close()
                except Exception as exc:
                    print(f'Could not stop {worker.phase} worker cleanly: {exc}', file=sys.stderr)
        finally:
            self._unload_ollama()

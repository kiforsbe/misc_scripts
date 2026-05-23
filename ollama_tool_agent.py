from __future__ import annotations

import argparse
import binascii
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from smolagents import ToolCallingAgent, tool
from smolagents.monitoring import AgentLogger, LogLevel
from smolagents.models import ApiModel, ChatMessage, ChatMessageToolCall, ChatMessageToolCallFunction, TokenUsage


DEFAULT_MODEL_NAME = "gemma4:e2b"
DEFAULT_OLLAMA_HOST = "http://localhost:11434"
DEFAULT_WORKING_DIRECTORY = "."
DEFAULT_MAX_STEPS = 8
DEFAULT_MAX_TOKENS = 32 * 1024
DEFAULT_NUM_CTX = 128 * 1024
DEFAULT_REASONING_EFFORT = "high"
DEFAULT_VERBOSITY = "compact"
CRC32_DEFAULT_CHUNK_SIZE = 1024 * 1024
FILE_INFO_AVAILABLE_FIELDS = (
    "path",
    "path_from_base",
    "absolute_path",
    "name",
    "stem",
    "suffix",
    "parent",
    "bytes",
    "created",
    "modified",
    "accessed",
    "is_symlink",
    "is_hidden",
    "is_readonly",
)
FILE_INFO_REQUIRED_OUTPUT_FIELDS = ("name",)
AGENT_INSTRUCTIONS = """
Use the available tools directly and keep tool usage minimal.

Rules:
- Prefer list_files_by_size when the user wants files ordered by size.
- For file_info, use the exact argument name paths.
- When list_files returns relative paths for one folder, pass that folder as base_path and the returned path values as paths.
- Do not invent filenames, placeholder values, or example outputs.
""".strip()
CURRENT_WORKING_DIRECTORY = Path(DEFAULT_WORKING_DIRECTORY).resolve()


class CompactAgentLogger(AgentLogger):
    def log(self, *args: Any, level: int | str | LogLevel = LogLevel.INFO, **kwargs: Any) -> None:
        if self._should_suppress(args, level):
            return
        super().log(*args, level=level, **kwargs)

    def _should_suppress(self, args: tuple[Any, ...], level: int | str | LogLevel) -> bool:
        normalized_level = LogLevel[level.upper()] if isinstance(level, str) else level
        if normalized_level != LogLevel.INFO:
            return False
        return any(self._is_filtered_message(arg) for arg in args)

    def _is_filtered_message(self, value: Any) -> bool:
        plain_text = self._extract_plain_text(value)
        if plain_text:
            return plain_text.startswith("Final answer:") or plain_text.startswith("Calling tool: 'final_answer'")
        if isinstance(value, str):
            return value.startswith("Observations:")
        return False

    def _extract_plain_text(self, value: Any) -> str:
        plain_text = getattr(value, "plain", None)
        if isinstance(plain_text, str):
            return plain_text

        renderable = getattr(value, "renderable", None)
        if renderable is not None:
            renderable_plain = getattr(renderable, "plain", None)
            if isinstance(renderable_plain, str):
                return renderable_plain

        return ""


def parse_verbosity_level(raw_level: str) -> LogLevel:
    if raw_level == "compact":
        return LogLevel.INFO
    return LogLevel[raw_level.upper()]


def build_logger(verbosity: str) -> AgentLogger | None:
    if verbosity == "compact":
        return CompactAgentLogger(level=LogLevel.INFO)
    return None


def normalize_ollama_host(host: str) -> str:
    return host.rstrip("/")


@dataclass
class OllamaApiClient:
    host: str
    timeout: float

    def chat(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            url=f"{self.host}/api/chat",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            response_body = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Ollama API request failed with HTTP {error.code}: {response_body}") from error
        except urllib.error.URLError as error:
            raise RuntimeError(f"Unable to reach Ollama at {self.host}: {error.reason}") from error


class OllamaChatModel(ApiModel):
    def __init__(
        self,
        model_id: str,
        host: str,
        timeout: float = 300,
        custom_role_conversions: dict[str, str] | None = None,
        flatten_messages_as_text: bool = False,
        **kwargs: Any,
    ) -> None:
        self.host = normalize_ollama_host(host)
        self.timeout = timeout
        super().__init__(
            model_id=model_id,
            custom_role_conversions=custom_role_conversions,
            flatten_messages_as_text=flatten_messages_as_text,
            **kwargs,
        )

    def create_client(self) -> OllamaApiClient:
        return OllamaApiClient(host=self.host, timeout=self.timeout)

    def _stringify_content(self, content: Any) -> str:
        if content is None:
            return ""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            text_chunks: list[str] = []
            for part in content:
                if not isinstance(part, dict):
                    text_chunks.append(str(part))
                    continue
                if part.get("type") == "text":
                    text_chunks.append(str(part.get("text", "")))
                    continue
                if "content" in part:
                    text_chunks.append(str(part["content"]))
            return "\n".join(chunk for chunk in text_chunks if chunk)
        return str(content)

    def _convert_message(self, message: dict[str, Any]) -> dict[str, Any]:
        converted_message = {
            "role": str(message["role"]),
            "content": self._stringify_content(message.get("content")),
        }
        return converted_message

    def _build_payload(self, completion_kwargs: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": completion_kwargs.pop("model", self.model_id),
            "messages": [self._convert_message(message) for message in completion_kwargs.pop("messages")],
            "stream": False,
        }

        tools = completion_kwargs.pop("tools", None)
        if tools:
            payload["tools"] = tools

        response_format = completion_kwargs.pop("response_format", None)
        if response_format:
            if response_format.get("type") == "json":
                payload["format"] = "json"
            else:
                payload["format"] = response_format

        think = completion_kwargs.pop("reasoning_effort", None)
        if think is not None:
            payload["think"] = False if think == "none" else think

        explicit_think = completion_kwargs.pop("think", None)
        if explicit_think is not None:
            payload["think"] = explicit_think

        completion_kwargs.pop("tool_choice", None)

        options: dict[str, Any] = {}
        max_tokens = completion_kwargs.pop("max_tokens", None)
        if max_tokens is not None:
            options["num_predict"] = max_tokens

        stop_sequences = completion_kwargs.pop("stop", None)
        if stop_sequences is not None:
            options["stop"] = stop_sequences

        for option_name in ("temperature", "top_k", "top_p", "min_p", "repeat_penalty", "seed", "num_ctx"):
            option_value = completion_kwargs.pop(option_name, None)
            if option_value is not None:
                options[option_name] = option_value

        keep_alive = completion_kwargs.pop("keep_alive", None)
        if keep_alive is not None:
            payload["keep_alive"] = keep_alive

        if options:
            payload["options"] = options

        if completion_kwargs:
            payload.setdefault("options", {}).update(completion_kwargs)

        return payload

    def _parse_tool_calls(self, response_message: dict[str, Any]) -> list[ChatMessageToolCall] | None:
        raw_tool_calls = response_message.get("tool_calls") or []
        if not raw_tool_calls:
            return None

        tool_calls: list[ChatMessageToolCall] = []
        for index, tool_call in enumerate(raw_tool_calls, start=1):
            function = tool_call.get("function") or {}
            tool_calls.append(
                ChatMessageToolCall(
                    id=tool_call.get("id") or f"ollama-tool-call-{index}",
                    type=tool_call.get("type", "function"),
                    function=ChatMessageToolCallFunction(
                        name=function.get("name", ""),
                        arguments=function.get("arguments", {}),
                        description=function.get("description"),
                    ),
                )
            )
        return tool_calls

    def _synthesize_final_answer_tool_call(
        self,
        content: Any,
        tools_to_call_from: list[Any] | None,
    ) -> list[ChatMessageToolCall] | None:
        if not tools_to_call_from:
            return None

        answer_text = self._stringify_content(content).strip()
        if not answer_text:
            return None

        final_answer_tool = next((tool for tool in tools_to_call_from if getattr(tool, "name", "") == "final_answer"), None)
        if final_answer_tool is None:
            return None

        return [
            ChatMessageToolCall(
                id="ollama-final-answer-fallback",
                type="function",
                function=ChatMessageToolCallFunction(
                    name="final_answer",
                    arguments={"answer": answer_text},
                    description=getattr(final_answer_tool, "description", None),
                ),
            )
        ]

    def _parse_tool_call_from_text(self, role: str, content: Any) -> list[ChatMessageToolCall] | None:
        answer_text = self._stringify_content(content).strip()
        if not answer_text:
            return None

        try:
            parsed_message = self.parse_tool_calls(ChatMessage(role=role, content=answer_text))
        except Exception:
            return None
        return parsed_message.tool_calls

    def generate(
        self,
        messages: list[ChatMessage | dict],
        stop_sequences: list[str] | None = None,
        response_format: dict[str, str] | None = None,
        tools_to_call_from: list[Any] | None = None,
        **kwargs: Any,
    ) -> ChatMessage:
        completion_kwargs = self._prepare_completion_kwargs(
            messages=messages,
            stop_sequences=stop_sequences,
            response_format=response_format,
            tools_to_call_from=tools_to_call_from,
            model=self.model_id,
            custom_role_conversions=self.custom_role_conversions,
            **kwargs,
        )
        payload = self._build_payload(completion_kwargs)

        self._apply_rate_limit()
        response = self.retryer(self.client.chat, payload)

        response_message = response.get("message") or {}
        role = response_message.get("role", "assistant")
        content = response_message.get("content")
        tool_calls = self._parse_tool_calls(response_message)
        if tool_calls is None:
            tool_calls = self._parse_tool_call_from_text(role, content)
        if tool_calls is None:
            tool_calls = self._synthesize_final_answer_tool_call(content, tools_to_call_from)

        return ChatMessage(
            role=role,
            content=content,
            tool_calls=tool_calls,
            raw=response,
            token_usage=TokenUsage(
                input_tokens=int(response.get("prompt_eval_count") or 0),
                output_tokens=int(response.get("eval_count") or 0),
            ),
        )


def resolve_tool_path(raw_path: str, *, base_path: str = "") -> Path:
    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()

    root = Path(base_path).expanduser() if base_path else CURRENT_WORKING_DIRECTORY
    return (root / candidate).resolve()


@tool
def list_files(path: str, recursive: bool = False, include_hidden: bool = False) -> dict[str, Any]:
    """List files and folders in a directory.

    Args:
        path: Directory to inspect. Relative paths are resolved from the configured working directory.
        recursive: Whether to include nested files and folders.
        include_hidden: Whether to include names starting with a dot.
    """
    target_path = resolve_tool_path(path)
    if not target_path.exists():
        raise FileNotFoundError(f"Path does not exist: {target_path}")
    if not target_path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {target_path}")

    iterator = target_path.rglob("*") if recursive else target_path.iterdir()
    entries: list[dict[str, Any]] = []
    for entry in sorted(iterator, key=lambda item: str(item).lower()):
        if not include_hidden and entry.name.startswith("."):
            continue
        entries.append(
            {
                "path": str(entry.relative_to(target_path)),
                "type": "directory" if entry.is_dir() else "file",
            }
        )

    return {
        "path": str(target_path),
        "recursive": recursive,
        "include_hidden": include_hidden,
        "count": len(entries),
        "entries": entries,
    }


@tool
def list_files_by_size(
    path: str,
    recursive: bool = False,
    include_hidden: bool = False,
    descending: bool = True,
    limit: int | None = None,
) -> dict[str, Any]:
    """List files in a directory ordered by byte size.

    Args:
        path: Directory to inspect. Relative paths are resolved from the configured working directory.
        recursive: Whether to include nested files.
        include_hidden: Whether to include names starting with a dot.
        descending: Whether to sort largest files first.
        limit: Optional maximum number of files to return after sorting.
    """
    target_path = resolve_tool_path(path)
    if not target_path.exists():
        raise FileNotFoundError(f"Path does not exist: {target_path}")
    if not target_path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {target_path}")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be greater than zero")

    iterator = target_path.rglob("*") if recursive else target_path.iterdir()
    files: list[dict[str, Any]] = []
    for entry in iterator:
        if not entry.is_file():
            continue
        if not include_hidden and entry.name.startswith("."):
            continue
        files.append(
            {
                "name": entry.name,
                "bytes": entry.stat().st_size,
            }
        )

    files.sort(key=lambda item: (item["bytes"], item["name"].casefold()), reverse=descending)
    if limit is not None:
        files = files[:limit]

    return {
        "path": str(target_path),
        "recursive": recursive,
        "include_hidden": include_hidden,
        "descending": descending,
        "count": len(files),
        "files": files,
    }


@tool
def crc32(paths: list[str], base_path: str = "", chunk_size: int = CRC32_DEFAULT_CHUNK_SIZE) -> dict[str, Any]:
    """Compute CRC32 checksums for one or more files.

    Args:
        paths: File paths to checksum. Relative paths are resolved from base_path when provided, otherwise from the configured working directory.
        base_path: Optional base directory used to resolve relative paths in paths.
        chunk_size: Read size in bytes for incremental checksum calculation.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    if not paths:
        raise ValueError("paths is required")

    resolved_base_path = resolve_tool_path(base_path) if base_path else CURRENT_WORKING_DIRECTORY
    if base_path and not resolved_base_path.is_dir():
        raise NotADirectoryError(f"base_path is not a directory: {resolved_base_path}")

    normalized_paths: list[str] = []
    seen_paths: set[str] = set()
    for raw_path in paths:
        path_text = str(raw_path).strip()
        if not path_text:
            continue
        key = path_text.casefold()
        if key in seen_paths:
            continue
        seen_paths.add(key)
        normalized_paths.append(path_text)

    if not normalized_paths:
        raise ValueError("paths is required")

    files: list[dict[str, Any]] = []
    for relative_path in normalized_paths:
        target_path = resolve_tool_path(relative_path, base_path=str(resolved_base_path))
        if not target_path.exists() or not target_path.is_file():
            raise FileNotFoundError(f"Path is not a file: {target_path}")

        checksum = 0
        with target_path.open("rb") as file_handle:
            while True:
                chunk = file_handle.read(chunk_size)
                if not chunk:
                    break
                checksum = binascii.crc32(chunk, checksum)

        try:
            display_path = str(target_path.relative_to(CURRENT_WORKING_DIRECTORY))
        except ValueError:
            display_path = str(target_path)

        files.append(
            {
                "path": display_path,
                "absolute_path": str(target_path),
                "crc32": f"{checksum & 0xFFFFFFFF:08X}",
                "bytes": target_path.stat().st_size,
            }
        )

    if len(files) == 1:
        return {**files[0], "files": files}
    return {"count": len(files), "files": files}


@tool
def file_info(paths: list[str], base_path: str = "", fields: list[str] | None = None) -> dict[str, Any]:
    """Get detailed file metadata for one or more files.

    Args:
        paths: File paths to inspect. Use relative paths when you also provide base_path.
        base_path: Optional base directory used to resolve relative paths in paths.
        fields: Optional list of metadata field names to return. Supported fields are path, path_from_base, absolute_path, name, stem, suffix, parent, bytes, created, modified, accessed, is_symlink, is_hidden, and is_readonly.
    """
    if not paths:
        raise ValueError("paths is required")

    resolved_base_path = resolve_tool_path(base_path) if base_path else CURRENT_WORKING_DIRECTORY
    if base_path and not resolved_base_path.is_dir():
        raise NotADirectoryError(f"base_path is not a directory: {resolved_base_path}")

    selected_fields = FILE_INFO_AVAILABLE_FIELDS
    if fields:
        normalized_fields: list[str] = []
        seen_fields: set[str] = set()
        for field_name in fields:
            name = str(field_name).strip()
            if not name:
                continue
            key = name.casefold()
            if key in seen_fields:
                continue
            seen_fields.add(key)
            normalized_fields.append(name)

        invalid_fields = [field_name for field_name in normalized_fields if field_name not in FILE_INFO_AVAILABLE_FIELDS]
        if invalid_fields:
            raise ValueError(
                "Unsupported file_info fields: "
                + ", ".join(invalid_fields)
                + ". Valid fields: "
                + ", ".join(FILE_INFO_AVAILABLE_FIELDS)
            )
        selected_fields = tuple(normalized_fields)

    selected_field_set = set(selected_fields)
    for required_field in FILE_INFO_REQUIRED_OUTPUT_FIELDS:
        if required_field not in selected_field_set:
            selected_fields = (required_field, *selected_fields)
            selected_field_set.add(required_field)

    files: list[dict[str, Any]] = []
    for raw_path in paths:
        path_text = str(raw_path).strip()
        if not path_text:
            continue
        target_path = resolve_tool_path(path_text, base_path=str(resolved_base_path))
        if not target_path.exists() or not target_path.is_file():
            raise FileNotFoundError(f"Path is not a file: {target_path}")

        stat_result = target_path.stat()
        try:
            relative_to_working_directory = str(target_path.relative_to(CURRENT_WORKING_DIRECTORY))
        except ValueError:
            relative_to_working_directory = str(target_path)

        try:
            relative_to_base_path = str(target_path.relative_to(resolved_base_path))
        except ValueError:
            relative_to_base_path = path_text

        file_record = {
            "path": relative_to_working_directory,
            "path_from_base": relative_to_base_path,
            "absolute_path": str(target_path),
            "name": target_path.name,
            "stem": target_path.stem,
            "suffix": target_path.suffix,
            "parent": str(target_path.parent),
            "bytes": stat_result.st_size,
            "created": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(stat_result.st_ctime)),
            "modified": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(stat_result.st_mtime)),
            "accessed": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(stat_result.st_atime)),
            "is_symlink": target_path.is_symlink(),
            "is_hidden": target_path.name.startswith("."),
            "is_readonly": not bool(stat_result.st_mode & 0o200),
        }
        files.append({field_name: file_record[field_name] for field_name in selected_fields})

    if len(files) == 1:
        return {**files[0], "files": files}
    return {"count": len(files), "files": files}


@tool
def rename_files(renames: list[dict[str, str]], base_path: str = "") -> dict[str, Any]:
    """Rename one or more files or folders using explicit from and to pairs.

    Args:
        renames: List of rename operations. Each item must contain from and to keys.
        base_path: Optional base directory used to resolve relative from and to paths.
    """
    if not renames:
        raise ValueError("renames is required")

    resolved_base_path = resolve_tool_path(base_path) if base_path else CURRENT_WORKING_DIRECTORY
    if base_path and not resolved_base_path.is_dir():
        raise NotADirectoryError(f"base_path is not a directory: {resolved_base_path}")

    planned_operations: list[tuple[Path, Path]] = []
    seen_sources: set[str] = set()
    seen_destinations: set[str] = set()
    for index, rename in enumerate(renames, start=1):
        source_raw = str(rename.get("from", "")).strip()
        destination_raw = str(rename.get("to", "")).strip()
        if not source_raw:
            raise ValueError(f"rename entry {index} is missing 'from'")
        if not destination_raw:
            raise ValueError(f"rename entry {index} is missing 'to'")

        source_path = resolve_tool_path(source_raw, base_path=str(resolved_base_path))
        destination_path = resolve_tool_path(destination_raw, base_path=str(resolved_base_path))
        source_key = str(source_path).casefold()
        destination_key = str(destination_path).casefold()
        if source_key in seen_sources:
            raise ValueError(f"duplicate source path in renames: {source_path}")
        if destination_key in seen_destinations:
            raise ValueError(f"duplicate destination path in renames: {destination_path}")

        seen_sources.add(source_key)
        seen_destinations.add(destination_key)

        if not source_path.exists():
            raise FileNotFoundError(f"source path does not exist: {source_path}")
        if destination_path.exists() and source_key != destination_key:
            raise FileExistsError(f"destination path already exists: {destination_path}")

        planned_operations.append((source_path, destination_path))

    renamed: list[dict[str, Any]] = []
    for source_path, destination_path in planned_operations:
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.rename(destination_path)
        renamed.append(
            {
                "from": str(source_path),
                "to": str(destination_path),
            }
        )

    return {"count": len(renamed), "renamed": renamed}


def build_agent(
    model_name: str,
    host: str,
    max_steps: int,
    max_tokens: int,
    num_ctx: int,
    reasoning_effort: str,
    verbosity: str,
) -> ToolCallingAgent:
    model = OllamaChatModel(
        model_id=model_name,
        host=host,
        temperature=0.1,
        max_tokens=max_tokens,
        num_ctx=num_ctx,
        reasoning_effort=reasoning_effort,
    )
    return ToolCallingAgent(
        tools=[list_files, list_files_by_size, crc32, file_info, rename_files],
        model=model,
        instructions=AGENT_INSTRUCTIONS,
        max_steps=max_steps,
        verbosity_level=parse_verbosity_level(verbosity),
        logger=build_logger(verbosity),
    )


def run_agent_task(agent: ToolCallingAgent, prompt: str, max_steps: int) -> None:
    result = agent.run(prompt, max_steps=max_steps, reset=True)
    print(result)


def interactive_repl(agent: ToolCallingAgent, max_steps: int) -> None:
    while True:
        prompt = input("Task> ").strip()
        if not prompt:
            continue
        if prompt.lower() in {"exit", "quit"}:
            return
        run_agent_task(agent, prompt, max_steps)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="smolagents tool agent for a local Ollama server.")
    parser.add_argument("prompt", nargs="?", help="Task for the agent. If omitted, starts interactive mode.")
    parser.add_argument("--model", default=DEFAULT_MODEL_NAME, help=f"Ollama model to use (default: {DEFAULT_MODEL_NAME}).")
    parser.add_argument("--host", default=DEFAULT_OLLAMA_HOST, help=f"Ollama server URL (default: {DEFAULT_OLLAMA_HOST}).")
    parser.add_argument(
        "--working-directory",
        default=DEFAULT_WORKING_DIRECTORY,
        help="Base working directory for tool operations (default: current directory).",
    )
    parser.add_argument("--max-steps", type=int, default=DEFAULT_MAX_STEPS, help=f"Maximum reasoning/tool rounds per task (default: {DEFAULT_MAX_STEPS}).")
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=DEFAULT_MAX_TOKENS,
        help=f"Maximum completion tokens to request from Ollama's native chat API (default: {DEFAULT_MAX_TOKENS}).",
    )
    parser.add_argument(
        "--num-ctx",
        type=int,
        default=DEFAULT_NUM_CTX,
        help=f"Context window to request from Ollama via options.num_ctx (default: {DEFAULT_NUM_CTX}).",
    )
    parser.add_argument(
        "--reasoning-effort",
        choices=["high", "medium", "low", "none"],
        default=DEFAULT_REASONING_EFFORT,
        help=f"Reasoning effort to request for thinking-capable models (default: {DEFAULT_REASONING_EFFORT}).",
    )
    parser.add_argument(
        "--verbosity",
        choices=["off", "error", "compact", "info", "debug"],
        default=DEFAULT_VERBOSITY,
        help=f"smolagents console trace level (default: {DEFAULT_VERBOSITY}).",
    )
    return parser.parse_args()


def main() -> None:
    global CURRENT_WORKING_DIRECTORY

    args = parse_args()
    CURRENT_WORKING_DIRECTORY = Path(args.working_directory).resolve()
    agent = build_agent(
        args.model,
        args.host,
        args.max_steps,
        args.max_tokens,
        args.num_ctx,
        args.reasoning_effort,
        args.verbosity,
    )

    if args.prompt:
        run_agent_task(agent, args.prompt, args.max_steps)
        return

    interactive_repl(agent, args.max_steps)


if __name__ == "__main__":
    main()
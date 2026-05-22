from __future__ import annotations

import argparse
import binascii
import httpx
import json
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import ollama
from colorama import Fore, Style, init
from tqdm import tqdm


AGENT_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "thinking": {
            "type": "string",
            "description": "A brief reasoning summary that is safe to show to the user.",
        },
        "message": {
            "type": "string",
            "description": "A short message for the user about the current plan or result.",
        },
        "tool_calls": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "tool": {"type": "string"},
                    "arguments": {"type": "object"},
                    "purpose": {"type": "string"},
                },
                "required": ["tool", "arguments", "purpose"],
                "additionalProperties": False,
            },
        },
        "final_answer": {
            "type": "string",
            "description": "The final response once no more tools are needed.",
        },
    },
    "required": ["thinking", "message", "tool_calls", "final_answer"],
    "additionalProperties": False,
}


init(autoreset=True)


def color_text(text: str, color: str = "", *, bright: bool = False) -> str:
    prefix = color
    if bright:
        prefix += Style.BRIGHT
    return f"{prefix}{text}{Style.RESET_ALL}"


def print_section(title: str, color: str = Fore.CYAN) -> None:
    print(f"\n{color_text(title, color, bright=True)}")


def print_kv(label: str, value: str, color: str = Fore.CYAN) -> None:
    print(f"{color_text(label, color, bright=True)} {value}")


@dataclass
class ToolDefinition:
    name: str
    description: str
    parameters_schema: dict[str, Any]
    handler: Callable[[dict[str, Any]], dict[str, Any]]


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, ToolDefinition] = {}

    def register(self, tool: ToolDefinition) -> None:
        self._tools[tool.name] = tool

    def describe(self) -> list[dict[str, Any]]:
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters_schema,
            }
            for tool in self._tools.values()
        ]

    def execute(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        tool = self._tools.get(tool_name)
        if tool is None:
            return {
                "ok": False,
                "error": f"Unknown tool: {tool_name}",
            }

        try:
            result = tool.handler(arguments)
            return {
                "ok": True,
                "tool": tool_name,
                "result": result,
            }
        except Exception as exc:  # pragma: no cover - defensive CLI boundary
            return {
                "ok": False,
                "tool": tool_name,
                "error": str(exc),
            }


class LiveStatus:
    def __init__(self, label: str, interval_seconds: float = 0.2) -> None:
        self.label = label
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self, final_message: str | None = None) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()

        clear_line = "\r" + (" " * 100) + "\r"
        print(clear_line, end="", flush=True)
        if final_message:
            print(color_text(final_message, Fore.BLUE, bright=True))

    def _run(self) -> None:
        frames = "|/-\\"
        frame_index = 0
        start_time = time.perf_counter()

        while not self._stop_event.wait(self.interval_seconds):
            elapsed = time.perf_counter() - start_time
            frame = frames[frame_index % len(frames)]
            print(
                color_text(f"\r{self.label} {frame} {elapsed:0.1f}s", Fore.BLUE, bright=True),
                end="",
                flush=True,
            )
            frame_index += 1


def format_duration_ns(duration_ns: int | None) -> str | None:
    if duration_ns is None:
        return None

    duration_seconds = duration_ns / 1_000_000_000
    if duration_seconds >= 1:
        return f"{duration_seconds:.2f}s"
    return f"{duration_seconds * 1000:.0f}ms"


def compute_tokens_per_second(token_count: int | None, duration_ns: int | None) -> float | None:
    if token_count is None or duration_ns is None or duration_ns <= 0:
        return None
    return token_count / (duration_ns / 1_000_000_000)


def build_turn_metrics(chunk: Any | None) -> dict[str, Any] | None:
    if chunk is None:
        return None

    metrics = {
        "total_duration": getattr(chunk, "total_duration", None),
        "load_duration": getattr(chunk, "load_duration", None),
        "prompt_eval_count": getattr(chunk, "prompt_eval_count", None),
        "prompt_eval_duration": getattr(chunk, "prompt_eval_duration", None),
        "eval_count": getattr(chunk, "eval_count", None),
        "eval_duration": getattr(chunk, "eval_duration", None),
    }

    if not any(value is not None for value in metrics.values()):
        return None

    return metrics


def print_turn_metrics(metrics: dict[str, Any] | None) -> None:
    if not metrics:
        return

    generated_tokens = metrics.get("eval_count")
    generated_tps = compute_tokens_per_second(generated_tokens, metrics.get("eval_duration"))
    prompt_tokens = metrics.get("prompt_eval_count")
    prompt_tps = compute_tokens_per_second(prompt_tokens, metrics.get("prompt_eval_duration"))
    total_duration = format_duration_ns(metrics.get("total_duration"))
    load_duration = format_duration_ns(metrics.get("load_duration"))

    parts: list[str] = []
    if generated_tokens is not None:
        parts.append(f"generated {generated_tokens} tok")
    if generated_tps is not None:
        parts.append(f"{generated_tps:.1f} tok/s")
    if prompt_tokens is not None:
        prompt_part = f"prompt {prompt_tokens} tok"
        if prompt_tps is not None:
            prompt_part += f" @ {prompt_tps:.1f} tok/s"
        parts.append(prompt_part)
    if load_duration is not None:
        parts.append(f"load {load_duration}")
    if total_duration is not None:
        parts.append(f"total {total_duration}")

    if parts:
        print_kv("Model stats:", " | ".join(parts), Fore.MAGENTA)


def compact_text(value: Any, max_length: int = 240) -> str:
    if isinstance(value, str):
        text = value.strip()
    else:
        text = json.dumps(value, ensure_ascii=True, separators=(",", ":"))

    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."


def append_task_history(task_history: list[str], entry: str, max_entries: int = 16) -> None:
    task_history.append(entry)
    if len(task_history) > max_entries:
        del task_history[:-max_entries]


def build_task_state_summary(original_prompt: str, task_history: list[str]) -> str:
    history_lines = "\n".join(f"- {entry}" for entry in task_history) if task_history else "- No prior steps yet."
    return (
        "Persistent task state. Use this to preserve continuity across turns.\n"
        f"Original task: {original_prompt}\n"
        "Known history:\n"
        f"{history_lines}"
    )


def build_turn_messages(messages: list[dict[str, str]], task_state_summary: str) -> list[dict[str, str]]:
    if not messages:
        return [{"role": "system", "content": task_state_summary}]

    return [messages[0], {"role": "system", "content": task_state_summary}, *messages[1:]]


def print_task_state_summary(task_state_summary: str) -> None:
    print_section("Task State", Fore.WHITE)
    for line in task_state_summary.splitlines():
        if line.startswith("- "):
            print(color_text(line, Fore.WHITE))
        else:
            print(color_text(line, Fore.WHITE, bright=True))


def build_system_prompt(registry: ToolRegistry, working_directory: Path) -> str:
    tool_descriptions = json.dumps(registry.describe(), indent=2)
    return f"""
You are a local task agent.

Return exactly one JSON object with these keys:
- thinking: a concise reasoning summary that is safe to display to the user
- message: short user-facing status text
- tool_calls: an array of planned tool calls
- final_answer: the completed answer when no more tools are needed

Rules:
- Output the keys in this exact order: thinking, message, tool_calls, final_answer.
- Keep thinking brief and high-level. Do not include hidden chain-of-thought. Summarize only the next step or decision.
- Never claim that a tool has already run unless tool results are provided later in the conversation.
- Any tool call must be proposed first. The human will review and approve or reject the plan.
- Propose at most one tool call per response. If a task needs multiple steps, propose only the next immediate tool action and wait for the result before proposing another one.
- Keep tool calls minimal and relevant to the user's request.
- Only use the exact tool names shown below.
- If a previous plan was rejected, revise it to match the user's feedback.
- After tool results are provided, use them to produce final_answer instead of repeating the same tool call.
- If a tool is needed, leave final_answer empty until after the tool results arrive.
- Do not return blank output.

The current working directory is:
{working_directory}

Available tools:
{tool_descriptions}
""".strip()


def parse_agent_response(raw_content: str) -> dict[str, Any]:
    data = json.loads(raw_content)
    if not isinstance(data, dict):
        raise ValueError("Agent response was not a JSON object.")

    thinking = data.get("thinking", "")
    message = data.get("message", "")
    tool_calls = data.get("tool_calls", [])
    final_answer = data.get("final_answer", "")

    if not isinstance(thinking, str):
        raise ValueError("thinking must be a string.")
    if not isinstance(message, str):
        raise ValueError("message must be a string.")
    if not isinstance(tool_calls, list):
        raise ValueError("tool_calls must be a list.")
    if not isinstance(final_answer, str):
        raise ValueError("final_answer must be a string.")

    return {
        "thinking": thinking,
        "message": message,
        "tool_calls": tool_calls,
        "final_answer": final_answer,
    }


def format_tool_call(tool_call: dict[str, Any], index: int) -> str:
    tool_name = str(tool_call.get("tool", "unknown"))
    purpose = str(tool_call.get("purpose", "")).strip()
    arguments = tool_call.get("arguments", {})
    arguments_text = json.dumps(arguments, ensure_ascii=True)

    if purpose:
        return f"  {index}. {tool_name} {arguments_text} - {purpose}"
    return f"  {index}. {tool_name} {arguments_text}"


def summarize_tool_result(tool_result: dict[str, Any]) -> str:
    execution = tool_result.get("execution", {})
    tool_name = str(tool_result.get("tool", "unknown"))

    if not isinstance(execution, dict):
        return f"- {tool_name}: invalid tool result"

    if not execution.get("ok"):
        return f"- {tool_name}: failed - {execution.get('error', 'unknown error')}"

    result = execution.get("result", {})
    if tool_name == "list_files" and isinstance(result, dict):
        path = result.get("path", "")
        count = result.get("count", 0)
        return f"- {tool_name}: listed {count} entries in {path}"
    if tool_name == "crc32" and isinstance(result, dict):
        files = result.get("files", [])
        if isinstance(files, list) and files:
            if len(files) == 1 and isinstance(files[0], dict):
                path = files[0].get("path", "")
                checksum = files[0].get("crc32", "")
                return f"- {tool_name}: {checksum} for {path}"
            return f"- {tool_name}: computed checksums for {len(files)} files"

        path = result.get("path", "")
        checksum = result.get("crc32", "")
        return f"- {tool_name}: {checksum} for {path}"

    return f"- {tool_name}: completed successfully"


def print_tool_plan(tool_calls: list[dict[str, Any]]) -> None:
    print_section("Planned actions:", Fore.YELLOW)
    for index, tool_call in enumerate(tool_calls, start=1):
        print(color_text(format_tool_call(tool_call, index), Fore.YELLOW))


def print_tool_results(tool_results: list[dict[str, Any]]) -> None:
    print_section("Tool results:", Fore.GREEN)
    for tool_result in tool_results:
        print(color_text(summarize_tool_result(tool_result), Fore.GREEN))


def extract_json_string_field(raw_content: str, field_name: str) -> tuple[str, bool] | None:
    field_marker = f'"{field_name}"'
    marker_index = raw_content.find(field_marker)
    if marker_index < 0:
        return None

    colon_index = raw_content.find(":", marker_index + len(field_marker))
    if colon_index < 0:
        return None

    quote_index = raw_content.find('"', colon_index + 1)
    if quote_index < 0:
        return None

    characters: list[str] = []
    position = quote_index + 1

    while position < len(raw_content):
        character = raw_content[position]

        if character == '"':
            return "".join(characters), True

        if character == "\\":
            position += 1
            if position >= len(raw_content):
                return "".join(characters), False

            escape_character = raw_content[position]
            escapes = {
                '"': '"',
                "\\": "\\",
                "/": "/",
                "b": "\b",
                "f": "\f",
                "n": "\n",
                "r": "\r",
                "t": "\t",
            }

            if escape_character == "u":
                unicode_chunk = raw_content[position + 1 : position + 5]
                if len(unicode_chunk) < 4:
                    return "".join(characters), False
                try:
                    characters.append(chr(int(unicode_chunk, 16)))
                except ValueError:
                    characters.append("?")
                position += 4
            else:
                characters.append(escapes.get(escape_character, escape_character))
        else:
            characters.append(character)

        position += 1

    return "".join(characters), False


def resolve_path(working_directory: Path, raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = working_directory / candidate
    return candidate.resolve()


def make_list_files_tool(working_directory: Path) -> ToolDefinition:
    def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        target_path = resolve_path(working_directory, str(arguments.get("path", ".")))
        recursive = bool(arguments.get("recursive", False))
        include_hidden = bool(arguments.get("include_hidden", False))

        if not target_path.exists():
            raise FileNotFoundError(f"Path does not exist: {target_path}")
        if not target_path.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {target_path}")

        iterator = target_path.rglob("*") if recursive else target_path.iterdir()
        entries: list[dict[str, Any]] = []

        for entry in sorted(iterator, key=lambda item: str(item).lower()):
            if not include_hidden and entry.name.startswith("."):
                continue

            try:
                relative_path = entry.relative_to(working_directory)
                display_path = str(relative_path)
            except ValueError:
                display_path = str(entry)

            entries.append(
                {
                    "path": display_path,
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

    return ToolDefinition(
        name="list_files",
        description="List files and folders in a directory.",
        parameters_schema={
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Directory to inspect. Relative paths are resolved from the working directory.",
                },
                "recursive": {
                    "type": "boolean",
                    "description": "Whether to include nested files and folders.",
                },
                "include_hidden": {
                    "type": "boolean",
                    "description": "Whether to include names starting with a dot.",
                },
            },
            "additionalProperties": False,
        },
        handler=handler,
    )


def make_crc32_tool(working_directory: Path) -> ToolDefinition:
    def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        chunk_size = int(arguments.get("chunk_size", 1024 * 1024))
        raw_paths = arguments.get("paths", [])

        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than zero")

        if not isinstance(raw_paths, list):
            raise ValueError("paths must be a list of file paths")

        requested_paths = [str(path).strip() for path in raw_paths if str(path).strip()]

        normalized_paths: list[str] = []
        seen_paths: set[str] = set()
        for requested_path in requested_paths:
            normalized_key = requested_path.casefold()
            if normalized_key in seen_paths:
                continue
            seen_paths.add(normalized_key)
            normalized_paths.append(requested_path)

        if not normalized_paths:
            raise ValueError("paths is required")

        resolved_paths: list[Path] = []
        total_bytes = 0
        for requested_path in normalized_paths:
            target_path = resolve_path(working_directory, requested_path)
            if not target_path.exists():
                raise FileNotFoundError(f"Path does not exist: {target_path}")
            if not target_path.is_file():
                raise FileNotFoundError(f"Path is not a file: {target_path}")

            resolved_paths.append(target_path)
            total_bytes += target_path.stat().st_size

        files: list[dict[str, Any]] = []
        with tqdm(
            total=total_bytes,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc="CRC32",
            colour="green",
            dynamic_ncols=True,
        ) as progress_bar:
            for target_path in resolved_paths:
                checksum = 0
                progress_bar.set_postfix_str(target_path.name)

                with target_path.open("rb") as file_handle:
                    while True:
                        chunk = file_handle.read(chunk_size)
                        if not chunk:
                            break
                        checksum = binascii.crc32(chunk, checksum)
                        progress_bar.update(len(chunk))

                checksum_text = f"{checksum & 0xFFFFFFFF:08X}"

                try:
                    relative_path = target_path.relative_to(working_directory)
                    display_path = str(relative_path)
                except ValueError:
                    display_path = str(target_path)

                files.append(
                    {
                        "path": display_path,
                        "absolute_path": str(target_path),
                        "crc32": checksum_text,
                        "bytes": target_path.stat().st_size,
                    }
                )

        if len(files) == 1:
            single = files[0]
            return {
                **single,
                "files": files,
            }

        return {
            "count": len(files),
            "files": files,
        }

    return ToolDefinition(
        name="crc32",
        description="Compute the CRC32 checksum of one or more files.",
        parameters_schema={
            "type": "object",
            "properties": {
                "paths": {
                    "type": "array",
                    "description": "Files to checksum. Relative paths are resolved from the working directory.",
                    "items": {
                        "type": "string",
                    },
                    "minItems": 1,
                },
                "chunk_size": {
                    "type": "integer",
                    "description": "Read size in bytes for incremental checksum calculation.",
                    "minimum": 1,
                },
            },
            "required": ["paths"],
            "additionalProperties": False,
        },
        handler=handler,
    )


def build_registry(working_directory: Path) -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(make_list_files_tool(working_directory))
    registry.register(make_crc32_tool(working_directory))
    return registry


def ask_for_approval(tool_calls: list[dict[str, Any]]) -> tuple[str, str]:
    print_tool_plan(tool_calls)
    decision = input(
        color_text("Approve this plan? [y]es / [g]uide / [n]o: ", Fore.YELLOW, bright=True)
    ).strip().lower()
    if decision in {"y", "yes"}:
        return "approve", ""
    if decision in {"g", "guide"}:
        guidance = input(
            color_text("Guidance for this plan: ", Fore.CYAN, bright=True)
        ).strip()
        if not guidance:
            return "cancel", ""
        return "guide", guidance

    feedback = input(
        color_text("Plan rejected. What should change? (leave blank to cancel): ", Fore.RED, bright=True)
    ).strip()
    if not feedback:
        return "cancel", ""
    return "reject", feedback


def ask_for_turn_guidance(prompt_text: str | None = None) -> str | None:
    if prompt_text is None:
        prompt_text = "Guidance for the next turn? (Enter to continue, 'cancel' to stop): "

    guidance = input(
        color_text(
            prompt_text,
            Fore.CYAN,
            bright=True,
        )
    ).strip()

    if not guidance:
        return None
    if guidance.lower() == "cancel":
        return ""
    return guidance


def request_agent_turn(
    client: ollama.Client,
    model: str,
    messages: list[dict[str, str]],
    task_state_summary: str,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    attempt_messages = build_turn_messages(messages, task_state_summary)

    for attempt_index in range(2):
        raw_parts: list[str] = []
        printed_thinking = ""
        thinking_started = False
        thinking_finished = False
        first_chunk_received = False
        completion_status_started = False
        completion_status: LiveStatus | None = None
        last_chunk: Any | None = None
        print_section("Model", Fore.BLUE)
        print(color_text("preparing response...", Fore.BLUE))
        status = LiveStatus("Model is thinking")
        status.start()

        try:
            try:
                stream = client.chat(
                    model=model,
                    messages=attempt_messages,
                    format=AGENT_RESPONSE_SCHEMA,
                    options={"temperature": 0.1},
                    stream=True,
                )

                for chunk in stream:
                    last_chunk = chunk
                    content = chunk.message.content if chunk.message and chunk.message.content else ""
                    if not content:
                        continue

                    if not first_chunk_received:
                        status.stop("Model: response stream started.")
                        first_chunk_received = True

                    raw_parts.append(content)

                    current_raw = "".join(raw_parts)
                    extracted = extract_json_string_field(current_raw, "thinking")
                    if extracted is None:
                        continue

                    current_thinking, is_complete = extracted
                    if not thinking_started:
                        print(color_text("Thinking: ", Fore.CYAN, bright=True), end="", flush=True)
                        thinking_started = True

                    if len(current_thinking) > len(printed_thinking):
                        delta = current_thinking[len(printed_thinking) :]
                        print(delta, end="", flush=True)
                        printed_thinking = current_thinking

                    if is_complete and not thinking_finished:
                        print()
                        sys.stdout.flush()
                        thinking_finished = True
                        print(color_text("Model: finalizing response...", Fore.BLUE))
                        completion_status = LiveStatus("Model is finalizing response")
                        completion_status.start()
                        completion_status_started = True
            except KeyboardInterrupt as exc:
                if thinking_started and not thinking_finished:
                    print()
                print(color_text("Model turn cancelled by user.", Fore.RED, bright=True))
                raise RuntimeError("Model turn cancelled by user.") from exc
            except httpx.TimeoutException as exc:
                if thinking_started and not thinking_finished:
                    print()
                print(color_text("Model stream timed out while waiting for Ollama.", Fore.RED, bright=True))
                if attempt_index == 0:
                    print(color_text("Retrying the model turn once...", Fore.YELLOW, bright=True))
                    continue
                raise RuntimeError(
                    "Ollama response timed out. Increase --read-timeout if the model needs more idle time."
                ) from exc
            except httpx.HTTPError as exc:
                if thinking_started and not thinking_finished:
                    print()
                raise RuntimeError(f"Ollama HTTP error: {exc}") from exc
        finally:
            if not first_chunk_received:
                status.stop("Model responded.")
            if completion_status_started and completion_status is not None:
                completion_status.stop("Model: response finalized.")

        raw_content = "".join(raw_parts)

        if thinking_started and not thinking_finished:
            print()
            sys.stdout.flush()

        turn_metrics = build_turn_metrics(last_chunk)
        print_turn_metrics(turn_metrics)

        if raw_content.strip():
            return parse_agent_response(raw_content), turn_metrics

        if attempt_index == 0:
            attempt_messages.append(
                {
                    "role": "user",
                    "content": (
                        "Your previous reply was blank. Return exactly one JSON object with the keys "
                        "thinking, message, tool_calls, and final_answer. Thinking must be a brief, user-visible "
                        "summary rather than hidden chain-of-thought. If you already have tool results, answer "
                        "from them instead of repeating the same tool call."
                    ),
                }
            )

    raise ValueError("Model returned blank output twice in a row.")


def run_agent_task(
    client: ollama.Client,
    model: str,
    registry: ToolRegistry,
    prompt: str,
    working_directory: Path,
    max_steps: int,
) -> None:
    original_prompt = prompt
    task_history: list[str] = []
    messages: list[dict[str, str]] = [
        {"role": "system", "content": build_system_prompt(registry, working_directory)},
        {"role": "user", "content": prompt},
    ]
    last_approved_plan_signature: str | None = None

    for step_index in range(max_steps):
        task_state_summary = build_task_state_summary(original_prompt, task_history)
        print_task_state_summary(task_state_summary)
        agent_response, _turn_metrics = request_agent_turn(client, model, messages, task_state_summary)
        tool_calls = agent_response["tool_calls"]

        if agent_response["thinking"].strip():
            append_task_history(task_history, f"Thinking: {compact_text(agent_response['thinking'])}")
        if agent_response["message"].strip():
            append_task_history(task_history, f"Message: {compact_text(agent_response['message'])}")
        if tool_calls:
            append_task_history(task_history, f"Proposed tool step: {compact_text(tool_calls)}")

        if tool_calls:
            if len(tool_calls) > 1:
                print()
                print(
                    color_text(
                        "Plan note: only one tool step can be approved at a time. Using the first proposed step and deferring the rest.",
                        Fore.YELLOW,
                        bright=True,
                    )
                )
                tool_calls = [tool_calls[0]]
                agent_response = {
                    **agent_response,
                    "tool_calls": tool_calls,
                }

            plan_signature = json.dumps(tool_calls, sort_keys=True)

            if plan_signature == last_approved_plan_signature:
                messages.append({"role": "assistant", "content": json.dumps(agent_response)})
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "You already executed this exact tool plan and already have its results. "
                            "Do not repeat it. Use the latest tool results to provide final_answer unless "
                            "a different tool is strictly required."
                        ),
                    }
                )
                continue

            if agent_response["message"]:
                print_section("Plan", Fore.YELLOW)
                print(color_text(agent_response["message"], Fore.YELLOW))

            decision, feedback = ask_for_approval(tool_calls)
            messages.append({"role": "assistant", "content": json.dumps(agent_response)})

            if decision == "cancel":
                print(color_text("Cancelled by user.", Fore.RED, bright=True))
                return

            if decision == "guide":
                append_task_history(task_history, f"Tool guidance: {compact_text(feedback)}")
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            f"Original task: {original_prompt}\n\n"
                            "Update your approach using this user guidance before proposing the next tool step: "
                            f"{feedback}"
                        ),
                    }
                )
                continue

            if decision == "reject":
                if not feedback:
                    print(color_text("Cancelled by user.", Fore.RED, bright=True))
                    return

                append_task_history(task_history, f"Rejected plan feedback: {compact_text(feedback)}")

                messages.append(
                    {
                        "role": "user",
                        "content": (
                            f"Original task: {original_prompt}\n\n"
                            "The previous tool plan was not approved. Revise the plan to fit this feedback: "
                            f"{feedback}"
                        ),
                    }
                )
                continue

            if decision != "approve":
                raise ValueError(f"Unexpected approval decision: {decision}")

            tool_results = []
            for tool_call in tool_calls:
                tool_name = str(tool_call.get("tool", ""))
                arguments = tool_call.get("arguments", {})
                if not isinstance(arguments, dict):
                    arguments = {}
                tool_results.append(
                    {
                        "tool": tool_name,
                        "purpose": tool_call.get("purpose", ""),
                        "execution": registry.execute(tool_name, arguments),
                    }
                )

            last_approved_plan_signature = plan_signature

            print_tool_results(tool_results)
            append_task_history(task_history, f"Tool results: {compact_text(tool_results, max_length=400)}")
            messages.append(
                {
                    "role": "user",
                    "content": (
                        f"Original task: {original_prompt}\n\n"
                        "The approved tool step has been executed. Decide whether you can now answer or need "
                        "one next tool step. Use these tool results to continue:\n"
                        f"{json.dumps(tool_results)}"
                    ),
                }
            )
            continue

        final_answer = agent_response["final_answer"].strip() or agent_response["message"].strip()
        if final_answer:
            print_section("Answer", Fore.GREEN)
            print(color_text(final_answer, Fore.GREEN))
            append_task_history(task_history, f"Answer: {compact_text(final_answer)}")

            response_guidance = ask_for_turn_guidance(
                "Guidance for this response? (Enter to accept, 'cancel' to stop): "
            )
            if response_guidance == "":
                print(color_text("Cancelled by user.", Fore.RED, bright=True))
                return
            if response_guidance is None:
                return

            append_task_history(task_history, f"Response guidance: {compact_text(response_guidance)}")

            messages.append({"role": "assistant", "content": json.dumps(agent_response)})
            messages.append(
                {
                    "role": "user",
                    "content": (
                        f"Original task: {original_prompt}\n\n"
                        "Revise your response using this user guidance: "
                        f"{response_guidance}"
                    ),
                }
            )
            continue

        messages.append(
            {
                "role": "user",
                "content": "Your last response was empty. Either provide a final_answer or a tool plan.",
            }
        )

        if step_index == max_steps - 1:
            raise RuntimeError("Agent reached the maximum number of steps without completing the task.")


def interactive_repl(
    client: ollama.Client,
    model: str,
    registry: ToolRegistry,
    working_directory: Path,
    max_steps: int,
) -> None:
    print(color_text("Enter a task for the agent. Type 'exit' or 'quit' to stop.", Fore.CYAN, bright=True))
    while True:
        prompt = input(color_text("\nTask> ", Fore.CYAN, bright=True)).strip()
        if not prompt:
            continue
        if prompt.lower() in {"exit", "quit"}:
            return
        run_agent_task(client, model, registry, prompt, working_directory, max_steps)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Minimal Ollama-based tool agent with mandatory user approval before tool execution."
    )
    parser.add_argument("prompt", nargs="?", help="Task for the agent. If omitted, starts interactive mode.")
    parser.add_argument(
        "--model",
        default="gemma4:e2b",
        help="Ollama model to use (default: gemma4:e2b).",
    )
    parser.add_argument(
        "--host",
        default="http://localhost:11434",
        help="Ollama server URL (default: http://localhost:11434).",
    )
    parser.add_argument(
        "--working-directory",
        default=".",
        help="Base working directory for tool operations (default: current directory).",
    )
    parser.add_argument(
        "--max-steps",
        type=int,
        default=8,
        help="Maximum reasoning/tool rounds per task (default: 8).",
    )
    parser.add_argument(
        "--read-timeout",
        type=float,
        default=60.0,
        help="Maximum idle seconds to wait for Ollama response stream data before timing out (default: 60).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    working_directory = Path(args.working_directory).resolve()
    registry = build_registry(working_directory)
    timeout = httpx.Timeout(connect=10.0, read=args.read_timeout, write=30.0, pool=30.0)
    client = ollama.Client(host=args.host, timeout=timeout)

    try:
        if args.prompt:
            run_agent_task(client, args.model, registry, args.prompt, working_directory, args.max_steps)
            return

        interactive_repl(client, args.model, registry, working_directory, args.max_steps)
    except KeyboardInterrupt:
        print()
        print(color_text("Cancelled by user.", Fore.RED, bright=True))
        raise SystemExit(130)
    except RuntimeError as exc:
        print()
        print(color_text(str(exc), Fore.RED, bright=True))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
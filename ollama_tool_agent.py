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
        "action": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": ["tool", "respond"],
                },
                "tool": {
                    "type": "string",
                    "description": "Tool name when action.type is 'tool'; otherwise an empty string.",
                },
                "arguments": {
                    "type": "object",
                    "description": "Tool arguments when action.type is 'tool'; otherwise an empty object.",
                },
                "purpose": {
                    "type": "string",
                    "description": "Short reason for the tool call when action.type is 'tool'; otherwise an empty string.",
                },
                "content": {
                    "type": "string",
                    "description": "User-facing plan text for a tool action, or the full user-facing answer for a respond action.",
                },
            },
            "required": ["type", "tool", "arguments", "purpose", "content"],
            "additionalProperties": False,
        },
    },
    "required": ["thinking", "action"],
    "additionalProperties": False,
}


MAX_VISIBLE_THINKING_CHARS = 4096
DEFAULT_NUM_PREDICT = 32*1024
DEFAULT_MODEL_NAME = "gemma4:e2b"
DEFAULT_OLLAMA_HOST = "http://localhost:11434"
DEFAULT_WORKING_DIRECTORY = "."
DEFAULT_MAX_STEPS = 8
DEFAULT_READ_TIMEOUT_SECONDS = 60.0
DEFAULT_NUM_CTX = 65536
MODEL_TURN_TEMPERATURE = 0.1
REPAIR_TEMPERATURE = 0.0
MODEL_TURN_MAX_ATTEMPTS = 3
RECENT_MESSAGES_LIMIT = 6
RECENT_MESSAGE_MAX_LENGTH = 32*1024
INVALID_JSON_PREVIEW_MAX_LENGTH = 300
DEFAULT_TASK_HISTORY_MAX_ENTRIES = 16
DEFAULT_TASK_STATE_LOG_MAX_ENTRIES = 12
DEFAULT_TASK_STATE_VALUE_MAX_ENTRIES = 18
TURN_CONTEXT_MAX_ENTRIES = 24
TURN_CONTEXT_THINKING_MAX_LENGTH = 32*1024
TURN_CONTEXT_MESSAGE_MAX_LENGTH = 32*1024
TURN_CONTEXT_TOOL_STEP_MAX_LENGTH = 32*1024
TURN_CONTEXT_GUIDANCE_MAX_LENGTH = 32*1024
CRC32_DEFAULT_CHUNK_SIZE = 1024 * 1024
HTTP_CONNECT_TIMEOUT_SECONDS = 10.0
HTTP_WRITE_TIMEOUT_SECONDS = 30.0
HTTP_POOL_TIMEOUT_SECONDS = 30.0


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
    format_arguments: Callable[[dict[str, Any]], list[str]] | None = None


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, ToolDefinition] = {}

    def register(self, tool: ToolDefinition) -> None:
        self._tools[tool.name] = tool

    def get(self, tool_name: str) -> ToolDefinition | None:
        return self._tools.get(tool_name)

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


def append_task_history(
    task_history: list[str],
    entry: str,
    max_entries: int = DEFAULT_TASK_HISTORY_MAX_ENTRIES,
) -> None:
    task_history.append(entry)
    if len(task_history) > max_entries:
        del task_history[:-max_entries]


def append_task_state_log(
    task_state_log: list[str],
    entry: str,
    max_entries: int = DEFAULT_TASK_STATE_LOG_MAX_ENTRIES,
) -> None:
    task_state_log.append(entry)
    if len(task_state_log) > max_entries:
        del task_state_log[:-max_entries]


def append_task_state_value(
    task_state_log: list[str],
    label: str,
    value: Any,
    *,
    max_entry_length: int = 3000,
    max_entries: int = DEFAULT_TASK_STATE_VALUE_MAX_ENTRIES,
) -> None:
    if isinstance(value, str):
        text = value.strip()
    else:
        text = json.dumps(value, ensure_ascii=True, separators=(",", ":"))

    if not text:
        append_task_state_log(task_state_log, f"{label}: <empty>", max_entries=max_entries)
        return

    if len(text) <= max_entry_length:
        append_task_state_log(task_state_log, f"{label}: {text}", max_entries=max_entries)
        return

    chunk_size = max_entry_length
    chunks = [text[index : index + chunk_size] for index in range(0, len(text), chunk_size)]
    total_chunks = len(chunks)
    for index, chunk in enumerate(chunks, start=1):
        append_task_state_log(
            task_state_log,
            f"{label} (part {index}/{total_chunks}): {chunk}",
            max_entries=max_entries,
        )


def answer_looks_speculative(response_text: str) -> bool:
    normalized = response_text.casefold()
    speculative_markers = [
        "assuming",
        "hypothetical",
        "placeholder",
        "i must assume",
        "if this is",
        "not provided",
        "example",
        "movie_name_1",
        "actual file names",
        "based on the previous step",
    ]
    return any(marker in normalized for marker in speculative_markers)


def answer_looks_like_meta_progress(response_text: str) -> bool:
    normalized = response_text.casefold()
    meta_progress_markers = [
        "i apologize",
        "sorry",
        "first, i need to",
        "first i need to",
        "i need to ",
        "i will re-evaluate",
        "i need to re-evaluate",
        "i need to review",
        "i will review",
        "i need to generate",
        "i will generate",
        "i need to construct",
        "i will construct",
        "i need to determine",
        "i will determine",
        "i need to calculate",
        "i will calculate",
        "i need to analyze",
        "i will analyze",
        "let me",
        "the previous attempt",
        "did not follow the desired pattern",
    ]
    return any(marker in normalized for marker in meta_progress_markers)


def format_task_state_block(title: str, lines: list[str]) -> str:
    if not lines:
        return f"{title}:\n- None"
    return title + ":\n" + "\n".join(f"- {line}" for line in lines)


def build_task_state_summary(current_task: str, task_history: list[str], task_state: dict[str, Any]) -> str:
    recent_turn_context = [str(item) for item in task_state.get("recent_turn_context", [])]
    recent_inputs = [str(item) for item in task_state.get("recent_tool_inputs", [])]
    recent_outputs = [str(item) for item in task_state.get("recent_tool_outputs", [])]

    return (
        "Persistent task state. Use this to preserve continuity across turns.\n"
        f"Current task: {current_task}\n"
        "Use the recent tool inputs and outputs from this state to preserve continuity across turns.\n"
        f"{format_task_state_block('Recent turn context', recent_turn_context)}\n"
        f"{format_task_state_block('Recent tool inputs', recent_inputs)}\n"
        f"{format_task_state_block('Recent tool outputs', recent_outputs)}"
    )


def build_turn_messages(messages: list[dict[str, str]], task_state_summary: str) -> list[dict[str, str]]:
    if not messages:
        return [{"role": "system", "content": task_state_summary}]

    recent_messages = messages[1:]
    if len(recent_messages) > RECENT_MESSAGES_LIMIT:
        recent_messages = recent_messages[-RECENT_MESSAGES_LIMIT:]

    compact_recent_messages: list[dict[str, str]] = []
    for message in recent_messages:
        role = str(message.get("role", "user"))
        content = str(message.get("content", ""))
        compact_recent_messages.append(
            {
                "role": role,
                "content": compact_text(content, max_length=RECENT_MESSAGE_MAX_LENGTH),
            }
        )

    return [messages[0], {"role": "system", "content": task_state_summary}, *compact_recent_messages]


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

Your job is to complete the user's requested work as far as the available tools and approved steps allow. Be execution-oriented, not summary-oriented.

Return exactly one JSON object with these keys:
- thinking: a concise reasoning summary that is safe to display to the user
- action: the single next action for this turn

Rules:
- Output the keys in this exact order: thinking, action.
- Keep thinking brief and high-level. Do not include hidden chain-of-thought. Summarize only the next step or decision.
- thinking must be at most 240 characters and should usually be one sentence.
- Spend tokens on the action and the actual answer, not on thinking.
- The run loop is simple: decide one next action, observe the result, then decide again until the task is complete.
- The action object must always have exactly these keys in this order: type, tool, arguments, purpose, content.
- action.type must be either "tool" or "respond".
- If action.type is "tool", provide exactly one immediate tool call. Set tool to the tool name, arguments to the tool arguments object, purpose to the reason for the call, and content to the human-readable plan shown to the user before approval.
- For tool actions, arguments are machine-readable data for execution. Keep them structured and exact.
- For tool actions, content is user-facing approval text. Write it for a human, not for a parser.
- For tool actions, content should start with a concise summary of what you will do followed by the detailed plan. Use newlines and formatting for readability. Do not include raw JSON, dict dumps, filename dumps, or long argument lists in content; instead, summarize them and keep content concise.
- Do not paste JSON, dicts, arrays, raw arguments, or filename/path dumps into content.
- Do not rely on content to carry execution details that belong in arguments.
- When proposing a rename, checksum, or file-listing tool step, keep content concise and readable even if the arguments contain many items.
- Use pending-action wording in content, not completed-action wording. Describe what you plan to do next, not what has already happened.
- If action.type is "respond", set tool to an empty string, arguments to {{}}, purpose to an empty string, and content to the full user-facing answer for this turn.
- Never emit multiple tool calls in one response.
- Treat the user's request as a task to complete, not a topic to discuss.
- Prefer making concrete progress over giving commentary, summaries, caveats, or generic advice.
- Do not answer with meta-progress. Replies like "I need to...", "first I need to...", "I will...", "let me...", or "I should..." are not valid final answers.
- If you need a tool in order to make progress, your next action must be action.type="tool", not a respond action that describes the intended tool use.
- On the first turn, if the task requires inspecting files, computing checksums, renaming files, or otherwise gathering data with an available tool, propose the single next tool step immediately.
- After any correction request, rejection, or user guidance, do not apologize or describe a future plan in a respond action. Either provide the corrected concrete answer now or propose the next tool step.
- Never use a respond action just to say that you will re-evaluate, review, analyze, calculate, determine, construct, list, or rename next.
- Do not stop early with a summary if the task is still actionable and a tool step or concrete answer is still possible.
- If the task depends on facts from the workspace, files, or prior tool results, use the exact available evidence and continue step by step until the work is done.
- If more information is required and an available tool can get it, propose that tool step instead of asking the user to do the work manually.
- If you already have enough verified information to answer, give the concrete answer directly instead of summarizing possibilities.
- A response should represent the best concrete completion you can provide for this turn, not a partial status update disguised as an answer.
- Do not give placeholder examples, hypothetical filenames, invented outputs, or template-style answers unless the user explicitly asked for an example.
- Do not hedge with phrases like "for example", "hypothetically", "you could", or "here is a summary" when the task requires an actual result.
- Never claim that a tool has already run unless tool results are provided later in the conversation.
- Any tool call must be proposed first. The human will review and approve or reject the plan.
- Propose a tool call when you need more verified data to continue the task correctly.
- Keep tool calls minimal and relevant to the user's request.
- When file paths are needed, reuse only exact paths already present in the conversation or task state. Do not invent or normalize unseen filenames.
- When calling crc32 for many files from one listed directory, prefer using base_path plus the short entry paths returned by list_files instead of repeating long absolute paths.
- Only use the exact tool names shown below.
- If a previous plan was rejected, revise it to match the user's feedback.
- After tool results are provided, either produce a concrete respond action or propose the single next tool action.
- If a tool result is incomplete or shows an error, use that exact result to decide the next concrete step; do not ignore it and do not pretend the task is done.
- When a task involves transformation, selection, renaming, calculation, or lookup, prefer returning the exact result over describing the process.
- If your reply would mainly apologize, say you will re-evaluate, or describe what you plan to do next, that is not a final answer. Return either a tool action or the corrected concrete answer instead.
- Do not return blank output.

The current working directory is:
{working_directory}

Available tools:
{tool_descriptions}
""".strip()


def parse_agent_response(raw_content: str) -> dict[str, Any]:
    normalized = raw_content.strip()
    if not normalized:
        raise ValueError("Agent response was empty.")

    if normalized.startswith("```"):
        lines = normalized.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            normalized = "\n".join(lines[1:-1]).strip()
            if normalized.lower().startswith("json\n"):
                normalized = normalized[5:].strip()
        elif normalized.lower().startswith("```json"):
            normalized = normalized[7:].strip()

    start_index = normalized.find("{")
    end_index = normalized.rfind("}")
    if start_index >= 0 and end_index >= start_index:
        normalized = normalized[start_index : end_index + 1]

    try:
        data = json.loads(normalized)
    except json.JSONDecodeError as exc:
        preview = compact_text(normalized, max_length=INVALID_JSON_PREVIEW_MAX_LENGTH)
        raise ValueError(f"Agent response was not valid JSON: {preview}") from exc

    if not isinstance(data, dict):
        raise ValueError("Agent response was not a JSON object.")

    thinking = data.get("thinking", "")
    if not isinstance(thinking, str):
        raise ValueError("thinking must be a string.")
    thinking = thinking.strip()
    if not thinking:
        raise ValueError("thinking must not be empty.")
    if len(thinking) > MAX_VISIBLE_THINKING_CHARS:
        raise ValueError(f"thinking must be at most {MAX_VISIBLE_THINKING_CHARS} characters.")

    if "action" in data:
        action = data.get("action")
        if not isinstance(action, dict):
            raise ValueError("action must be an object.")

        action_type = action.get("type", "")
        tool = action.get("tool", "")
        arguments = action.get("arguments", {})
        purpose = action.get("purpose", "")
        content = action.get("content", "")

        if action_type not in {"tool", "respond"}:
            raise ValueError("action.type must be 'tool' or 'respond'.")
        if not isinstance(tool, str):
            raise ValueError("action.tool must be a string.")
        if not isinstance(arguments, dict):
            raise ValueError("action.arguments must be an object.")
        if not isinstance(purpose, str):
            raise ValueError("action.purpose must be a string.")
        if not isinstance(content, str):
            raise ValueError("action.content must be a string.")

        if action_type == "tool":
            if not tool.strip():
                raise ValueError("action.tool is required when action.type is 'tool'.")
            return {
                "thinking": thinking,
                "message": content.strip() or purpose.strip(),
                "tool_calls": [
                    {
                        "tool": tool.strip(),
                        "arguments": arguments,
                        "purpose": purpose.strip(),
                    }
                ],
                "response": "",
            }

        if not content.strip():
            raise ValueError("action.content is required when action.type is 'respond'.")
        return {
            "thinking": thinking,
            "message": "",
            "tool_calls": [],
            "response": content.strip(),
        }

    message = data.get("message", "")
    tool_calls = data.get("tool_calls", [])
    response = data.get("response", "")

    if not isinstance(message, str):
        raise ValueError("message must be a string.")
    if not isinstance(tool_calls, list):
        raise ValueError("tool_calls must be a list.")
    if not isinstance(response, str):
        raise ValueError("response must be a string.")

    return {
        "thinking": thinking,
        "message": message,
        "tool_calls": tool_calls,
        "response": response,
    }


def repair_agent_response(
    client: ollama.Client,
    model: str,
    raw_content: str,
    task_state_summary: str,
    num_ctx: int,
) -> dict[str, Any] | None:
    repair_prompt = (
        "The following model output did not follow the required response schema. Convert it into exactly one raw "
        "JSON object with these keys in this order: thinking, action. Preserve the original "
        "intent, but do not invent facts, paths, filenames, checksums, or tool results. If the malformed output is "
        "asking for or implying the next tool action, return action.type='tool' with exactly one immediate tool call. "
        "If it already contains a user-facing answer and no tool is needed, return action.type='respond'. For tool "
        "actions, keep action.arguments machine-readable and make action.content a human-readable approval plan in "
        "plain text rather than raw JSON or argument dumps. Return raw "
        "JSON only. The action object must have exactly these keys in this order: type, tool, arguments, purpose, "
        "content.\n\n"
        f"Task state:\n{task_state_summary}\n\n"
        "Malformed output to repair:\n"
        f"{raw_content}"
    )

    try:
        repaired = client.chat(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "You repair malformed agent replies into the required JSON schema.",
                },
                {
                    "role": "user",
                    "content": repair_prompt,
                },
            ],
            format=AGENT_RESPONSE_SCHEMA,
            options={"temperature": REPAIR_TEMPERATURE, "num_ctx": num_ctx, "num_predict": DEFAULT_NUM_PREDICT},
        )
    except httpx.HTTPError:
        return None
    except Exception:
        return None

    repaired_content = repaired.message.content if repaired.message and repaired.message.content else ""
    if not repaired_content.strip():
        return None

    try:
        return parse_agent_response(repaired_content)
    except ValueError:
        return None


def format_tool_call(registry: ToolRegistry, tool_call: dict[str, Any], index: int) -> str:
    tool_name = str(tool_call.get("tool", "unknown"))
    purpose = str(tool_call.get("purpose", "")).strip()
    arguments = tool_call.get("arguments", {})
    if not isinstance(arguments, dict):
        arguments = {}

    tool_definition = registry.get(tool_name)
    if tool_definition and tool_definition.format_arguments is not None:
        argument_lines = tool_definition.format_arguments(arguments)
    else:
        argument_lines = [json.dumps(arguments, ensure_ascii=True)]

    if not argument_lines:
        argument_lines = [json.dumps(arguments, ensure_ascii=True)]

    formatted_lines = [f"  {index}. {tool_name}: {argument_lines[0]}"]

    for detail in argument_lines[1:]:
        formatted_lines.append(f"     {detail}")

    if purpose:
        formatted_lines.append(f"     why: {purpose}")

    return "\n".join(formatted_lines)


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


def print_tool_plan(registry: ToolRegistry, tool_calls: list[dict[str, Any]]) -> None:
    print_section("Planned actions:", Fore.YELLOW)
    for index, tool_call in enumerate(tool_calls, start=1):
        print(color_text(format_tool_call(registry, tool_call, index), Fore.YELLOW))


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
    def format_arguments(arguments: dict[str, Any]) -> list[str]:
        target_path = str(arguments.get("path", ".")).strip() or "."
        recursive = bool(arguments.get("recursive", False))
        include_hidden = bool(arguments.get("include_hidden", False))
        mode = "recursive" if recursive else "non-recursive"
        hidden = "including hidden entries" if include_hidden else "excluding hidden entries"
        return [f"list files in {target_path}", f"mode: {mode}, {hidden}"]

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

            display_path = str(entry.relative_to(target_path))

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
        format_arguments=format_arguments,
    )


def make_crc32_tool(working_directory: Path) -> ToolDefinition:
    def format_arguments(arguments: dict[str, Any]) -> list[str]:
        paths = arguments.get("paths", [])
        base_path = str(arguments.get("base_path", "")).strip()
        if not isinstance(paths, list):
            paths = []

        lines = [f"compute CRC32 for {len(paths)} file(s)"]
        if base_path:
            lines.append(f"folder: {base_path}")

        for path in paths:
            path_text = str(path).strip()
            if path_text:
                lines.append(path_text)

        return lines

    def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        chunk_size = int(arguments.get("chunk_size", CRC32_DEFAULT_CHUNK_SIZE))
        base_path_raw = str(arguments.get("base_path", "")).strip()
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

        base_path = resolve_path(working_directory, base_path_raw) if base_path_raw else working_directory
        if base_path_raw:
            if not base_path.exists():
                raise FileNotFoundError(f"base_path does not exist: {base_path}")
            if not base_path.is_dir():
                raise NotADirectoryError(f"base_path is not a directory: {base_path}")

        resolved_paths: list[Path] = []
        total_bytes = 0
        for requested_path in normalized_paths:
            target_path = resolve_path(base_path, requested_path)
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
                "base_path": {
                    "type": "string",
                    "description": "Optional base directory used to resolve relative paths in paths.",
                },
                "paths": {
                    "type": "array",
                    "description": "Files to checksum. Relative paths are resolved from base_path when provided, otherwise from the working directory.",
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
        format_arguments=format_arguments,
    )


def make_rename_files_tool(working_directory: Path) -> ToolDefinition:
    def format_arguments(arguments: dict[str, Any]) -> list[str]:
        renames = arguments.get("renames", [])
        base_path = str(arguments.get("base_path", "")).strip()
        if not isinstance(renames, list):
            renames = []

        lines = [f"rename {len(renames)} item(s)"]
        if base_path:
            lines.append(f"folder: {base_path}")

        for rename in renames:
            if not isinstance(rename, dict):
                continue
            source = str(rename.get("from", "")).strip()
            destination = str(rename.get("to", "")).strip()
            if source and destination:
                lines.append(f"{source} -> {destination}")

        return lines

    def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        base_path_raw = str(arguments.get("base_path", "")).strip()
        raw_renames = arguments.get("renames", [])

        if not isinstance(raw_renames, list):
            raise ValueError("renames must be a list of rename operations")

        base_path = resolve_path(working_directory, base_path_raw) if base_path_raw else working_directory
        if base_path_raw:
            if not base_path.exists():
                raise FileNotFoundError(f"base_path does not exist: {base_path}")
            if not base_path.is_dir():
                raise NotADirectoryError(f"base_path is not a directory: {base_path}")

        planned_operations: list[dict[str, Any]] = []
        seen_sources: set[str] = set()
        seen_destinations: set[str] = set()

        for index, raw_rename in enumerate(raw_renames, start=1):
            if not isinstance(raw_rename, dict):
                raise ValueError(f"rename entry {index} must be an object")

            source_raw = str(raw_rename.get("from", "")).strip()
            destination_raw = str(raw_rename.get("to", "")).strip()

            if not source_raw:
                raise ValueError(f"rename entry {index} is missing 'from'")
            if not destination_raw:
                raise ValueError(f"rename entry {index} is missing 'to'")

            source_path = resolve_path(base_path, source_raw)
            destination_path = resolve_path(base_path, destination_raw)

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

            planned_operations.append(
                {
                    "from": source_path,
                    "to": destination_path,
                }
            )

        if not planned_operations:
            raise ValueError("renames is required")

        renamed: list[dict[str, Any]] = []
        for operation in planned_operations:
            source_path = operation["from"]
            destination_path = operation["to"]

            destination_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.rename(destination_path)

            try:
                display_from = str(source_path.relative_to(working_directory))
            except ValueError:
                display_from = str(source_path)

            try:
                display_to = str(destination_path.relative_to(working_directory))
            except ValueError:
                display_to = str(destination_path)

            renamed.append(
                {
                    "from": display_from,
                    "to": display_to,
                    "absolute_from": str(source_path),
                    "absolute_to": str(destination_path),
                }
            )

        return {
            "count": len(renamed),
            "renamed": renamed,
        }

    return ToolDefinition(
        name="rename_files",
        description="Rename one or more files or folders using explicit from/to pairs.",
        parameters_schema={
            "type": "object",
            "properties": {
                "base_path": {
                    "type": "string",
                    "description": "Optional base directory used to resolve relative from/to paths in renames.",
                },
                "renames": {
                    "type": "array",
                    "description": "List of rename operations to perform.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "from": {
                                "type": "string",
                                "description": "Existing source path to rename.",
                            },
                            "to": {
                                "type": "string",
                                "description": "Destination path after the rename.",
                            },
                        },
                        "required": ["from", "to"],
                        "additionalProperties": False,
                    },
                    "minItems": 1,
                },
            },
            "required": ["renames"],
            "additionalProperties": False,
        },
        handler=handler,
        format_arguments=format_arguments,
    )


def build_registry(working_directory: Path) -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(make_list_files_tool(working_directory))
    registry.register(make_crc32_tool(working_directory))
    registry.register(make_rename_files_tool(working_directory))
    return registry


def ask_for_approval(registry: ToolRegistry, tool_calls: list[dict[str, Any]]) -> tuple[str, str]:
    print_tool_plan(registry, tool_calls)
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

def ask_for_next_session_request() -> str | None:
    while True:
        next_request = input(
            color_text(
                "Next request? (type 'exit' or 'quit' to stop): ",
                Fore.CYAN,
                bright=True,
            )
        ).strip()

        if not next_request:
            continue
        if next_request.lower() in {"exit", "quit"}:
            return None
        return next_request


def request_agent_turn(
    client: ollama.Client,
    model: str,
    messages: list[dict[str, str]],
    task_state_summary: str,
    num_ctx: int,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    attempt_messages = build_turn_messages(messages, task_state_summary)

    for attempt_index in range(MODEL_TURN_MAX_ATTEMPTS):
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
                    options={"temperature": MODEL_TURN_TEMPERATURE, "num_ctx": num_ctx, "num_predict": DEFAULT_NUM_PREDICT},
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
            try:
                return parse_agent_response(raw_content), turn_metrics
            except ValueError as exc:
                repaired_response = repair_agent_response(client, model, raw_content, task_state_summary, num_ctx)
                if repaired_response is not None:
                    print(color_text("Recovered malformed model output into the required JSON schema.", Fore.YELLOW, bright=True))
                    return repaired_response, turn_metrics

                if attempt_index < MODEL_TURN_MAX_ATTEMPTS - 1:
                    print(color_text("Model returned output that did not match the required JSON schema.", Fore.YELLOW, bright=True))
                    print(color_text("Retrying with a stricter formatting reminder...", Fore.YELLOW, bright=True))
                    attempt_messages.append(
                        {
                            "role": "user",
                            "content": (
                                "Your previous reply did not follow the required output format. Return exactly one raw "
                                "JSON object with no markdown, no code fences, and no extra text before or after the "
                                "object. Do not write prose outside the JSON object. Use exactly this schema and these "
                                "keys in this order: "
                                '{"thinking":"...","action":{"type":"tool|respond","tool":"...","arguments":{},"purpose":"...","content":"..."}}. '
                                f"thinking must be at most {MAX_VISIBLE_THINKING_CHARS} characters. "
                                "If you need a tool, set action.type to 'tool' and propose exactly one immediate tool "
                                "call. Keep action.arguments machine-readable. Make action.content a human-readable "
                                "approval plan in plain text, not raw JSON or argument dumps. If no tool is needed, "
                                "set action.type to 'respond' and put the user-facing answer in action.content."
                            ),
                        }
                    )
                    continue
                raise RuntimeError("Model failed to return valid JSON after multiple retries.") from exc

        if attempt_index < MODEL_TURN_MAX_ATTEMPTS - 1:
            attempt_messages.append(
                {
                    "role": "user",
                    "content": (
                        "Your previous reply was blank. Return exactly one JSON object with the keys "
                        "thinking and action. Thinking must be a brief, user-visible summary rather than hidden "
                        f"chain-of-thought, and thinking must be at most {MAX_VISIBLE_THINKING_CHARS} characters. "
                        "The action object must choose exactly one next step: either one tool call "
                        "or one user-facing response. If you already have tool results, use them to answer or choose "
                        "the single next tool step."
                    ),
                }
            )

    raise ValueError("Model returned blank output multiple times in a row.")


def run_agent_task(
    client: ollama.Client,
    model: str,
    registry: ToolRegistry,
    prompt: str,
    working_directory: Path,
    max_steps: int,
    num_ctx: int,
) -> None:
    current_task = prompt
    task_history: list[str] = []
    task_state: dict[str, Any] = {
        "recent_turn_context": [],
        "recent_tool_inputs": [],
        "recent_tool_outputs": [],
    }
    messages: list[dict[str, str]] = [
        {"role": "system", "content": build_system_prompt(registry, working_directory)},
        {"role": "user", "content": prompt},
    ]
    while True:
        last_approved_plan_signature: str | None = None

        for step_index in range(max_steps):
            task_state_summary = build_task_state_summary(current_task, task_history, task_state)
            print_task_state_summary(task_state_summary)
            agent_response, _turn_metrics = request_agent_turn(client, model, messages, task_state_summary, num_ctx)
            tool_calls = agent_response["tool_calls"]

            if agent_response["thinking"].strip():
                append_task_history(task_history, f"Thinking: {compact_text(agent_response['thinking'])}")
                append_task_state_value(
                    task_state["recent_turn_context"],
                    "Thinking",
                    agent_response["thinking"],
                    max_entry_length=TURN_CONTEXT_THINKING_MAX_LENGTH,
                    max_entries=TURN_CONTEXT_MAX_ENTRIES,
                )
            if agent_response["message"].strip():
                append_task_history(task_history, f"Message: {compact_text(agent_response['message'])}")
                message_label = "Plan" if tool_calls else "Message"
                append_task_state_value(
                    task_state["recent_turn_context"],
                    message_label,
                    agent_response["message"],
                    max_entry_length=TURN_CONTEXT_MESSAGE_MAX_LENGTH,
                    max_entries=TURN_CONTEXT_MAX_ENTRIES,
                )
            if tool_calls:
                proposed_tools = ", ".join(str(tool_call.get("tool", "unknown")) for tool_call in tool_calls)
                append_task_history(task_history, f"Proposed tool step: {proposed_tools}")
                append_task_state_value(
                    task_state["recent_turn_context"],
                    "Proposed tool step",
                    proposed_tools,
                    max_entry_length=TURN_CONTEXT_TOOL_STEP_MAX_LENGTH,
                    max_entries=TURN_CONTEXT_MAX_ENTRIES,
                )

            if tool_calls:
                if len(tool_calls) > 1:
                    append_task_history(task_history, "Rejected multi-tool plan and requested a single next tool step")
                    messages.append({"role": "assistant", "content": json.dumps(agent_response)})
                    messages.append(
                        {
                            "role": "user",
                            "content": (
                                f"Current task: {current_task}\n\n"
                                "You proposed multiple tool calls in one response. Propose exactly one next tool "
                                "step only. Choose the single most useful immediate tool call and leave any later "
                                "steps for later turns."
                            ),
                        }
                    )
                    continue

                plan_signature = json.dumps(tool_calls, sort_keys=True)

                if plan_signature == last_approved_plan_signature:
                    messages.append({"role": "assistant", "content": json.dumps(agent_response)})
                    messages.append(
                        {
                            "role": "user",
                            "content": (
                                "You already executed this exact tool plan and already have its results. "
                                "Do not repeat it. Use the latest tool results to provide response unless "
                                "a different tool is strictly required."
                            ),
                        }
                    )
                    continue

                if agent_response["message"]:
                    print_section("Plan", Fore.YELLOW)
                    print(color_text(agent_response["message"], Fore.YELLOW))

                decision, feedback = ask_for_approval(registry, tool_calls)
                messages.append({"role": "assistant", "content": json.dumps(agent_response)})

                if decision == "cancel":
                    print(color_text("Cancelled by user.", Fore.RED, bright=True))
                    return

                if decision == "guide":
                    append_task_history(task_history, f"Tool guidance: {compact_text(feedback)}")
                    append_task_state_value(
                        task_state["recent_turn_context"],
                        "Tool guidance",
                        feedback,
                        max_entry_length=TURN_CONTEXT_GUIDANCE_MAX_LENGTH,
                        max_entries=TURN_CONTEXT_MAX_ENTRIES,
                    )
                    messages.append(
                        {
                            "role": "user",
                            "content": (
                                f"Current task: {current_task}\n\n"
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
                    append_task_state_value(
                        task_state["recent_turn_context"],
                        "Rejected plan feedback",
                        feedback,
                        max_entry_length=TURN_CONTEXT_GUIDANCE_MAX_LENGTH,
                        max_entries=TURN_CONTEXT_MAX_ENTRIES,
                    )

                    messages.append(
                        {
                            "role": "user",
                            "content": (
                                f"Current task: {current_task}\n\n"
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
                    append_task_state_value(
                        task_state["recent_tool_inputs"],
                        f"{tool_name} input",
                        arguments,
                    )
                    tool_results.append(
                        {
                            "tool": tool_name,
                            "purpose": tool_call.get("purpose", ""),
                            "execution": registry.execute(tool_name, arguments),
                        }
                    )

                for tool_result in tool_results:
                    execution = tool_result.get("execution", {})
                    if not isinstance(execution, dict):
                        continue
                    if execution.get("ok"):
                        append_task_state_value(
                            task_state["recent_tool_outputs"],
                            f"{tool_result.get('tool', 'unknown')} output",
                            execution.get("result", {}),
                        )
                    else:
                        append_task_state_value(
                            task_state["recent_tool_outputs"],
                            f"{tool_result.get('tool', 'unknown')} error",
                            execution.get("error", "Unknown tool error"),
                        )

                last_approved_plan_signature = plan_signature

                print_tool_results(tool_results)
                successful_tool_names = [
                    str(tool_result.get("tool", "unknown"))
                    for tool_result in tool_results
                    if isinstance(tool_result.get("execution"), dict) and tool_result["execution"].get("ok")
                ]
                failed_tool_names = [
                    str(tool_result.get("tool", "unknown"))
                    for tool_result in tool_results
                    if isinstance(tool_result.get("execution"), dict) and not tool_result["execution"].get("ok")
                ]
                if successful_tool_names:
                    append_task_history(task_history, f"Successful tools: {', '.join(successful_tool_names)}")
                if failed_tool_names:
                    append_task_history(task_history, f"Failed tools: {', '.join(failed_tool_names)}")
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            f"Current task: {current_task}\n\n"
                            "The approved tool step has been executed. Decide whether you can now answer or need "
                            "one next tool step. Use the exact recent tool outputs from task state as the source of "
                            "truth. Tool execution summary:\n"
                            f"{chr(10).join(summarize_tool_result(tool_result) for tool_result in tool_results)}"
                        ),
                    }
                )
                continue

            response_text = agent_response["response"].strip() or agent_response["message"].strip()
            if response_text:
                if task_state.get("recent_tool_outputs") and answer_looks_speculative(response_text):
                    append_task_history(task_history, "Rejected speculative answer that ignored available tool context")
                    messages.append({"role": "assistant", "content": json.dumps(agent_response)})
                    messages.append(
                        {
                            "role": "user",
                            "content": (
                                f"Current task: {current_task}\n\n"
                                "Your previous response was speculative or used placeholders even though recent tool "
                                "outputs are available in task state. Use the exact recent tool inputs/outputs from task "
                                "state, do not assume missing filenames, and either provide a concrete answer or propose "
                                "the next tool step needed."
                            ),
                        }
                    )
                    continue

                if answer_looks_like_meta_progress(response_text):
                    append_task_history(task_history, "Rejected meta-progress answer that should have been a plan or corrected result")
                    messages.append({"role": "assistant", "content": json.dumps(agent_response)})
                    if task_state.get("recent_tool_outputs"):
                        retry_prompt = (
                            f"Current task: {current_task}\n\n"
                            "Your previous reply was not a completion. Do not apologize or say that you will "
                            "re-evaluate, review, generate, analyze, or determine next. Either provide the corrected "
                            "concrete answer now using the exact recent tool inputs/outputs from task state, or "
                            "propose the single next tool step as a plan if more action is still required."
                        )
                    else:
                        retry_prompt = (
                            f"Current task: {current_task}\n\n"
                            "Your previous reply described what you need to do next instead of actually doing it. "
                            "If an available tool is needed, propose the single next tool step as a plan. If no tool "
                            "is needed, provide the concrete answer now. Do not answer with meta-progress such as "
                            "'I need to', 'I will', or 'first I need to'."
                        )

                    messages.append(
                        {
                            "role": "user",
                            "content": retry_prompt,
                        }
                    )
                    continue

                print_section("Answer", Fore.GREEN)
                print(color_text(response_text, Fore.GREEN))
                append_task_history(task_history, f"Answer: {compact_text(response_text)}")
                append_task_state_value(
                    task_state["recent_turn_context"],
                    "Answer",
                    response_text,
                    max_entry_length=TURN_CONTEXT_MESSAGE_MAX_LENGTH,
                    max_entries=TURN_CONTEXT_MAX_ENTRIES,
                )

                response_guidance = ask_for_turn_guidance(
                    "Guidance for this response? (Enter to accept, 'cancel' to stop): "
                )
                if response_guidance == "":
                    print(color_text("Cancelled by user.", Fore.RED, bright=True))
                    return
                if response_guidance is not None:
                    append_task_history(task_history, f"Response guidance: {compact_text(response_guidance)}")
                    append_task_state_value(
                        task_state["recent_turn_context"],
                        "Response guidance",
                        response_guidance,
                        max_entry_length=TURN_CONTEXT_GUIDANCE_MAX_LENGTH,
                        max_entries=TURN_CONTEXT_MAX_ENTRIES,
                    )
                    current_task = (
                        f"{current_task}\n\n"
                        f"Follow-up instruction: {response_guidance}"
                    )
                    messages.append({"role": "assistant", "content": json.dumps(agent_response)})
                    messages.append(
                        {
                            "role": "user",
                            "content": (
                                f"Current task: {current_task}\n\n"
                                "The user has given follow-up guidance for the same task. Treat this as a task "
                                "continuation, not as a request to restate the previous answer. If the follow-up "
                                "guidance asks you to perform an action and an available tool can do it, propose "
                                "the next tool step instead of giving another answer. Only respond directly if the "
                                "task is already complete and no tool step is needed. User guidance: "
                                f"{response_guidance}"
                            ),
                        }
                    )
                    continue

                messages.append({"role": "assistant", "content": json.dumps(agent_response)})
                next_request = ask_for_next_session_request()
                if next_request is None:
                    return

                current_task = next_request
                append_task_history(task_history, f"New user request: {compact_text(next_request)}")
                messages.append({"role": "user", "content": next_request})
                break

            messages.append(
                {
                    "role": "user",
                    "content": "Your last response was empty. Either provide a response or a tool plan.",
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
    num_ctx: int,
) -> None:
    print(color_text("Enter a task for the agent. Type 'exit' or 'quit' to stop.", Fore.CYAN, bright=True))
    prompt = input(color_text("\nTask> ", Fore.CYAN, bright=True)).strip()
    if not prompt:
        return
    if prompt.lower() in {"exit", "quit"}:
        return
    run_agent_task(client, model, registry, prompt, working_directory, max_steps, num_ctx)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Minimal Ollama-based tool agent with mandatory user approval before tool execution."
    )
    parser.add_argument("prompt", nargs="?", help="Task for the agent. If omitted, starts interactive mode.")
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL_NAME,
        help=f"Ollama model to use (default: {DEFAULT_MODEL_NAME}).",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_OLLAMA_HOST,
        help=f"Ollama server URL (default: {DEFAULT_OLLAMA_HOST}).",
    )
    parser.add_argument(
        "--working-directory",
        default=DEFAULT_WORKING_DIRECTORY,
        help="Base working directory for tool operations (default: current directory).",
    )
    parser.add_argument(
        "--max-steps",
        type=int,
        default=DEFAULT_MAX_STEPS,
        help=f"Maximum reasoning/tool rounds per task (default: {DEFAULT_MAX_STEPS}).",
    )
    parser.add_argument(
        "--read-timeout",
        type=float,
        default=DEFAULT_READ_TIMEOUT_SECONDS,
        help=f"Maximum idle seconds to wait for Ollama response stream data before timing out (default: {DEFAULT_READ_TIMEOUT_SECONDS:g}).",
    )
    parser.add_argument(
        "--num-ctx",
        type=int,
        default=DEFAULT_NUM_CTX,
        help=f"Context window to request from Ollama for each model call (default: {DEFAULT_NUM_CTX}).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    working_directory = Path(args.working_directory).resolve()
    registry = build_registry(working_directory)
    timeout = httpx.Timeout(
        connect=HTTP_CONNECT_TIMEOUT_SECONDS,
        read=args.read_timeout,
        write=HTTP_WRITE_TIMEOUT_SECONDS,
        pool=HTTP_POOL_TIMEOUT_SECONDS,
    )
    client = ollama.Client(host=args.host, timeout=timeout)

    try:
        if args.prompt:
            run_agent_task(client, args.model, registry, args.prompt, working_directory, args.max_steps, args.num_ctx)
            return

        interactive_repl(client, args.model, registry, working_directory, args.max_steps, args.num_ctx)
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
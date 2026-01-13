"""Command-line tool to manage metadata provider caches.

Supported operations:
- refresh: invalidate cache and reload data
- invalidate: mark cache stale without reloading
- set-expiry: adjust cache TTL via absolute date or relative days/weeks/months
- status: print cache configuration for providers

Windows-friendly; no special dependencies beyond existing providers.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import math
from datetime import datetime
from typing import Dict, Iterable, List

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    HAS_COLORAMA = True
except ImportError:
    HAS_COLORAMA = False
    # Fallback ANSI codes for systems without colorama
    class Fore:
        CYAN = '\033[36m'
        GREEN = '\033[32m'
        YELLOW = '\033[33m'
        WHITE = '\033[37m'
    class Style:
        RESET_ALL = '\033[0m'
        BRIGHT = '\033[1m'

from anime_metadata import AnimeDataProvider
from imdb_metadata import IMDbDataProvider

ProviderMap = Dict[str, object]

# Global color flag
_use_color = True


def _colorize(text: str, color: str) -> str:
    """Apply color to text if coloring is enabled."""
    if not _use_color:
        return text
    return f"{color}{text}{Style.RESET_ALL}"


def _colorize_value(value: str) -> str:
    """Colorize a value (muted green)."""
    return _colorize(value, Fore.GREEN)


def _colorize_provider(name: str) -> str:
    """Colorize provider name (muted cyan)."""
    return _colorize(name, Fore.CYAN)


def _build_providers() -> ProviderMap:
    return {
        "imdb": IMDbDataProvider(),
        "anime": AnimeDataProvider(),
    }


def _select_providers(all_providers: ProviderMap, selection: List[str]) -> Iterable[object]:
    if "all" in selection:
        return all_providers.values()
    return (all_providers[name] for name in selection if name in all_providers)


def _parse_relative_text(text: str) -> int:
    """Parse free-text duration like "2 months and 3 days" or short form like "7d", "2m7d" into total days."""
    units = {"day": 0, "week": 0, "month": 0}
    
    # Single regex for both short form (7d, 2m, 2m7d) and long form (3 days, 2 months)
    for value, unit in re.findall(r"(\d+)\s*([dwm]|days?|weeks?|months?)", text.lower()):
        n = int(value)
        unit_lower = unit.lower()
        
        # Map short and long forms to unit keys
        if unit_lower in ('d', 'day', 'days'):
            units["day"] += n
        elif unit_lower in ('w', 'week', 'weeks'):
            units["week"] += n
        elif unit_lower in ('m', 'month', 'months'):
            units["month"] += n
    
    if not any(units.values()):
        raise SystemExit("Could not parse relative duration. Examples: '7d', '2m', '2m7d' or '3 days', '2 months and 3 days'.")
    return units["day"] + units["week"] * 7 + units["month"] * 30


def _parse_expiry_args(args: argparse.Namespace) -> int:
    """Return desired TTL in whole days."""
    if args.date:
        try:
            expires_at = datetime.fromisoformat(args.date)
        except ValueError as exc:
            raise SystemExit(f"Invalid date format for --date: {exc}")
        delta = expires_at - datetime.now()
        days = math.ceil(delta.total_seconds() / 86400)
        if days <= 0:
            raise SystemExit("Expiry date must be in the future")
        return days

    relative_text = args.relative or " ".join(args.relative_positional or []).strip()
    if relative_text:
        rel = _parse_relative_text(relative_text)
        return rel

    raise SystemExit("Specify either --date or --in '<duration>' (e.g. '3 days' or '2 months and 1 week')")


def _format_timedelta(seconds: float) -> str:
    days, rem = divmod(int(seconds), 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes or not parts:
        parts.append(f"{minutes}m")
    return " ".join(parts)


def _last_modified(path: str) -> str:
    try:
        ts = os.path.getmtime(path)
    except OSError:
        return "n/a"
    return datetime.fromtimestamp(ts).isoformat(timespec="seconds")


def cmd_status(providers: Iterable[object]) -> None:
    for provider in providers:
        name = provider.__class__.__name__
        summary = provider.cache_summary()
        ttl_seconds = summary["cache_duration_seconds"]
        expiry = summary["cache_expiry"].isoformat(timespec="seconds")
        cache_dir = summary["cache_dir"]
        db_path = getattr(provider, "_db_path", None)
        last_mtime = _last_modified(db_path) if db_path else "n/a"
        
        provider_name = _colorize_provider(name)
        print(f"[{provider_name}] cache_dir={_colorize_value(cache_dir)}")
        print(f"  ttl={_colorize_value(_format_timedelta(ttl_seconds))} expires_at={_colorize_value(expiry)}")
        print(f"  data_file={_colorize_value(db_path or 'n/a')} last_modified={_colorize_value(last_mtime)}")


def cmd_refresh(providers: Iterable[object]) -> None:
    for provider in providers:
        name = provider.__class__.__name__
        logging.info("Refreshing %s", name)
        provider.refresh_data()
        logging.info("Refreshed %s", name)


def cmd_invalidate(providers: Iterable[object]) -> None:
    for provider in providers:
        name = provider.__class__.__name__
        logging.info("Invalidating %s", name)
        provider.invalidate_cache()
        # Set TTL to 0 days so cache is immediately stale
        provider.set_cache_expiry(0)
        logging.info("Invalidated %s", name)


def cmd_set_expiry(providers: Iterable[object], days: int) -> None:
    for provider in providers:
        name = provider.__class__.__name__
        try:
            new_expiry = provider.set_cache_expiry(days)
        except ValueError as exc:
            raise SystemExit(f"[{_colorize_provider(name)}] {exc}")
        logging.info("Updated %s cache expiry to %s", name, new_expiry.isoformat(timespec="seconds"))
        ttl_str = _format_timedelta(provider.cache_duration.total_seconds())
        print(f"[{_colorize_provider(name)}] new expiry at {_colorize_value(new_expiry.isoformat(timespec='seconds'))} (ttl {_colorize_value(ttl_str)})")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Metadata cache manager")
    parser.add_argument("--provider", "-p", nargs="+", default=["all"], choices=["imdb", "anime", "all"], help="Target provider(s)")
    parser.add_argument("--verbose", "-v", action="count", default=0, help="Increase verbosity")
    parser.add_argument("--no-color", action="store_true", help="Disable colored output")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="Show cache configuration")
    sub.add_parser("refresh", help="Invalidate then reload cache")
    sub.add_parser("invalidate", help="Mark cache stale without reload")

    expiry = sub.add_parser("set-expiry", help="Adjust cache expiry")
    expiry.add_argument("--date", help="Absolute expiry (ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM[:SS])")
    expiry.add_argument("--in", dest="relative", help="Relative duration (e.g. '3 days', '2 months and 1 week')")
    expiry.add_argument("relative_positional", nargs="*", help="Relative duration without flag (e.g. '14 days')")

    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    log_level = logging.WARNING
    if args.verbose == 1:
        log_level = logging.INFO
    elif args.verbose >= 2:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")

    # Set global color flag
    global _use_color
    _use_color = not args.no_color

    providers_map = _build_providers()
    selected = list(_select_providers(providers_map, args.provider))

    if not selected:
        parser.error("No providers matched selection")

    if args.command == "status":
        cmd_status(selected)
    elif args.command == "refresh":
        cmd_refresh(selected)
    elif args.command == "invalidate":
        cmd_invalidate(selected)
    elif args.command == "set-expiry":
        days = _parse_expiry_args(args)
        cmd_set_expiry(selected, days)
    else:
        parser.error(f"Unknown command: {args.command}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

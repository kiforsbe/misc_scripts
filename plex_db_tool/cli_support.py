import importlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .models import (
    PlannedMutation,
    PlexAccount,
    PlexLibrarySection,
    PlexPlaylist,
    TableColumnSpec,
)


class PlexCliSupport:
    DRY_RUN_FILTER_MODES = {"all", "warnings", "errors"}
    _questionary_module = None
    _questionary_checked = False
    _questionary_warning_shown = False
    PLAYLIST_ROW_COLUMNS = (
        "playlist_id",
        "source_playlist",
        "target_playlist",
        "source_added_at",
        "target_added_at",
        "status",
        "action",
        "source_item_count",
        "matched_item_count",
        "transfer_item_count",
        "existing_item_count",
        "unmatched_item_count",
        "notes",
        "unmatched_items",
    )
    PLAYLIST_TABLE_COLUMNS = (
        TableColumnSpec("playlist_id"),
        TableColumnSpec("source_playlist"),
        TableColumnSpec("target_playlist"),
        TableColumnSpec("source_added_at"),
        TableColumnSpec("target_added_at"),
        TableColumnSpec("status"),
        TableColumnSpec("action"),
        TableColumnSpec("matched_item_count"),
        TableColumnSpec("transfer_item_count"),
        TableColumnSpec("unmatched_item_count"),
        TableColumnSpec("notes"),
    )
    PLAYLIST_LIST_ROW_COLUMNS = (
        "playlist_id",
        "source_playlist",
        "account_id",
        "source_item_count",
        "status",
        "notes",
    )
    PLAYLIST_LIST_TABLE_COLUMNS = (
        TableColumnSpec("playlist_id"),
        TableColumnSpec("source_playlist"),
        TableColumnSpec("account_id"),
        TableColumnSpec("source_item_count"),
        TableColumnSpec("status"),
        TableColumnSpec("notes"),
    )

    @staticmethod
    def load_questionary_module():
        if not PlexCliSupport._questionary_checked:
            PlexCliSupport._questionary_checked = True
            try:
                PlexCliSupport._questionary_module = importlib.import_module("questionary")
            except Exception:
                PlexCliSupport._questionary_module = None
        return PlexCliSupport._questionary_module

    @classmethod
    def maybe_warn_questionary_unavailable(cls) -> None:
        if cls.load_questionary_module() is not None or cls._questionary_warning_shown:
            return
        cls._questionary_warning_shown = True
        print(
            "questionary is not installed; using plain text prompts instead of the interactive selector."
        )

    @staticmethod
    def require_questionary_selection(selected: Any) -> Any:
        if selected is None:
            raise KeyboardInterrupt
        return selected

    @classmethod
    def prompt_questionary_checkbox(
        cls,
        prompt: str,
        choices: Sequence[Any],
        instruction: Optional[str] = None,
        selection_prompt: str = "Selection",
    ) -> Optional[List[Any]]:
        questionary = cls.load_questionary_module()
        if questionary is None:
            return None

        print(prompt)
        prompt_kwargs: Dict[str, Any] = {
            "choices": list(choices),
        }
        if instruction is not None:
            prompt_kwargs["instruction"] = instruction
        selected = questionary.checkbox(selection_prompt, **prompt_kwargs).ask()
        return list(cls.require_questionary_selection(selected))

    @classmethod
    def prompt_questionary_select(
        cls,
        prompt: str,
        choices: Sequence[Any],
        default: Optional[str] = None,
        selection_prompt: str = "Selection",
    ) -> Optional[Any]:
        questionary = cls.load_questionary_module()
        if questionary is None:
            return None

        print(prompt)
        selected = questionary.select(selection_prompt, choices=list(choices), default=default).ask()
        return cls.require_questionary_selection(selected)

    @staticmethod
    def prompt_with_default(prompt: str, default: Optional[str] = None) -> str:
        while True:
            suffix = f" [{default}]" if default not in (None, "") else ""
            value = input(f"{prompt}{suffix}: ").strip()
            if value:
                return value
            if default not in (None, ""):
                return str(default)
            print("A value is required.")

    @classmethod
    def prompt_int_with_default(cls, prompt: str, default: Optional[int] = None) -> int:
        while True:
            raw_value = cls.prompt_with_default(prompt, None if default is None else str(default))
            try:
                return int(raw_value)
            except ValueError:
                print("Enter a whole number.")

    @staticmethod
    def describe_account(account: PlexAccount) -> str:
        if account.name:
            return f"{account.id}: {account.name}"
        return f"{account.id}: (unnamed account)"

    @staticmethod
    def resolve_account_prompt_default(
        accounts: Sequence[PlexAccount],
        default: Optional[int] = None,
    ) -> Optional[int]:
        valid_ids = {account.id for account in accounts}
        if default is not None and default in valid_ids:
            return default

        named_accounts = [account for account in accounts if account.name]
        if len(named_accounts) == 1:
            return named_accounts[0].id
        return None

    @classmethod
    def prompt_account_id(
        cls,
        prompt: str,
        accounts: Sequence[PlexAccount],
        default: Optional[int] = None,
    ) -> int:
        valid_ids = {account.id for account in accounts}
        if not valid_ids:
            raise RuntimeError("No Plex accounts were found in the selected database.")

        resolved_default = cls.resolve_account_prompt_default(accounts, default)

        questionary = cls.load_questionary_module()
        if questionary is not None:
            choices = [
                questionary.Choice(
                    title=cls.describe_account(account),
                    value=account.id,
                )
                for account in accounts
            ]
            selected = cls.prompt_questionary_select(
                prompt,
                choices,
                default=resolved_default,
                selection_prompt="Account",
            )
            if selected is not None:
                return int(selected)

        print(prompt)
        for account in accounts:
            print(f"  {cls.describe_account(account)}")

        if default is not None and resolved_default is None:
            print(f"Default account id {default} is not present in this database.")

        while True:
            selected_id = cls.prompt_int_with_default("Choose account id", resolved_default)
            if selected_id in valid_ids:
                return selected_id
            print("Choose one of the listed account ids.")

    @classmethod
    def infer_interactive_account_defaults(
        cls,
        source_accounts: Sequence[PlexAccount],
        target_accounts: Sequence[PlexAccount],
        source_default: Optional[int],
        target_default: Optional[int],
    ) -> Tuple[Optional[int], Optional[int]]:
        def named_account_map(accounts: Sequence[PlexAccount]) -> Dict[str, PlexAccount]:
            return {
                account.name.casefold(): account
                for account in accounts
                if account.name
            }

        resolved_source_default = source_default
        resolved_target_default = target_default

        source_named = named_account_map(source_accounts)
        target_named = named_account_map(target_accounts)
        shared_names = sorted(set(source_named) & set(target_named))
        if len(shared_names) == 1:
            shared_name = shared_names[0]
            if resolved_source_default is None:
                resolved_source_default = source_named[shared_name].id
            if resolved_target_default is None:
                resolved_target_default = target_named[shared_name].id

        if resolved_source_default is None and len(source_named) == 1:
            resolved_source_default = next(iter(source_named.values())).id
        if resolved_target_default is None and len(target_named) == 1:
            resolved_target_default = next(iter(target_named.values())).id

        resolved_source_default = cls.resolve_account_prompt_default(source_accounts, resolved_source_default)
        resolved_target_default = cls.resolve_account_prompt_default(target_accounts, resolved_target_default)

        return resolved_source_default, resolved_target_default

    @staticmethod
    def describe_library_section(section: PlexLibrarySection) -> str:
        if section.name:
            return f"{section.id}: {section.name}"
        return f"{section.id}: (unnamed library)"

    @classmethod
    def prompt_library_filters(
        cls,
        prompt: str,
        libraries: Sequence[PlexLibrarySection],
        default_names: Sequence[str],
    ) -> List[str]:
        library_by_id = {library.id: library for library in libraries}
        library_by_name = {library.name.casefold(): library for library in libraries if library.name}

        invalid_defaults = [name for name in default_names if name.casefold() not in library_by_name]
        if invalid_defaults:
            print(
                "Ignoring default libraries not present in this database: "
                + ", ".join(invalid_defaults)
            )
            default_names = [name for name in default_names if name.casefold() in library_by_name]

        default_values = list(default_names) if default_names else [
            library.name
            for library in libraries
            if library.name
        ]
        default_value_set = set(default_values)

        questionary = cls.load_questionary_module()
        if questionary is not None:
            choices = [
                questionary.Choice(
                    title=f"{library.id}: {library.name}",
                    value=library.name,
                    checked=library.name in default_value_set,
                )
                for library in libraries
            ]
            return cls.prompt_questionary_checkbox(
                prompt,
                choices=choices,
                instruction="Space to toggle, Enter to confirm, Esc to cancel",
                selection_prompt="Libraries",
            )

        cls.maybe_warn_questionary_unavailable()

        print(prompt)
        for library in libraries:
            print(f"  {cls.describe_library_section(library)}")

        default_label = ", ".join(default_names) if default_names else "all"
        prompt_suffix = f" [{default_label}]" if default_label else ""
        while True:
            raw_value = input(
                f"Choose library ids or names (comma-separated, Enter for all libraries){prompt_suffix}: "
            ).strip()
            if not raw_value:
                return list(default_names)

            selections: List[str] = []
            seen = set()
            valid = True
            for token in (part.strip() for part in raw_value.split(",")):
                if not token:
                    continue
                library: Optional[PlexLibrarySection] = None
                if token.isdigit():
                    library = library_by_id.get(int(token))
                if library is None:
                    library = library_by_name.get(token.casefold())
                if library is None:
                    print(f"Choose only listed libraries. Invalid selection: {token}")
                    valid = False
                    break
                if library.name not in seen:
                    seen.add(library.name)
                    selections.append(library.name)
            if valid:
                return selections

    @staticmethod
    def prompt_yes_no(prompt: str, default: bool = False) -> bool:
        default_hint = "[Y/n]" if default else "[y/N]"
        while True:
            value = input(f"{prompt} {default_hint}: ").strip().casefold()
            if not value:
                return default
            if value in {"y", "yes"}:
                return True
            if value in {"n", "no"}:
                return False
            print("Answer yes or no.")

    @classmethod
    def prompt_choice(
        cls,
        prompt: str,
        choices: Sequence[str],
        default: Optional[str] = None,
    ) -> str:
        valid = {choice.casefold(): choice for choice in choices}
        selected = cls.prompt_questionary_select(prompt, choices, default, selection_prompt="Conflict behavior")
        if selected is not None:
            return str(selected)

        cls.maybe_warn_questionary_unavailable()

        while True:
            rendered_choices = "/".join(choices)
            raw_value = cls.prompt_with_default(f"{prompt} ({rendered_choices})", default)
            selected = valid.get(raw_value.casefold())
            if selected is not None:
                return selected
            print("Choose one of the listed values.")

    @classmethod
    def prompt_playlist_filters(
        cls,
        prompt: str,
        playlists: Sequence[PlexPlaylist],
        default_selectors: Sequence[str],
        include_empty_playlists: bool,
    ) -> List[str]:
        playlist_by_id = {str(playlist.id): playlist for playlist in playlists}
        playlist_by_name = {playlist.name.casefold(): playlist for playlist in playlists}

        default_values: List[str] = []
        if default_selectors:
            default_values = list(default_selectors)
        else:
            default_values = [
                str(playlist.id)
                for playlist in playlists
                if include_empty_playlists or not playlist.is_empty_in_scope
            ]
        default_value_set = set(default_values)

        questionary = cls.load_questionary_module()
        if questionary is not None:
            choices = []
            for playlist in playlists:
                status = "empty" if playlist.is_empty_in_scope else f"{len(playlist.scoped_items)} in scope"
                title = f"{playlist.id}: {playlist.name} ({status})"
                choices.append(
                    questionary.Choice(
                        title=title,
                        value=str(playlist.id),
                        checked=str(playlist.id) in default_value_set,
                    )
                )
            return cls.prompt_questionary_checkbox(prompt, choices, selection_prompt="Playlists")

        cls.maybe_warn_questionary_unavailable()

        print(prompt)
        for playlist in playlists:
            status = "empty" if playlist.is_empty_in_scope else f"{len(playlist.scoped_items)} in scope"
            print(f"  {playlist.id}: {playlist.name} ({status})")

        default_label = ", ".join(default_values) if default_values else "none"
        while True:
            raw_value = input(
                f"Choose playlist ids or exact names (comma-separated, Enter for default selection) [{default_label}]: "
            ).strip()
            if not raw_value:
                return list(default_values)

            selections: List[str] = []
            seen = set()
            valid = True
            for token in (part.strip() for part in raw_value.split(",")):
                if not token:
                    continue
                playlist = playlist_by_id.get(token) or playlist_by_name.get(token.casefold())
                if playlist is None:
                    print(f"Choose only listed playlists. Invalid selection: {token}")
                    valid = False
                    break
                playlist_id = str(playlist.id)
                if playlist_id not in seen:
                    seen.add(playlist_id)
                    selections.append(playlist_id)
            if valid:
                return selections

    @staticmethod
    def apply_planned_mutations(target_db_path: Path, mutations: Sequence[PlannedMutation]) -> None:
        from .infrastructure import PlexDatabase, PlexEnvironment

        PlexEnvironment.wait_for_plex_shutdown()
        database = PlexDatabase(target_db_path, readonly=False)
        try:
            database.begin_immediate()
            database.apply_mutations(mutations)
            database.commit()
        finally:
            database.close()
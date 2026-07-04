from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Literal, Sequence

from .codegen import generate_client
from .model_inspector import ModelInfo, inspect_models
from .push import db_push
from .push.base import ExistingColumn, SchemaDiff, SchemaPlan
from .runtime.datasource import open_sqlite_connection


DEFAULT_MODEL_FILE = "model.py"
GENERATED_CLIENT_FILENAME = "client.py"

GenerateTarget = Literal["model-dir", "package"]
ConfirmRebuildMode = Literal["auto", "prompt"]
ConfirmCallback = Callable[
    [ModelInfo, SchemaPlan, tuple[ExistingColumn, ...] | None, SchemaDiff],
    bool,
]


def _module_name_from_path(module_path: Path) -> str:
    candidate = module_path.with_suffix("")
    cwd = Path.cwd()
    try:
        rel = candidate.relative_to(cwd)
        parts = rel.parts
    except ValueError:
        parts = candidate.parts
    trimmed_parts = list(parts)

    if not trimmed_parts:
        trimmed_parts = [module_path.stem]

    sanitized_parts = [re.sub(r"[^0-9a-zA-Z_]+", "_", part) or "_" for part in trimmed_parts]
    return ".".join(sanitized_parts)

def load_module(module_path: Path) -> ModuleType:
    module_path = module_path.resolve()
    if not module_path.exists():
        raise FileNotFoundError(f"Model file '{module_path}' does not exist")
    module_name = _module_name_from_path(module_path)
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from '{module_path}'")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    original_sys_path = list(sys.path)
    search_paths = [str(module_path.parent)]
    cwd = str(Path.cwd())
    if cwd not in search_paths:
        search_paths.append(cwd)
    for path in reversed(search_paths):
        sys.path.insert(0, path)
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = original_sys_path
    return module


def _find_package_directory() -> Path:
    spec = importlib.util.find_spec("dclassql")
    if spec is None or not spec.submodule_search_locations:
        raise RuntimeError("Cannot locate installed dclassql package to write generated client")
    return Path(next(iter(spec.submodule_search_locations))).resolve()


def resolve_client_package_name(module_path: Path) -> str:
    stem = re.sub(r"[^0-9a-zA-Z_]+", "_", module_path.stem) or "_"
    return f"{stem}_client"


def resolve_client_class_name(module_path: Path) -> str:
    package_name = resolve_client_package_name(module_path)
    return "".join(part.capitalize() for part in package_name.split("_") if part)


def resolve_generated_package_dir(module_path: Path, target: GenerateTarget = "model-dir") -> Path:
    package_name = resolve_client_package_name(module_path)
    if target == "model-dir":
        return module_path.resolve().parent / package_name
    return _find_package_directory() / package_name


def collect_models(module: ModuleType) -> list[type[Any]]:
    from dataclasses import is_dataclass

    models: list[type[Any]] = []
    for value in vars(module).values():
        if isinstance(value, type) and is_dataclass(value) and value.__module__ == module.__name__:
            models.append(value)
    if not models:
        raise ValueError("No dataclass models were found in the provided module")
    return models


def _describe_schema_diff(info: ModelInfo, diff: SchemaDiff) -> str:
    datasource_key = getattr(info.datasource, "key", None)
    prefix = f"[{datasource_key}] " if datasource_key else ""
    parts: list[str] = [f"{prefix}模型 {info.model.__name__} 需要重建表"]
    if diff.added:
        added = ", ".join(f"+{column.name}:{column.type_sql}" for column in diff.added)
        parts.append(f"新增列: {added}")
    if diff.removed:
        removed = ", ".join(f"-{column.name}:{column.type_sql}" for column in diff.removed)
        parts.append(f"删除列: {removed}")
    if diff.changed:
        changed = ", ".join(
            f"~{change.name}({'; '.join(change.reasons)})" for change in diff.changed
        )
        parts.append(f"变更列: {changed}")
    return "; ".join(parts)


def _build_confirm_callback(mode: ConfirmRebuildMode) -> ConfirmCallback:
    def confirm(
        info: ModelInfo,
        _plan: SchemaPlan,
        _existing: tuple[ExistingColumn, ...] | None,
        diff: SchemaDiff,
    ) -> bool:
        summary = _describe_schema_diff(info, diff)
        sys.stdout.write(summary + "\n")
        if mode == "auto":
            sys.stdout.write("已根据 --confirm-rebuild=auto 自动确认。\n")
            return True
        while True:
            response = input("确认重建该表? [y/N]: ").strip().lower()
            if response in {"y", "yes"}:
                return True
            if response in {"", "n", "no"}:
                return False
            sys.stdout.write("请输入 y 或 n。\n")

    return confirm


def push_database(
    models: Sequence[type[Any]],
    *,
    sync_indexes: bool = False,
    confirm_mode: ConfirmRebuildMode | None = None,
) -> None:
    model_infos = inspect_models(models)
    connections: dict[str, Any] = {}
    opened: list[Any] = []
    confirm_callback = _build_confirm_callback(confirm_mode) if confirm_mode else None
    try:
        for info in model_infos.values():
            config = info.datasource
            key = config.key
            if key in connections:
                continue
            if config.provider != "sqlite":
                raise ValueError(f"Unsupported provider '{config.provider}'")
            connection = open_sqlite_connection(config.url)
            connections[key] = connection
            opened.append(connection)
        db_push(
            models,
            connections,
            sync_indexes=sync_indexes,
            confirm_rebuild=confirm_callback,
        )
    finally:
        for conn in opened:
            try:
                conn.close()
            except Exception:
                pass


def command_generate(module_path: Path, *, target: GenerateTarget = "model-dir") -> None:
    module = load_module(module_path)
    models = collect_models(module)
    client_class_name = resolve_client_class_name(module_path)
    generated = generate_client(models, client_class_name=client_class_name)
    output_dir = resolve_generated_package_dir(module_path, target)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "__init__.py").write_text(generated.init_code, encoding="utf-8")
    (output_dir / "__init__.pyi").write_text(generated.init_stub, encoding="utf-8")
    (output_dir / GENERATED_CLIENT_FILENAME).write_text(generated.code, encoding="utf-8")
    (output_dir / "asdict.pyi").write_text(generated.asdict_stub, encoding="utf-8")
    sys.stdout.write(f"Client package written to {output_dir}\n")


def command_push_db(
    module_path: Path,
    *,
    sync_indexes: bool,
    confirm_mode: ConfirmRebuildMode | None,
) -> None:
    module = load_module(module_path)
    models = collect_models(module)
    push_database(models, sync_indexes=sync_indexes, confirm_mode=confirm_mode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="typed-db", description="Typed DB utilities.")
    parser.add_argument(
        "-m",
        "--module",
        type=Path,
        default=Path(DEFAULT_MODEL_FILE),
        help="Path to the model module file (default: model.py)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate", help="Generate client code for given models")
    generate_parser.add_argument(
        "--target",
        choices=("model-dir", "package"),
        default="model-dir",
        help="生成 client 的位置: model-dir 写到模型文件同目录; package 写到 dclassql 包内",
    )
    generate_parser.set_defaults(handler=lambda args: command_generate(args.module, target=args.target))

    push_parser = subparsers.add_parser("push-db", help="Apply schema and indexes to configured databases")
    push_parser.add_argument(
        "--sync-indexes",
        action="store_true",
        help="Drop extra indexes and create missing ones to match model definitions",
    )
    push_parser.add_argument(
        "--confirm-rebuild",
        choices=("auto", "prompt"),
        default=None,
        help="auto: 自动确认所有重建; prompt: 交互式逐表确认",
    )
    push_parser.set_defaults(
        handler=lambda args: command_push_db(
            args.module,
            sync_indexes=args.sync_indexes,
            confirm_mode=args.confirm_rebuild,
        )
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 1
    try:
        handler(args)
        return 0
    except Exception:  # pragma: no cover - CLI error reporting
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

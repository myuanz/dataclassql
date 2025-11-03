from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Sequence

from .codegen import generate_client
from .push import db_push
from .runtime.datasource import open_sqlite_connection, resolve_sqlite_path


DEFAULT_MODEL_FILE = "model.py"


def load_module(module_path: Path) -> ModuleType:
    module_path = module_path.resolve()
    if not module_path.exists():
        raise FileNotFoundError(f"Model file '{module_path}' does not exist")
    module_name = module_path.stem
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from '{module_path}'")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    sys.path.insert(0, str(module_path.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


def collect_models(module: ModuleType) -> list[type[Any]]:
    from dataclasses import is_dataclass

    models: list[type[Any]] = []
    for value in vars(module).values():
        if isinstance(value, type) and is_dataclass(value) and value.__module__ == module.__name__:
            models.append(value)
    if not models:
        raise ValueError("No dataclass models were found in the provided module")
    return models


def push_database(models: Sequence[type[Any]]) -> None:
    from .model_inspector import inspect_models

    model_infos = inspect_models(models)
    connections: dict[str, Any] = {}
    opened: list[Any] = []
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
        db_push(models, connections)
    finally:
        for conn in opened:
            try:
                conn.close()
            except Exception:
                pass


def command_generate(module_path: Path) -> None:
    module = load_module(module_path)
    models = collect_models(module)
    generated = generate_client(models)
    sys.stdout.write(generated.code)


def command_push_db(module_path: Path) -> None:
    module = load_module(module_path)
    models = collect_models(module)
    push_database(models)


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
    generate_parser.set_defaults(handler=lambda args: command_generate(args.module))

    push_parser = subparsers.add_parser("push-db", help="Apply schema and indexes to configured databases")
    push_parser.set_defaults(handler=lambda args: command_push_db(args.module))

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
    except Exception as exc:  # pragma: no cover - CLI error reporting
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

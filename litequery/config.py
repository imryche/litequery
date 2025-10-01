import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def get_database_path() -> Path:
    db_path = os.getenv("DATABASE_PATH")
    if not db_path:
        raise ValueError("DATABASE_PATH environment variable not set")
    return Path(db_path).resolve()


def _find_nearest_dir(name: str, start_dir: Path, stop_dir: Path) -> Path | None:
    current = start_dir
    while True:
        candidate = current / name
        if candidate.is_dir():
            return candidate
        if current == stop_dir or current.parent == current:
            break
        current = current.parent
    return None


@lru_cache(maxsize=128)
def _autodiscover_paths(
    start_dir: Path, stop_dir: Path
) -> tuple[Path | None, Path | None]:
    queries = _find_nearest_dir("queries", start_dir, stop_dir)
    migrations = _find_nearest_dir("migrations", start_dir, stop_dir)
    return queries, migrations


@dataclass
class Config:
    database_path: Path
    queries_path: Path
    migrations_path: Path

    def ensure_directories(self):
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.queries_path.mkdir(parents=True, exist_ok=True)
        self.migrations_path.mkdir(parents=True, exist_ok=True)


def get_config(db_path: str | None = None):
    database_path = Path(db_path).resolve() if db_path else get_database_path()
    root_dir = database_path.parent
    cwd = Path.cwd().resolve()

    queries_found, migrations_found = _autodiscover_paths(root_dir, cwd)

    queries_path = queries_found or (root_dir / "queries")
    migrations_path = migrations_found or (root_dir / "migrations")

    config = Config(
        database_path=database_path,
        queries_path=queries_path,
        migrations_path=migrations_path,
    )
    config.ensure_directories()

    return config

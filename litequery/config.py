import os
from dataclasses import dataclass
from pathlib import Path


def get_database_path() -> Path:
    db_path = os.getenv("DATABASE_PATH")
    if not db_path:
        raise ValueError("DATABASE_PATH environment variable not set")
    return Path(db_path)


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
    database_path = Path(db_path) if db_path else get_database_path()
    root_dir = database_path.parent
    config = Config(
        database_path=database_path,
        queries_path=root_dir / "queries",
        migrations_path=root_dir / "migrations",
    )
    config.ensure_directories()

    return config

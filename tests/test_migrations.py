import os
import sqlite3
import tempfile
import textwrap
from pathlib import Path

import pytest

from litequery.migrations import migrate


@pytest.fixture
def temp_db():
    fd, path = tempfile.mkstemp()
    yield path
    os.close(fd)
    os.unlink(path)


@pytest.fixture
def temp_migrations_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        migrations_dir = Path(tmpdir) / "migrations"
        migrations_dir.mkdir()
        yield migrations_dir


def create_migration_file(dir_path, filename, content):
    migration_path = dir_path / filename
    with open(migration_path, "w") as f:
        f.write(content)
    return migration_path


def test_migrate(temp_db, temp_migrations_dir):
    migrations = {
        "001_initial.sql": "create table users (id integer primary key autoincrement);",
        "002_add_name.sql": "alter table users add column name text;",
    }
    for filename, content in migrations.items():
        create_migration_file(temp_migrations_dir, filename, content)

    migrate(temp_db, temp_migrations_dir)

    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()

    applied_migrations = cursor.execute(
        "select filename from migrations order by run_at"
    ).fetchall()
    assert len(applied_migrations) == 2
    assert applied_migrations[0][0] == "001_initial.sql"
    assert applied_migrations[1][0] == "002_add_name.sql"

    table_info = cursor.execute("pragma table_info(users)").fetchall()
    assert table_info[0][1] == "id"
    assert table_info[1][1] == "name"

    conn.close()


def test_creates_schema(temp_db, temp_migrations_dir):
    create_migration_file(
        temp_migrations_dir,
        "001_initial.sql",
        "create table users (id integer primary key autoincrement);",
    )

    migrate(temp_db, temp_migrations_dir)

    with open(os.path.join(os.path.dirname(temp_db), "schema.sql")) as f:
        schema_content = """
            CREATE TABLE migrations (
                id integer primary key autoincrement,
                filename text not null,
                run_at text not null default current_timestamp
            );
            CREATE TABLE sqlite_sequence(name,seq);
            CREATE TABLE users (id integer primary key autoincrement);
        """
        assert f.read().strip() == textwrap.dedent(schema_content).strip()

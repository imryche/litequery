import os
import shutil
import sqlite3
import tempfile
import textwrap
from pathlib import Path

import pytest

from litequery.config import get_config
from litequery.migrations import migrate


@pytest.fixture
def temp_db():
    fd, path = tempfile.mkstemp()
    yield Path(path)
    os.close(fd)
    os.unlink(path)


@pytest.fixture
def temp_migrations_dir(temp_db):
    migrations_path = temp_db.parent / "migrations"
    migrations_path.mkdir(exist_ok=True)
    yield migrations_path
    shutil.rmtree(migrations_path)


def create_migration_file(dir_path, filename, content):
    migration_path = dir_path / filename
    with open(migration_path, "w") as f:
        f.write(content)
    return migration_path


def test_migrate(temp_db: Path, temp_migrations_dir: Path):
    migrations = {
        "001_initial.sql": "create table users (id integer primary key autoincrement);",
        "002_add_name.sql": "alter table users add column name text;",
    }
    for filename, content in migrations.items():
        create_migration_file(temp_migrations_dir, filename, content)

    migrate(get_config(str(temp_db)))

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

    migrate(get_config(str(temp_db)))

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


def test_migration_rollback_on_failure(temp_db: Path, temp_migrations_dir: Path):
    """Test that a failing migration rolls back completely"""
    # Create a successful migration first
    create_migration_file(
        temp_migrations_dir,
        "001_initial.sql",
        "create table users (id integer primary key autoincrement);",
    )

    # First migration should succeed
    migrate(get_config(str(temp_db)))

    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()

    # Verify first migration was applied
    applied_migrations = cursor.execute(
        "select filename from migrations order by run_at"
    ).fetchall()
    assert len(applied_migrations) == 1
    assert applied_migrations[0][0] == "001_initial.sql"

    # Verify users table exists
    cursor.execute("select count(*) from users")
    assert cursor.fetchone()[0] == 0

    conn.close()

    # Now create a migration that will fail partway through
    failing_migration = textwrap.dedent("""
        create table products (id integer primary key autoincrement);
        insert into products (id) values (1);
        create table invalid_table (id integer primary key, id integer);
    """).strip()

    create_migration_file(temp_migrations_dir, "002_failing.sql", failing_migration)

    # Try to apply the failing migration
    with pytest.raises(Exception):
        migrate(get_config(str(temp_db)))

    # Verify the failing migration was not recorded and no partial changes were made
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()

    applied_migrations = cursor.execute(
        "select filename from migrations order by run_at"
    ).fetchall()
    # Should still be only 1 migration (the successful one)
    assert len(applied_migrations) == 1
    assert applied_migrations[0][0] == "001_initial.sql"

    # Verify products table was not created (rollback worked)
    try:
        cursor.execute("select count(*) from products")
        assert False, "products table should not exist"
    except sqlite3.OperationalError as e:
        assert "no such table: products" in str(e)

    conn.close()

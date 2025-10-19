import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pytest

import litequery
from litequery.core import parse_queries


@pytest.fixture
def lq(tmp_path):
    db_path = tmp_path / "test.db"
    queries_path = "tests/queries"

    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            create table users (
                id integer primary key autoincrement,
                name text not null,
                email text not null,
                created_at datetime not null default current_timestamp
            );
            create table events (
                id integer primary key autoincrement,
                user_id integer not null,
                name text not null,
                created_at datetime not null default current_timestamp,
                foreign key (user_id) references users (id) on delete cascade
            );
            insert into users (id, name, email) values (1, 'Alice', 'alice@example.com');
            insert into users (id, name, email) values (2, 'Bob', 'bob@example.com');
            insert into events (user_id, name) values (1, 'user_logged_in');
            insert into events (user_id, name) values (2, 'password_changed');
        """)
        conn.commit()
    lq = litequery.setup(db_path, queries_path)
    yield lq
    lq.close()


def test_parse_queries_from_directory():
    queries_path = Path("tests/queries").resolve()
    queries = parse_queries(queries_path)
    assert len(queries) == 6


def test_get_all_users(lq):
    users = lq.get_all_users()
    assert_all_users(users)


def assert_all_users(users):
    assert len(users) == 2
    assert users[0].name == "Alice"
    assert isinstance(users[0].created_at, datetime)
    assert users[1].name == "Bob"
    assert isinstance(users[1].created_at, datetime)


def test_get_user_by_id(lq):
    user = lq.get_user_by_id(id=1)
    assert user.email == "alice@example.com"


def test_get_user_into(lq):
    @dataclass
    class User:
        id: int
        name: str
        email: str
        created_at: datetime

    user = lq.get_user_by_id(id=1).into(User)
    assert isinstance(user, User)

    users = lq.get_all_users().into(User)
    assert isinstance(users[0], User)


def test_get_last_user_id(lq):
    user_id = lq.get_last_user_id()
    assert user_id == 2


def test_insert_user(lq):
    user_id = lq.insert_user(name="Eve", email="eve@example.com")
    assert user_id == 3
    user = lq.get_user_by_id(id=user_id)
    assert user.name == "Eve"
    assert user.email == "eve@example.com"


def test_delete_all_users(lq):
    users = lq.get_all_users()
    assert len(users) == 2
    rowcount = lq.delete_all_users()
    assert rowcount == 2
    users = lq.get_all_users()
    assert len(users) == 0


def test_transaction_commit(lq):
    with lq.transaction():
        lq.insert_user(name="Charlie", email="charlie@example.com")
        lq.insert_user(name="Eve", email="eve@example.com")

    users = lq.get_all_users()
    assert len(users) == 4
    assert users[2].name, users[2].email == ("Charlie", "charlie@example.com")
    assert users[3].name, users[3].email == ("Eve", "eve@example.com")


def test_transaction_rollback(lq):
    with pytest.raises(Exception):
        with lq.transaction():
            lq.insert_user(name="Charlie", email="charlie@example.com")
            raise Exception("Force rollback")
            lq.insert_user(name="Eve", email="eve@example.com")

    users = lq.get_all_users()
    assert len(users) == 2


def test_foreign_keys_enabled(lq):
    events = lq.get_all_events()
    assert len(events) == 2

    lq.delete_all_users()

    events = lq.get_all_events()
    assert len(events) == 0


def test_pragmas_configured(lq):
    expected_pragmas = [
        ("journal_mode", "wal"),
        ("foreign_keys", 1),
        ("synchronous", 1),
        ("mmap_size", 134217728),  # 128 Mb
        ("journal_size_limit", 67108864),  # 64 Mb
        ("cache_size", 2000),
        ("busy_timeout", 5000),
    ]

    for pragma, expected_value in expected_pragmas:
        value = lq.raw_value(f"pragma {pragma}")
        assert value == expected_value

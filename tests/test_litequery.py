import sqlite3
import os
import pytest
import litequery
import pytest_asyncio

from litequery.core import parse_queries

DATABASE_PATH = "users.db"
QUERIES_FILE_PATH = "tests/queries.sql"
QUERIES_DIR_PATH = "tests/queries/"


@pytest.fixture
def setup_database():
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute(
            """
            create table users (
                id integer primary key autoincrement,
                name text not null,
                email text not null
            )
            """
        )
        conn.execute(
            """
            create table events (
                id integer primary key autoincrement,
                user_id integer not null,
                name text not null,
                created_at datetime not null default current_timestamp,
                foreign key (user_id) references users (id)
            )
            """
        )
        conn.execute(
            "insert into users (id, name, email) values (1, 'Alice', 'alice@example.com')"
        )
        conn.execute(
            "insert into users (id, name, email) values (2, 'Bob', 'bob@example.com')"
        )
        conn.execute("insert into events (user_id, name) values (1, 'user_logged_in')")
        conn.execute(
            "insert into events (user_id, name) values (2, 'password_changed')"
        )
        conn.commit()
    yield
    os.remove(DATABASE_PATH)


@pytest_asyncio.fixture
async def lq_async(setup_database):
    lq = litequery.setup(DATABASE_PATH, QUERIES_DIR_PATH, use_async=True)
    await lq.connect()
    yield lq
    await lq.disconnect()


@pytest.fixture
def lq_sync(setup_database):
    lq = litequery.setup(DATABASE_PATH, QUERIES_DIR_PATH, use_async=False)
    lq.connect()
    yield lq
    lq.disconnect()


@pytest.mark.asyncio
async def test_parse_queries_from_file():
    queries = parse_queries(QUERIES_FILE_PATH)
    assert len(queries) == 5


@pytest.mark.asyncio
async def test_parse_queries_from_directory():
    queries = parse_queries(QUERIES_DIR_PATH)
    assert len(queries) == 6


@pytest.mark.asyncio
async def test_get_all_users_async(lq_async):
    users = await lq_async.get_all_users()
    assert len(users) == 2
    assert users[0].name == "Alice"
    assert users[1].name == "Bob"


def test_get_all_users_sync(lq_sync):
    users = lq_sync.get_all_users()
    assert len(users) == 2
    assert users[0].name == "Alice"
    assert users[1].name == "Bob"


@pytest.mark.asyncio
async def test_get_user_by_id_async(lq_async):
    user = await lq_async.get_user_by_id(id=1)
    assert user.email == "alice@example.com"


def test_get_user_by_id_sync(lq_sync):
    user = lq_sync.get_user_by_id(id=1)
    assert user.email == "alice@example.com"


@pytest.mark.asyncio
async def test_get_last_user_id_async(lq_async):
    user_id = await lq_async.get_last_user_id()
    assert user_id == 2


def test_get_last_user_id_sync(lq_sync):
    user_id = lq_sync.get_last_user_id()
    assert user_id == 2


@pytest.mark.asyncio
async def test_insert_user_async(lq_async):
    user_id = await lq_async.insert_user(name="Eve", email="eve@example.com")
    assert user_id == 3
    user = await lq_async.get_user_by_id(id=user_id)
    assert user.name == "Eve"
    assert user.email == "eve@example.com"


def test_insert_user_sync(lq_sync):
    user_id = lq_sync.insert_user(name="Eve", email="eve@example.com")
    assert user_id == 3
    user = lq_sync.get_user_by_id(id=user_id)
    assert user.name == "Eve"
    assert user.email == "eve@example.com"


@pytest.mark.asyncio
async def test_delete_all_users_async(lq_async):
    users = await lq_async.get_all_users()
    assert len(users) == 2
    rowcount = await lq_async.delete_all_users()
    assert rowcount == 2
    users = await lq_async.get_all_users()
    assert len(users) == 0


def test_delete_all_users_sync(lq_sync):
    users = lq_sync.get_all_users()
    assert len(users) == 2
    rowcount = lq_sync.delete_all_users()
    assert rowcount == 2
    users = lq_sync.get_all_users()
    assert len(users) == 0


@pytest.mark.asyncio
async def test_transaction_commit_async(lq_async):
    async with lq_async.transaction():
        await lq_async.insert_user(name="Charlie", email="charlie@example.com")
        await lq_async.insert_user(name="Eve", email="eve@example.com")

    users = await lq_async.get_all_users()
    assert len(users) == 4
    assert users[2].name, users[2].email == ("Charlie", "charlie@example.com")
    assert users[3].name, users[3].email == ("Eve", "eve@example.com")


def test_transaction_commit_sync(lq_sync):
    with lq_sync.transaction():
        lq_sync.insert_user(name="Charlie", email="charlie@example.com")
        lq_sync.insert_user(name="Eve", email="eve@example.com")

    users = lq_sync.get_all_users()
    assert len(users) == 4
    assert users[2].name, users[2].email == ("Charlie", "charlie@example.com")
    assert users[3].name, users[3].email == ("Eve", "eve@example.com")


@pytest.mark.asyncio
async def test_transaction_rollback_async(lq_async):
    with pytest.raises(Exception):
        async with lq_async.transaction():
            await lq_async.insert_user(name="Charlie", email="charlie@example.com")
            raise Exception("Force rollback")
            await lq_async.insert_user(name="Eve", email="eve@example.com")

    users = await lq_async.get_all_users()
    assert len(users) == 2


def test_transaction_rollback_sync(lq_sync):
    with pytest.raises(Exception):
        with lq_sync.transaction():
            lq_sync.insert_user(name="Charlie", email="charlie@example.com")
            raise Exception("Force rollback")
            lq_sync.insert_user(name="Eve", email="eve@example.com")

    users = lq_sync.get_all_users()
    assert len(users) == 2

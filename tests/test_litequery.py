import os
import pytest
import aiosqlite
import litequery
import pytest_asyncio

from litequery.core import parse_queries

DATABASE_PATH = "users.db"
QUERIES_FILE_PATH = "tests/queries.sql"
QUERIES_DIR_PATH = "tests/queries/"


@pytest_asyncio.fixture
async def setup_database():
    async with aiosqlite.connect(DATABASE_PATH) as conn:
        await conn.execute(
            """
            create table users (
                id integer primary key autoincrement,
                name text not null,
                email text not null
            )
            """
        )
        await conn.execute(
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
        await conn.execute(
            "insert into users (id, name, email) values (1, 'Alice', 'alice@example.com')"
        )
        await conn.execute(
            "insert into users (id, name, email) values (2, 'Bob', 'bob@example.com')"
        )
        await conn.execute(
            "insert into events (user_id, name) values (1, 'user_logged_in')"
        )
        await conn.execute(
            "insert into events (user_id, name) values (2, 'password_changed')"
        )
        await conn.commit()
    yield
    os.remove(DATABASE_PATH)


@pytest_asyncio.fixture
async def lq(setup_database):
    lq = litequery.setup(DATABASE_PATH, QUERIES_DIR_PATH)
    await lq.connect()
    yield lq
    await lq.disconnect()


@pytest.mark.asyncio
async def test_parse_queries_from_file():
    queries = parse_queries(QUERIES_FILE_PATH)
    assert len(queries) == 5


@pytest.mark.asyncio
async def test_parse_queries_from_directory():
    queries = parse_queries(QUERIES_DIR_PATH)
    assert len(queries) == 6


@pytest.mark.asyncio
async def test_get_all_users(lq):
    users = await lq.get_all_users()
    assert len(users) == 2
    assert users[0].name == "Alice"
    assert users[1].name == "Bob"


@pytest.mark.asyncio
async def test_get_user_by_id(lq):
    user = await lq.get_user_by_id(id=1)
    assert user.email == "alice@example.com"


@pytest.mark.asyncio
async def test_get_last_user_id(lq):
    user_id = await lq.get_last_user_id()
    assert user_id == 2


@pytest.mark.asyncio
async def test_insert_user(lq):
    user_id = await lq.insert_user(name="Eve", email="eve@example.com")
    assert user_id == 3
    user = await lq.get_user_by_id(id=user_id)
    user.name == "Eve"
    user.email == "eve@example.com"


@pytest.mark.asyncio
async def test_delete_all_users(lq):
    users = await lq.get_all_users()
    assert len(users) == 2
    rowcount = await lq.delete_all_users()
    assert rowcount == 2
    users = await lq.get_all_users()
    assert len(users) == 0


@pytest.mark.asyncio
async def test_transaction_commit(lq):
    async with lq.transaction():
        await lq.insert_user(name="Charlie", email="charlie@example.com")
        await lq.insert_user(name="Eve", email="eve@example.com")

    users = await lq.get_all_users()
    assert len(users) == 4
    assert users[2].name, users[2].email == ("Charlie", "charlie@example.com")
    assert users[3].name, users[3].email == ("Eve", "eve@example.com")


@pytest.mark.asyncio
async def test_transaction_rollback(lq):
    with pytest.raises(Exception):
        async with lq.transaction():
            await lq.insert_user(name="Charlie", email="charlie@example.com")
            raise Exception("Force rollback")
            await lq.insert_user(name="Eve", email="eve@example.com")

    users = await lq.get_all_users()
    assert len(users) == 2

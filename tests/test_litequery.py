import os
import pytest
import aiosqlite
import litequery
import pytest_asyncio

DATABASE_PATH = "users.db"
QUERIES_PATH = "tests/queries.sql"


@pytest_asyncio.fixture
async def setup_database():
    async with aiosqlite.connect(DATABASE_PATH) as conn:
        await conn.execute(
            "create table users (id integer primary key autoincrement, name text not null, email text not null)"
        )
        await conn.execute(
            "insert into users (name, email) values ('Alice', 'alice@example.com')"
        )
        await conn.execute(
            "insert into users (name, email) values ('Bob', 'bob@example.com')"
        )
        await conn.commit()
    yield
    os.remove(DATABASE_PATH)


@pytest_asyncio.fixture
async def lq(setup_database):
    lq = litequery.setup(DATABASE_PATH, QUERIES_PATH)
    await lq.connect()
    yield lq
    await lq.disconnect()


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

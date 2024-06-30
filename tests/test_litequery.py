import os
import pytest
import aiosqlite
import litequery
import pytest_asyncio

DATABASE_PATH = "test.db"
QUERIES_PATH = "queries.sql"


@pytest_asyncio.fixture
async def setup_database():
    async with aiosqlite.connect(DATABASE_PATH) as conn:
        await conn.execute(
            "create table users (id integer primary key autoincrement, name text)"
        )
        await conn.commit()
    yield
    os.remove(DATABASE_PATH)


@pytest_asyncio.fixture
async def lq(setup_database):
    lq = litequery.create(DATABASE_PATH, QUERIES_PATH)
    await lq.connect()
    yield lq
    await lq.disconnect()


@pytest.mark.asyncio
async def test_queries(lq):
    print(await lq.users_delete_all())
    print(await lq.users_insert(name="kocia"))
    print(await lq.users_insert(name="kot"))
    print(await lq.users_insert(name="simba"))
    print(await lq.users_insert(name="bunchik"))
    print(await lq.users_all())
    print(await lq.users_first())

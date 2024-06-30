import pytest
import litequery


@pytest.mark.asyncio
async def test_queries():
    lq = litequery.create("users.db", "queries.sql")
    await lq.connect()
    print(await lq.users_delete_all())
    print(await lq.users_insert(name="kocia"))
    print(await lq.users_insert(name="kot"))
    print(await lq.users_insert(name="simba"))
    print(await lq.users_insert(name="bunchik"))
    print(await lq.users_all())
    print(await lq.users_first())
    await lq.disconnect()

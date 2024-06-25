import pytest
import litequery


@pytest.mark.asyncio
async def test_idea():
    lq = litequery.create("users.db", "queries.sql")
    await lq.connect()
    print(await lq.users_all())
    print(await lq.users_first())
    await lq.disconnect()

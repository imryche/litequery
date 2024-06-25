import re
from dataclasses import make_dataclass
import aiosqlite


def parse_queries(path):
    with open(path) as f:
        content = f.read()
    return dict(re.findall(r"-- name: (\w+)\n(.*?);", content, re.DOTALL))


def create(database, queries_path):
    queries = parse_queries(queries_path)
    return LiteQuery(database, queries)


def dataclass_factory(cursor, row):
    fields = [col[0] for col in cursor.description]
    cls = make_dataclass("Row", fields)
    return cls(*row)


class LiteQuery:
    def __init__(self, database, queries):
        self._database = database
        self._queries = queries
        self._conn = None
        self._create_methods()

    def _create_method(self, name):
        async def query_method():
            query = self._queries[name]
            conn = await self.conn()
            async with conn.execute(query) as cursor:
                rows = await cursor.fetchall()
            return rows

        return query_method

    def _create_methods(self):
        for name, query in self._queries.items():
            setattr(self, name, self._create_method(name))

    async def connect(self):
        self._conn = await aiosqlite.connect(self._database)
        self._conn.row_factory = dataclass_factory

    async def disconnect(self):
        if self._conn is None:
            return
        await self._conn.close()

    async def conn(self):
        if self._conn is None:
            await self.connect()
        return self._conn

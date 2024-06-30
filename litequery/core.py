from enum import Enum, auto
import re
from dataclasses import dataclass, make_dataclass
import aiosqlite


class Op(Enum):
    SELECT = auto()
    INSERT = auto()


OP_TYPES = {
    "": Op.SELECT,
    "!": Op.INSERT,
}


@dataclass
class Query:
    name: str
    sql: str
    args: list
    op: Op = Op.SELECT


def parse_queries(path):
    with open(path) as f:
        content = f.read()
    raw_queries = re.findall(r"-- name: (.+)\n([\s\S]*?);", content)
    queries = []
    for query_name, sql in raw_queries:
        match = re.match(r"^([a-z_][a-z0-9_-]*)([!]?)$", query_name)
        if not match:
            raise NameError(f'Invalid query name: "{query_name}"')
        query_name = match.group(1)
        op_symbol = match.group(2)
        op = OP_TYPES.get(op_symbol, Op.SELECT)

        args = re.findall(r":(\w+)", sql)
        query = Query(name=query_name, sql=sql, args=args, op=op)
        queries.append(query)
    return queries


def create(database, queries_path):
    queries = parse_queries(queries_path)
    return Litequery(database, queries)


def dataclass_factory(cursor, row):
    fields = [col[0] for col in cursor.description]
    cls = make_dataclass("Record", fields)
    return cls(*row)


class Litequery:
    def __init__(self, database, queries):
        self._database = database
        self._conn = None
        self._create_methods(queries)

    def _create_method(self, query: Query):
        async def query_method(**kwargs):
            conn = await self.conn()
            async with conn.execute(query.sql, kwargs) as cur:
                if query.op == Op.SELECT:
                    return await cur.fetchall()
                if query.op == Op.INSERT:
                    return cur.rowcount

        return query_method

    def _create_methods(self, queries: list[Query]):
        for query in queries:
            setattr(self, query.name, self._create_method(query))

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

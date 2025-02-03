import glob
import os
import re
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, fields, make_dataclass
from datetime import datetime
from enum import Enum

import aiosqlite

_iso8601_pattern = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
)


class Op(str, Enum):
    SELECT = ""
    SELECT_ONE = "^"
    SELECT_VALUE = "$"
    MODIFY = "!"
    INSERT_RETURNING = "<!"


@dataclass
class Query:
    name: str
    sql: str
    args: list
    op: Op = Op.SELECT


def parse_file_queries(file_path):
    with open(file_path) as f:
        content = f.read()
    raw_queries = re.findall(r"-- name: (.+)\n([\s\S]*?);", content)

    queries = []
    op_pattern = "|".join("\\" + "\\".join(list(op.value)) for op in Op if op.value)
    pattern = rf"^([a-z_][a-z0-9_-]*)({op_pattern})?$"
    for query_name, sql in raw_queries:
        match = re.match(pattern, query_name)
        if not match:
            raise NameError(f'Invalid query name: "{query_name}"')
        query_name = match.group(1)
        op_symbol = match.group(2) or ""
        op = Op(op_symbol)

        args = re.findall(r":(\w+)", sql)
        query = Query(name=query_name, sql=sql, args=args, op=op)
        queries.append(query)
    return queries


def parse_queries(path):
    queries = []
    if os.path.isdir(path):
        for file_path in glob.glob(os.path.join(path, "*.sql")):
            queries.extend(parse_file_queries(file_path))
    elif os.path.isfile(path):
        queries.extend(parse_file_queries(path))
    else:
        raise ValueError(f"Path {path} is neither a file nor a directory.")
    return queries


def setup(database, queries_path, use_async=False):
    queries = parse_queries(queries_path)
    if use_async:
        return LitequeryAsync(database, queries)
    return LitequerySync(database, queries)


def dataclass_factory(cursor, row):
    fields, values = [], []
    for desc, value in zip(cursor.description, row):
        fields.append(desc[0])
        if isinstance(value, str) and _iso8601_pattern.match(value):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                pass
        values.append(value)

    cls = make_dataclass("Record", fields)
    return cls(*values)


class LitequeryBase:
    PRAGMAS = [
        ("journal_mode", "wal"),
        ("foreign_keys", 1),
        ("synchronous", "normal"),
        ("mmap_size", 134217728),  # 128 Mb
        ("journal_size_limit", 67108864),  # 64 Mb
        ("cache_size", 2000),
        ("busy_timeout", 5000),
    ]

    def __init__(self, database, queries):
        self._database = database
        self._conn = None
        self._in_transaction = False
        self._create_methods(queries)

    def _create_methods(self, queries: list[Query]):
        for query in queries:
            setattr(self, query.name, self._create_method(query))

    def _create_method(self, query):
        raise NotImplementedError("This method should be overridden!")


class LitequeryAsync(LitequeryBase):
    def _create_method(self, query: Query):
        # TODO: check for invalid named arguments
        # TODO: add support for positional arguments
        async def query_method(**kwargs):
            conn = await self.get_connection()
            async with conn.execute(query.sql, kwargs) as cur:
                if query.op == Op.SELECT:
                    return await cur.fetchall()
                if query.op == Op.SELECT_ONE:
                    return await cur.fetchone()
                if query.op == Op.SELECT_VALUE:
                    row = await cur.fetchone()
                    return getattr(row, fields(row)[0].name) if row else None
                if query.op == Op.MODIFY:
                    if not self._in_transaction:
                        await conn.commit()
                    return cur.rowcount
                if query.op == Op.INSERT_RETURNING:
                    if not self._in_transaction:
                        await conn.commit()
                    return cur.lastrowid

        return query_method

    async def _apply_pragmas(self):
        for pragma, value in self.PRAGMAS:
            await self._conn.execute(f"pragma {pragma} = {value}")

    @asynccontextmanager
    async def transaction(self):
        conn = await self.get_connection()
        try:
            await conn.execute("begin")
            self._in_transaction = True
            yield
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        finally:
            self._in_transaction = False

    async def connect(self):
        self._conn = await aiosqlite.connect(self._database, timeout=5)
        self._conn.row_factory = dataclass_factory
        await self._apply_pragmas()

    async def disconnect(self):
        if self._conn is None:
            return
        await self._conn.close()
        self._conn = None

    async def get_connection(self):
        if self._conn is None:
            await self.connect()
        return self._conn


class LitequerySync(LitequeryBase):
    def _create_method(self, query: Query):
        def query_method(**kwargs):
            conn = self.get_connection()
            cur = conn.execute(query.sql, kwargs)
            if query.op == Op.SELECT:
                return cur.fetchall()
            if query.op == Op.SELECT_ONE:
                return cur.fetchone()
            if query.op == Op.SELECT_VALUE:
                row = cur.fetchone()
                return getattr(row, fields(row)[0].name) if row else None
            if query.op == Op.MODIFY:
                if not self._in_transaction:
                    conn.commit()
                return cur.rowcount
            if query.op == Op.INSERT_RETURNING:
                if not self._in_transaction:
                    conn.commit()
                return cur.lastrowid

        return query_method

    def _apply_pragmas(self):
        for pragma, value in self.PRAGMAS:
            self._conn.execute(f"pragma {pragma} = {value}")

    @contextmanager
    def transaction(self):
        conn = self.get_connection()
        try:
            conn.execute("begin")
            self._in_transaction = True
            yield
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._in_transaction = False

    def connect(self):
        self._conn = sqlite3.connect(self._database, timeout=5)
        self._conn.row_factory = dataclass_factory
        self._apply_pragmas()

    def disconnect(self):
        if self._conn is None:
            return
        self._conn.close()
        self._conn = None

    def get_connection(self):
        if self._conn is None:
            self.connect()
        return self._conn

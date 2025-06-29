import glob
import os
import re
import sqlite3
from collections import OrderedDict
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Iterator

import aiosqlite

from litequery.config import Config, get_config

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


class Row:
    def __init__(self, columns: list[str], values: list[Any]):
        self._data = OrderedDict()
        self._values: list[Any] = []

        for col, val in zip(columns, values):
            if isinstance(val, str) and _iso8601_pattern.match(val):
                try:
                    val = datetime.fromisoformat(val)
                except ValueError:
                    pass
            self._values.append(val)
            self._data[col] = val

    def _available_columns(self) -> str:
        return ", ".join([f"'{c}'" for c in self._data.keys()])

    def __repr__(self) -> str:
        items = [f"{col}={repr(val)}" for col, val in self._data.items()]
        return f"{self.__class__.__name__}({', '.join(items)})"

    def __getitem__(self, key: int | str) -> Any:
        if isinstance(key, int):
            try:
                return self._values[key]
            except IndexError:
                raise IndexError(
                    f"Row only has {len(self._values)} columns, "
                    f"can't access index {key}"
                )
        try:
            return self._data[key]
        except KeyError:
            raise KeyError(
                f"No column '{key}' found. Available: {self._available_columns()}"
            )

    def __getattr__(self, name: str) -> Any:
        error = AttributeError(
            f"No column '{name}' found. Available: {self._available_columns()}"
        )
        if name.startswith("_"):
            raise error

        try:
            return self._data[name]
        except KeyError:
            raise error

    def __setitem__(self, key: int | str, value: Any) -> None:
        raise TypeError("Row assignment not supported")

    def __contains__(self, name: str) -> bool:
        return name in self._data

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Row):
            return False
        return self._data == other._data

    def keys(self) -> list[str]:
        return list(self._data.keys())

    def values(self) -> list[Any]:
        return list(self._data.values())

    def items(self) -> list[tuple[str, Any]]:
        return list(self._data.items())

    def to_dict(self) -> dict:
        return dict(self._data)


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


def parse_queries(config: Config):
    queries = []
    if os.path.isdir(config.queries_path):
        for file_path in glob.glob(os.path.join(config.queries_path, "*.sql")):
            queries.extend(parse_file_queries(file_path))
    elif os.path.isfile(config.queries_path):
        queries.extend(parse_file_queries(config.queries_path))
    else:
        raise ValueError(
            f"Path {config.queries_path} is neither a file nor a directory."
        )
    return queries


def setup(db_path: str | None = None, *, use_async=False):
    config = get_config(db_path) if db_path else get_config()
    queries = parse_queries(config)
    if use_async:
        return LitequeryAsync(config, queries)
    return LitequerySync(config, queries)


def row_factory(cursor, row):
    columns = [desc[0] for desc in cursor.description]
    return Row(columns, row)


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

    def __init__(self, config: Config, queries):
        self.config = config
        self._conn = None
        self._in_transaction = False
        self._create_methods(queries)

    def _create_methods(self, queries: list[Query]):
        for query in queries:
            setattr(self, query.name, self._create_method(query))

    def _create_method(self, query):
        raise NotImplementedError("This method should be overridden!")


class LitequeryAsync(LitequeryBase):
    async def _execute_query(self, conn, query: Query, kwargs: dict):
        async with conn.execute(query.sql, kwargs) as cur:
            if query.op == Op.SELECT:
                return await cur.fetchall()
            if query.op == Op.SELECT_ONE:
                return await cur.fetchone()
            if query.op == Op.SELECT_VALUE:
                row = await cur.fetchone()
                return row[0] if row else None
            if query.op == Op.MODIFY:
                if not self._in_transaction:
                    await conn.commit()
                return cur.rowcount
            if query.op == Op.INSERT_RETURNING:
                if not self._in_transaction:
                    await conn.commit()
                return cur.lastrowid

    def _create_method(self, query: Query):
        # TODO: check for invalid named arguments
        # TODO: add support for positional arguments
        async def query_method(**kwargs):
            if self._in_transaction and self._conn:
                return await self._execute_query(self._conn, query, kwargs)
            async with self.connect() as conn:
                return await self._execute_query(conn, query, kwargs)

        return query_method

    @asynccontextmanager
    async def connect(self):
        if self._in_transaction and self._conn:
            yield self._conn
        else:
            conn = await aiosqlite.connect(self.config.database_path, timeout=5)
            conn.row_factory = row_factory
            for pragma, value in self.PRAGMAS:
                await conn.execute(f"pragma {pragma} = {value}")
            try:
                yield conn
            finally:
                await conn.close()

    @asynccontextmanager
    async def transaction(self):
        if self._in_transaction and self._conn:
            yield self._conn
        else:
            conn = await aiosqlite.connect(self.config.database_path, timeout=5)
            conn.row_factory = row_factory
            for pragma, value in self.PRAGMAS:
                await conn.execute(f"pragma {pragma} = {value}")
            try:
                self._conn = conn
                self._in_transaction = True
                await conn.execute("begin")
                yield
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
            finally:
                self._conn = None
                self._in_transaction = False
                await conn.close()


class LitequerySync(LitequeryBase):
    def _execute_query(self, conn, query: Query, kwargs: dict):
        cur = conn.execute(query.sql, kwargs)
        if query.op == Op.SELECT:
            return cur.fetchall()
        if query.op == Op.SELECT_ONE:
            return cur.fetchone()
        if query.op == Op.SELECT_VALUE:
            row = cur.fetchone()
            return row[0] if row else None
        if query.op == Op.MODIFY:
            if not self._in_transaction:
                conn.commit()
            return cur.rowcount
        if query.op == Op.INSERT_RETURNING:
            if not self._in_transaction:
                conn.commit()
            return cur.lastrowid

    def _create_method(self, query: Query):
        def query_method(**kwargs):
            if self._in_transaction and self._conn:
                return self._execute_query(self._conn, query, kwargs)
            with self.connect() as conn:
                return self._execute_query(conn, query, kwargs)

        return query_method

    @contextmanager
    def connect(self):
        if self._in_transaction and self._conn:
            yield self._conn
        else:
            conn = sqlite3.connect(self.config.database_path, timeout=5)
            conn.row_factory = row_factory
            for pragma, value in self.PRAGMAS:
                conn.execute(f"pragma {pragma} = {value}")
            try:
                yield conn
            finally:
                conn.close()

    @contextmanager
    def transaction(self):
        if self._in_transaction and self._conn:
            yield self._conn
        else:
            conn = sqlite3.connect(self.config.database_path, timeout=5)
            conn.row_factory = row_factory
            for pragma, value in self.PRAGMAS:
                conn.execute(f"pragma {pragma} = {value}")
            try:
                self._conn = conn
                self._in_transaction = True
                conn.execute("begin")
                yield
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                self._conn = None
                self._in_transaction = False
                conn.close()

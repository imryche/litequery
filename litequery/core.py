import glob
import os
import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from litequery.config import Config, get_config


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
        if len(set(columns)) != len(columns):
            dups = [c for c in columns if columns.count(c) > 1]
            raise ValueError(f"Duplicate columns: {set(dups)}. Use AS to alias.")

        self._values = tuple(
            self._parse_datetime(v) if isinstance(v, str) else v for v in values
        )
        self._index = {c: i for i, c in enumerate(columns)}

    def _parse_datetime(self, value: str):
        if 19 <= len(value) <= 32 and value[0].isdigit():
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                pass
        return value

    def _available_columns(self) -> str:
        return ", ".join([f"'{c}'" for c in self._index.keys()])

    def __repr__(self) -> str:
        items = [f"{col}={self._values[idx]!r}" for col, idx in self._index.items()]
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
            return self._values[self._index[key]]
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
            return self._values[self._index[name]]
        except KeyError:
            raise error

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Row):
            return False
        return self._values == other._values

    def to_dict(self) -> dict:
        return dict(zip(self._index.keys(), self._values))

    def into(self, cls):
        return cls(**self.to_dict())


class Rows(list):
    def into(self, cls):
        return Rows([cls(**row.to_dict()) for row in self])


def parse_file_queries(file_path):
    with open(file_path) as f:
        content = f.read()
    raw_queries = re.findall(r"-- name: (.+)\n([\s\S]*?);", content)

    queries = []
    op_pattern = "|".join("\\" + "\\".join(list(op.value)) for op in Op if op.value)
    pattern = rf"^([a-z_][a-z0-9_]*)({op_pattern})?$"
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


def setup(db_path: str | None = None):
    config = get_config(db_path) if db_path else get_config()
    queries = parse_queries(config)
    return Litequery(config, queries)


def row_factory(cursor, row):
    columns = [desc[0] for desc in cursor.description]
    return Row(columns, row)


class Litequery:
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
            if hasattr(self, query.name):
                if hasattr(Litequery, query.name):
                    raise NameError(f"Query name {query.name} isn't allowed.")

                if callable(getattr(self, query.name)):
                    raise NameError(
                        f"Duplicate query name '{query.name}'. "
                        "Each query must have a unique name."
                    )
            setattr(self, query.name, self._create_method(query))

    def _execute_query(self, conn: sqlite3.Connection, query: Query, kwargs: dict):
        cursor = conn.cursor()
        cursor.execute(query.sql, kwargs)

        if query.op == Op.SELECT:
            return Rows(cursor.fetchall())
        if query.op == Op.SELECT_ONE:
            return cursor.fetchone()
        if query.op == Op.SELECT_VALUE:
            row = cursor.fetchone()
            return row[0] if row else None
        if query.op == Op.MODIFY:
            if not self._in_transaction:
                conn.commit()
            return cursor.rowcount
        if query.op == Op.INSERT_RETURNING:
            if not self._in_transaction:
                conn.commit()
            return cursor.lastrowid

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
            pragmas = (f"pragma {p} = {v}" for p, v in self.PRAGMAS)
            conn.executescript(";".join(pragmas))
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

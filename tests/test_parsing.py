from pathlib import Path

from litequery.core import parse_queries


def test_parse_queries_from_directory():
    queries_path = Path("tests/queries").resolve()
    queries = parse_queries(queries_path)
    assert len(queries) == 6

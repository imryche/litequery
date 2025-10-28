import sqlite3

import pytest

import litequery


@pytest.fixture
def lq(tmp_path):
    db_path = tmp_path / "test.db"
    queries_path = "tests/queries"

    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            create table users (
                id integer primary key autoincrement,
                name text not null,
                email text not null,
                created_at datetime not null default current_timestamp
            );
            create table events (
                id integer primary key autoincrement,
                user_id integer not null,
                name text not null,
                created_at datetime not null default current_timestamp,
                foreign key (user_id) references users (id) on delete cascade
            );
            insert into users (id, name, email) values (1, 'Alice', 'alice@example.com');
            insert into users (id, name, email) values (2, 'Bob', 'bob@example.com');
            insert into users (id, name, email) values (3, 'Charlie', 'charlie@example.com');
            insert into events (user_id, name) values (1, 'user_logged_in');
            insert into events (user_id, name) values (2, 'password_changed');
        """)
        conn.commit()
    lq = litequery.setup(db_path, queries_path)
    yield lq
    lq.close()

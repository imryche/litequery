# Litequery

Litequery is a minimalist, async-first for interacting with SQLite in Python. It
lets you define your queries once and call them as methods. No ORM bloat, just
raw SQL power.

## Why Litequery?

- **Simplicity**: Define SQL queries in `.sql` files. No complex ORM layers.
- **Async first**: Built for modern async Python with `aiosqlite`.
- **Flexible**: Supports different SQL operations seamlessly.

## Installation

```
pip install litequery
```

## Getting Started

### Define your queries

Create a `queries.sql` file. Name your queries using comments and write them in
pure SQL.

```sql
-- name: get_all_users
SELECT * FROM users;

-- name: get_user_by_id^
SELECT * FROM users WHERE id = :id;

-- name: get_last_user_id$
SELECT MAX(id) FROM users;

-- name: insert_user<!
INSERT INTO users (name, email) VALUES (:name, :email);

-- name: delete_all_users!
DELETE FROM users;
```

### Call your queries

```python
import litequery
import asyncio

async def main():
    lq = litequery.setup("database.db", "queries.sql")
    await lq.connect()

    user_id = await lq.insert_user(name="Alice", email="alice@example.com")
    print(user_id)

    users = await lq.get_all_users()
    print(users)

    user = await lq.get_user_by_id(id=user_id)
    print(user)

    rows_count = await lq.delete_all_users()

    await lq.disconnect()


asyncio.run(main())
```

Happy querying! If you run into any issues or have feature requests, open an
issue or submit a pull request. Let's keep it simple and powerful.

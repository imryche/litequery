# Litequery

Litequery is a minimalist library for interacting with SQLite in Python. It lets
you define your queries once and call them as methods. No ORM bloat, just raw
SQL power, with the flexibility to operate in both asynchronous and synchronous
modes.

## Why Litequery?

- **Simplicity**: Define SQL queries in `.sql` files. No complex ORM layers.
- **Async first**: Built for modern async Python, but also supports synchronous
  operations for traditional use cases.
- **Flexible**: Supports different SQL operations seamlessly.

## Installation

```
pip install litequery
```

## Getting Started

### Define Your Queries

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

### Using Your Queries

Define your database and queries, and then call them as methods. Choose async or
sync setup based on your needs. It's as straightforward as it sounds.

```python
import litequery
import asyncio


async def main():
    lq = litequery.setup("database.db", "queries.sql", use_async=True)
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

### Transaction Support

Litequery also supports transactions in both async and sync contexts, allowing
you to execute multiple queries atomicaly.

```python
import litequery
import asyncio


async def main():
    lq = litequery.setup("database.db", "queries.sql")
    await lq.connect()

    try:
        async with lq.transaction():
            await lq.insert_user(name="Charlie", email="charlie@example.com")
            raise Exception("Force rollback")
            await lq.insert_user(name="Eve", email="eve@example.com")
    except Exception:
        print("Transaction failed")

    users = await lq.get_all_users()
    print(users)

    await lq.disconnect()


asyncio.run(main())

```

## Wrapping Up

Litequery is all about simplicity and efficiency. Why wrestle with bloated ORMs
when you can have raw SQL power? If you think there's a better way or have
suggestions, let's hear them. Happy querying!

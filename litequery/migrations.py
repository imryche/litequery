import glob
import os
import re
import sqlite3
import textwrap
from datetime import datetime

from litequery.config import Config


def migrate(config: Config):
    conn = sqlite3.connect(config.database_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    filenames = sort_migration_filenames(
        os.path.basename(p) for p in glob.glob(f"{config.migrations_path}/*.sql")
    )
    query = """
        create table if not exists migrations (
            id integer primary key autoincrement,
            filename text not null,
            run_at text not null default current_timestamp
        );
    """
    cur.execute(textwrap.dedent(query).strip())
    migrations = cur.execute("select * from migrations order by run_at asc").fetchall()
    migrations = [m["filename"] for m in migrations]
    unapplied = sort_migration_filenames(set(filenames) - set(migrations))

    if not unapplied:
        print("Nothing to apply.")
        return

    print("Applying migrations:")
    for file in unapplied:
        print(f"- {file}")
        try:
            conn.execute("BEGIN")
            with open(f"{config.migrations_path}/{file}") as f:
                migration_sql = f.read()
                # Split by semicolon and execute each statement
                statements = [s.strip() for s in migration_sql.split(';') if s.strip()]
                for statement in statements:
                    cur.execute(statement)
            
            cur.execute(
                "insert into migrations (filename) values (:filename)",
                {"filename": file},
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error applying migration {file}: {e}")
            raise

    generate_schema(cur, config)

    conn.close()


def generate_schema(cur, config: Config):
    with open(
        os.path.join(os.path.dirname(config.database_path), "schema.sql"), "w"
    ) as f:
        statements = cur.execute(
            "select sql from sqlite_master where sql is not null"
        ).fetchall()
        for (statement,) in statements:
            f.write(f"{statement};\n")


def sort_migration_filenames(filenames):
    def get_sort_key(filename):
        match = re.match(r"(\d+)", filename)
        if match:
            return int(match.group(1))
        return float("inf")

    return sorted(filenames, key=get_sort_key)


def create_migration(name, config: Config):
    os.makedirs(config.migrations_path, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    filename = f"{timestamp}_{name.lower().replace(' ', '_')}.sql"
    filepath = os.path.join(config.migrations_path, filename)

    template = f"-- Created: {datetime.now().isoformat()}\n"
    with open(filepath, "w") as f:
        f.write(template)

    print(f"Migration created: {filename}")

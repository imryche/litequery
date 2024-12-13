import glob
import os
import re
import sqlite3
import textwrap


def migrate(db_path, migrations_dir):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    filenames = sort_migration_filenames(
        os.path.basename(p) for p in glob.glob(f"{migrations_dir}/*.sql")
    )
    query = """
        create table if not exists migrations (
            id integer primary key autoincrement,
            filename text not null,
            run_at text not null default current_timestamp
        );
    """
    c.execute(textwrap.dedent(query).strip())
    migrations = c.execute("select * from migrations order by run_at asc").fetchall()
    migrations = {m["filename"] for m in migrations}
    unapplied = sort_migration_filenames(set(filenames) - migrations)

    if not unapplied:
        print("Nothing to apply.")
        return

    print("Applying migrations:")
    for file in unapplied:
        print(f"- {file}")
        with open(f"{migrations_dir}/{file}") as f:
            c.executescript(f.read())

        c.execute(
            "insert into migrations (filename) values (:filename)",
            {"filename": file},
        )
        conn.commit()

    with open(os.path.join(os.path.dirname(db_path), "schema.sql"), "w") as f:
        statements = c.execute(
            "select sql from sqlite_master where sql is not null"
        ).fetchall()
        for (statement,) in statements:
            f.write(f"{statement};\n")

    conn.close()


def sort_migration_filenames(filenames):
    def get_sort_key(filename):
        match = re.match(r"(\d+)", filename)
        if match:
            return int(match.group(1))
        return float("inf")

    return sorted(filenames, key=get_sort_key)

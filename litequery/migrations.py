import re
import sqlite3
import os
import glob


def migrate(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    filenames = sort_migration_filenames(
        os.path.basename(p) for p in glob.glob("migrations/*.sql")
    )
    c.execute(
        """
        --sql
        create table if not exists migrations (
            id integer primary key autoincrement,
            filename text not null,
            run_at datetime not null default current_timestamp
        );
        """
    )
    migrations = c.execute("select * from migrations order by run_at asc").fetchall()
    migrations = {m["filename"] for m in migrations}
    unapplied = sort_migration_filenames(set(filenames) - migrations)

    if not unapplied:
        print("Nothing to apply.")
        return

    print("Applying migrations:")
    for file in unapplied:
        print(f"- {file}")
        with open(f"migrations/{file}") as f:
            c.executescript(f.read())

        c.execute(
            "insert into migrations (filename) values (:filename)",
            {"filename": file},
        )
        conn.commit()

    conn.close()


def sort_migration_filenames(filenames):
    def get_sort_key(filename):
        match = re.match(r"(\d+)", filename)
        if match:
            return int(match.group(1))
        return float("inf")

    return sorted(filenames, key=get_sort_key)

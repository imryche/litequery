import argparse

from litequery.migrations import create_migration, migrate

MIGRATIONS_PATH = "migrations"


def main():
    parser = argparse.ArgumentParser(prog="lq")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    new_parser = subparsers.add_parser("new", help="Create new stuff")
    new_subparsers = new_parser.add_subparsers(
        dest="new_command", help="What to create"
    )

    migration_parser = new_subparsers.add_parser(
        "migration", help="Creeate new migration"
    )
    migration_parser.add_argument(
        "name", help="Migration name (e.g., 'add users table')"
    )

    migrate_parser = subparsers.add_parser("migrate", help="Run database migrations")
    migrate_parser.add_argument("path", help="Path to the SQLite database")

    args = parser.parse_args()

    if args.command == "migrate":
        migrate(args.path, MIGRATIONS_PATH)
    elif args.command == "new":
        if args.new_command == "migration":
            create_migration(args.name, MIGRATIONS_PATH)
        elif args.new_command is None:
            new_parser.print_help()
        else:
            print(f"Unknown new command: {args.new_command}")

    elif args.command is None:
        parser.print_help()
    else:
        print(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()

import argparse

from litequery.migrations import migrate


def main():
    parser = argparse.ArgumentParser(prog="lq")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    migrate_parser = subparsers.add_parser("migrate", help="Run database migrations")
    migrate_parser.add_argument("path", help="Path to the SQLite database")

    args = parser.parse_args()

    if args.command == "migrate":
        migrate(args.path, "migrations")
    elif args.command is None:
        parser.print_help()
    else:
        print(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()

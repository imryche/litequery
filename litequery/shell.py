import subprocess
import sys

from litequery.config import Config


def start_shell(config: Config):
    try:
        subprocess.run(["sqlite3", str(config.database_path)])
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)
    except FileNotFoundError:
        print("Error: sqlite3 command not found")
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(0)

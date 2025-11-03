from runners import ServerRunner
from utils.db import write_db_url_to_alembic_ini


if __name__ == "__main__":
    ServerRunner("localhost", 8000, True).run()
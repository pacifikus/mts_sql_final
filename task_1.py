import os
import time
from dataclasses import dataclass

import matplotlib
import matplotlib.pyplot as plt
import psycopg2
from dotenv import load_dotenv

load_dotenv()  # Load creds from .env

matplotlib.use("Agg")

DB_NAME = "postgres"
HOST = "localhost"
port = "5432"
PG_USER = os.environ.get("PG_USER")
PASSWORD = os.environ.get("PASSWORD")


@dataclass
class RowCount:
    time: float
    connect_1: int
    connect_2: int


class PostgresCommitter:
    def __init__(self, isolation_level=0, duration=60):
        self.isolation_level = isolation_level
        self.duration = duration
        self.connection_string = (
            f"dbname={DB_NAME} user={PG_USER} password={PASSWORD} host={HOST}"
        )
        self.connect_1 = self._create_connect()
        self.connect_2 = self._create_connect()
        self.result = []

    def _create_connect(self):
        connect = psycopg2.connect(self.connection_string)
        connect.set_session(
            isolation_level=self.isolation_level,
        )
        return connect

    def _get_row_count(self, elapsed_time, cursor_1, cursor_2):
        cursor_1.execute("""SELECT COUNT(*) FROM test""")
        count_1 = cursor_1.fetchone()[0]
        cursor_2.execute("""SELECT COUNT(*) FROM test""")
        count_2 = cursor_2.fetchone()[0]

        self.result.append(
            RowCount(
                time=round(elapsed_time, 2),
                connect_1=count_1,
                connect_2=count_2,
            )
        )

    def start(self):
        i = 1
        start_time = time.time()
        prev_time = last_commit_time = start_time
        cur_1 = self.connect_1.cursor()
        cur_2 = self.connect_2.cursor()

        while True:
            cur_1.execute(
                f"""
                INSERT INTO test (id, data) VALUES ({i}, {str(i)})
            """
            )
            elapsed = time.time() - start_time

            # get row count from 2nd connect every 1s
            if time.time() - prev_time >= 1:
                prev_time = time.time()
                self._get_row_count(elapsed, cur_1, cur_2)

            # commit and get row count from 1st connect every 5s
            if time.time() - last_commit_time >= 5:
                self.connect_1.commit()
                last_commit_time = time.time()

            if elapsed >= 15:
                cur_1.close()
                cur_2.close()
                return self.result
            i += 1

    def __enter__(self):
        with psycopg2.connect(self.connection_string) as connect:
            with connect.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE test (
                        id int PRIMARY KEY,
                        data text)
                    """
                )
                connect.commit()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connect_1.close()
        self.connect_2.close()

        with psycopg2.connect(self.connection_string) as connect:
            with connect.cursor() as cur:
                cur.execute("""DROP TABLE if exists test""")
                connect.commit()


def plot_counts(data, level):
    plt.clf()
    x = [item.time for item in data]
    counts_1 = [item.connect_1 for item in data]
    counts_2 = [item.connect_2 for item in data]
    plt.title(f"Isolation level {level}")
    plt.plot(x, counts_1, label="connect 1")
    plt.plot(x, counts_2, label="connect 2")
    plt.xlabel("Time (seconds)")
    plt.ylabel("Row count")
    plt.legend()
    plt.savefig(f"plots/{level}.png")


isolation_levels = {
    "ISOLATION_LEVEL_READ_COMMITTED":
        psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED,
    "ISOLATION_LEVEL_REPEATABLE_READ":
        psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
    "ISOLATION_LEVEL_SERIALIZABLE":
        psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE,
    "ISOLATION_LEVEL_READ_UNCOMMITTED":
        psycopg2.extensions.ISOLATION_LEVEL_READ_UNCOMMITTED,
}

for level, num in isolation_levels.items():
    with PostgresCommitter(isolation_level=num, duration=15) as pc:
        result = pc.start()
        plot_counts(result, level)

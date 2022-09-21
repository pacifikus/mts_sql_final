import os
import time
from dataclasses import dataclass
from random import randint
from threading import Thread

import psycopg2
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()  # Load creds from .env

DB_NAME = "postgres"
HOST = "localhost"
port = "5432"
PG_USER = os.environ.get("PG_USER")
PASSWORD = os.environ.get("PASSWORD")


@dataclass
class ThreadInfo:
    idx: int
    PID: int
    error_count: int

    @property
    def updated_count(self) -> int:
        return 1000 - self.error_count


class Updater(Thread):
    def __init__(self, idx):
        self.isolation_level = \
            psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ
        self.connection_string = (
            f"dbname={DB_NAME} user={PG_USER} password={PASSWORD} host={HOST}"
        )
        self.id = idx
        self.connect = self._create_connect()
        self.error_count = 0

    def _create_connect(self):
        connect = psycopg2.connect(self.connection_string)
        connect.set_session(
            isolation_level=self.isolation_level,
        )
        return connect

    def update(self):
        # print(self.id)
        with self._create_connect() as connect:
            with connect.cursor() as cur:
                for i in range(1001):
                    try:
                        cur.execute(
                            f"SELECT * FROM test WHERE id = {i} FOR UPDATE"
                        )
                        cur.execute(
                            f"UPDATE test set data = {self.id} WHERE id = {i}"
                        )
                        connect.commit()
                    except psycopg2.errors.SerializationFailure:
                        self.error_count += 1
                        connect.rollback()


class PostgresUpdater:
    def __init__(self, process_num):
        self.process_num = process_num
        self.connection_string = (
            f"dbname={DB_NAME} user={PG_USER} password={PASSWORD} host={HOST}"
        )
        self.threads = {}

    def start(self):
        for i in range(self.process_num):
            idx = randint(1, 100000)
            updater = Updater(idx)
            self.threads[idx] = (updater, Thread(target=updater.update))

        for _, thread in self.threads.values():
            thread.start()
        time.sleep(15)

        result = []
        for updater, thread in self.threads.values():
            result.append(
                ThreadInfo(
                    PID=thread.native_id,
                    idx=updater.id,
                    error_count=updater.error_count,
                )
            )
        return result

    def __enter__(self):
        with psycopg2.connect(self.connection_string) as connect:
            with connect.cursor() as cur:
                cur.execute("""DROP TABLE if exists test""")
                cur.execute(
                    """
                    CREATE TABLE test (
                        id int PRIMARY KEY,
                        data text)
                    """
                )
                for i in range(1001):
                    cur.execute(
                        f"""
                        INSERT INTO test (id, data) VALUES ({i}, {str(i)})
                    """
                    )
                connect.commit()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with psycopg2.connect(self.connection_string) as connect:
            with connect.cursor() as cur:
                cur.execute("DROP TABLE if exists test")
                connect.commit()


if __name__ == '__main__':
    with PostgresUpdater(process_num=20) as pu:
        result = pu.start()
        print(
            tabulate(
                [
                    [item.PID, item.error_count, item.updated_count]
                    for item in result
                ],
                headers=["PID", "Errors", "Updated rows"],
            )
        )

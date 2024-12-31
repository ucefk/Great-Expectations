"""Initialise une base de donnée postgres
en créant et remplissant une table.
La table créée sera utiliser comme source de
données pour les tests de Great Expectations.
"""
import csv
from datetime import datetime

import psycopg2

from config.DBConfig import DBConfig

conf = DBConfig()

conn = psycopg2.connect(
    dbname=conf.POSTGRES_DB,
    user=conf.POSTGRES_USER,
    password=conf.POSTGRES_PASSWORD,
    host=conf.POSTGRES_HOST,
    port=conf.POSTGRES_PORT,
)


def create_table(table_name):
    cursor = conn.cursor()
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            VISUAL_ID TEXT,
            ACTIVITY_DATE DATE,
            ACTIVITY_DATETIME TIMESTAMP,
            ACTIVITY_ENDTIME TIMESTAMP,
            REGISTERED_DATE DATE,
            REGISTRATION_STATUS_LABEL TEXT,
            INVENTORY_BUCKET TEXT,
            CODE_PLU TEXT,
            PRODUCT_SEGMENT TEXT,
            PRODUCT_NAME_2 TEXT,
            PRODUCT_NAME TEXT,
            PRODUCT_DETAILS TEXT,
            TICKET_PRICING_SEASON TEXT,
            INVENTORY_CLUSTER TEXT,
            QUANTITY_REGISTRATION INTEGER
        )
    """
    )
    conn.commit()


def parse_date(date: str, mformat: str):

    date_splits = date.split(" ")

    mdate = ""

    if len(date_splits) > 1:
        date_part, time_part = date_splits
        time_part = time_part[:-3]
        mdate = f"{date_part} {time_part}"

    if mdate != "":
        parsed_date = datetime.strptime(mdate, mformat)
    else:
        parsed_date = datetime.strptime(date, mformat)
    return parsed_date


def insert_data_from_csv(file_path, table_name):
    with open(file_path, "r") as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)
        cursor = conn.cursor()

        for row in csvreader:

            cursor.execute(
                f"""
                INSERT INTO {table_name} (
                    VISUAL_ID,
                    ACTIVITY_DATE,
                    ACTIVITY_DATETIME,
                    ACTIVITY_ENDTIME,
                    REGISTERED_DATE,
                    REGISTRATION_STATUS_LABEL,
                    INVENTORY_BUCKET,
                    CODE_PLU,
                    PRODUCT_SEGMENT,
                    PRODUCT_NAME_2,
                    PRODUCT_NAME,
                    PRODUCT_DETAILS,
                    TICKET_PRICING_SEASON,
                    INVENTORY_CLUSTER,
                    QUANTITY_REGISTRATION
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row[1],
                    parse_date(row[2], "%Y-%m-%d"),
                    parse_date(row[3], "%Y-%m-%d %H:%M:%S.%f"),
                    parse_date(row[4], "%Y-%m-%d %H:%M:%S.%f"),
                    parse_date(row[5], "%Y-%m-%d"),
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[12],
                    row[13],
                    row[14],
                    int(row[15]),
                ),
            )
        conn.commit()


def drop_table(table_name):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()


if __name__ == "__main__":
    table_name = "TICKET_REGISTRATION_VIEW_V2"
    drop_table(table_name)
    create_table(table_name)
    csv_file_path = conf.csv_file_path
    insert_data_from_csv(csv_file_path, table_name)

conn.close()

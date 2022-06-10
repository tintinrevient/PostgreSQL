# pip install psycopg2
import psycopg2
from psycopg2 import sql
import psycopg2.extras
import random
import uuid
from datetime import datetime, timedelta
import time


# config
batch_num = 10
batch_size = 100000
total_days = 60

# database setup
# create user admin with encrypted password 'admin';
# grant usage on schema featurestore to admin;
# grant select, delete, update, insert on all tables in schema featurestore to admin;
# grant usage on FeatureStore.ServingFeatureIdSec_seq to admin;

conn = psycopg2.connect(
    host="localhost",
    database="alfredgateway",
    user="admin",
    password="admin"
)

conn.autocommit = False

def create_table(cursor, today) -> None:

    tomorrow = today + timedelta(days=1)
    query = "CREATE TABLE IF NOT EXISTS featurestore.servingfeature_y{}m{}d{} PARTITION OF featurestore.servingfeature FOR VALUES FROM ('{}') TO ('{}')"

    try:
        cursor.execute(query.format(today.year, today.month, today.day, today.strftime('%Y-%m-%d'), tomorrow.strftime('%Y-%m-%d')))
        conn.commit()
        print("Create the table:", "featurestore.servingfeature_y{}m{}d{}".format(today.year, today.month, today.day))
    except Exception as ex:
        conn.rollback()
        print(ex)


def insert_tuples(cursor, today) -> None:
    start = time.time()

    table_name = "featurestore.servingfeature_y{}m{}d{}".format(today.year, today.month, today.day)
    query = "INSERT INTO {} (featureDefinitionReference, entitiesIdentifier, valueString, uuid, creationDate, createdTimestamp, eventTimestamp, expires) VALUES %s"

    batch_uuid = str(uuid.uuid4())
    today_date_str = today.strftime('%Y-%m-%d')
    today_timestamp_str = today.strftime('%Y-%m-%d %H:%M:%S')

    for batch_idx in range(batch_num):
        tuples = []

        for row_idx in range(batch_size):
            uuid_str = str(uuid.uuid4())

            tuples.append(("fs_company_merchant_dummy__company_merchant_total_received_payments_3D", f"company_&&_{uuid_str}_!!_merchant_&&_{uuid_str}", random.randint(0, 1e6), batch_uuid, today_date_str, today_timestamp_str, today_timestamp_str, today_timestamp_str))
            tuples.append(("fs_company_merchant_dummy__company_merchant_total_received_payments_7D", f"company_&&_{uuid_str}_!!_merchant_&&_{uuid_str}", random.randint(0, 1e6), batch_uuid, today_date_str, today_timestamp_str, today_timestamp_str, today_timestamp_str))
            tuples.append(("fs_company_merchant_dummy__company_merchant_avg_eur_amount_3D", f"company_&&_{uuid_str}_!!_merchant_&&_{uuid_str}", random.randint(0, 1e6) / 100.0, batch_uuid, today_date_str, today_timestamp_str, today_timestamp_str, today_timestamp_str))
            tuples.append(("fs_company_merchant_dummy__company_merchant_avg_eur_amount_7D", f"company_&&_{uuid_str}_!!_merchant_&&_{uuid_str}", random.randint(0, 1e6) / 100.0, batch_uuid, today_date_str, today_timestamp_str, today_timestamp_str, today_timestamp_str))

        try:
            psycopg2.extras.execute_values(cursor, query.format(table_name), tuples)
            conn.commit()

        except Exception as ex:
            conn.rollback()
            print(ex)

        print(f"Batch {batch_idx + 1} has been inserted with {batch_size} tuples.")


    end = time.time()
    print(f"{end - start} seconds are taken to insert for {batch_num} batches with batch_size as {batch_size}.")


def drop_expired_table(cursor, today, retention_days) -> None:

    expired_day = today - timedelta(days=retention_days)
    expired_table_name = "featurestore.servingfeature_y{}m{}d{}".format(expired_day.year, expired_day.month, expired_day.day)

    query = "DROP TABLE IF EXISTS {}"
    try:
        cursor.execute(query.format(expired_table_name))
        conn.commit()
        print("Drop the table:", expired_table_name)
    except Exception as ex:
        conn.rollback()
        print(ex)


if __name__ == '__main__':

    with conn.cursor() as cursor:

        for days in range(total_days):
            today = datetime.today() + timedelta(days=days)

            # step 1: create table
            create_table(cursor, today)

            # step 2: insert into table
            insert_tuples(cursor, today)

            # step 3: delete expired table
            drop_expired_table(cursor, today, retention_days=7)

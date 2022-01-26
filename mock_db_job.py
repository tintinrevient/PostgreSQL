# pip install psycopg2
import psycopg2
import psycopg2.extras
import random
import uuid
import datetime
import time


# Connection
conn = psycopg2.connect(
    host="localhost",
    database="test",
    user="admin",
    password="admin"
)

conn.autocommit = False

# Settings
batch_num = 50000
batch_size = 100
update_rows = 50000


def drop_index(cursor) -> None:
    start = time.time()

    query = '''
    DROP INDEX IF EXISTS test_schema.test_table_uuid;
    DROP INDEX IF EXISTS test_schema.test_table_feature_entity;
    '''
    cursor.execute(query)
    conn.commit()

    end = time.time()
    print(f"{end - start} seconds are taken to drop the indexes.")


def create_index(cursor) -> None:
    start = time.time()

    query = '''
    CREATE INDEX test_table_uuid ON test_schema.test_table(uuid);
    CREATE INDEX test_table_feature_entity ON test_schema.test_table(md5(featureDefRef::text || entityId::text));
    '''
    cursor.execute(query)
    conn.commit()

    end = time.time()
    print(f"{end - start} seconds are taken to create the indexes.")


def insert_tuples(cursor) -> None:
    start = time.time()

    query = '''
    INSERT INTO test_schema.test_table (
        featureDefRef,
        entityId,
        valueStr,
        active,
        uuid,
        timestamp,
        expire
    ) VALUES %s;
    '''

    for batch_idx in range(batch_num):
        tuples = []
        for row_idx in range(batch_size):
            row_id = batch_idx * batch_size + row_idx
            feature_def_ref = f"feature_def_ref_{row_id}"
            entity_id = f"entity_id_{row_id}"
            active = "true" if bool(random.getrandbits(1)) else "false"
            uuid_str = str(uuid.uuid4())
            now = datetime.datetime.utcnow() + datetime.timedelta(days=random.randrange(-10, 10)) # [-10, 10)
            expire = now + datetime.timedelta(days=random.randrange(1, 8)) # [1, 8)

            tuples.append((feature_def_ref, entity_id, "value_str", active, uuid_str, now, expire))
        try:
            psycopg2.extras.execute_values(cursor, query, tuples)
            conn.commit()

            if batch_idx % 100 == 0:
                print(f"Batch {batch_idx + 1} has been inserted with {batch_size} tuples.")

        except Exception as ex:
            print(ex)
            conn.rollback()

    end = time.time()
    print(f"{end - start} seconds are taken to insert for {batch_num} batches with batch_size as {batch_size}.")


def update_tuples(cursor) -> None:
    start = time.time()

    query = '''
    INSERT INTO test_schema.test_table (
        featureDefRef,
        entityId,
        valueStr,
        active,
        uuid,
        timestamp,
        expire
    ) VALUES %s;
    
    UPDATE test_schema.test_table
    SET active = false
    WHERE uuid != %s AND md5(featureDefRef::text || entityId::text) = md5(%s || %s);
    '''

    for row in range(update_rows):
        row_id = random.randrange(0, batch_num * batch_size)
        feature_def_ref = f"feature_def_ref_{row_id}"
        entity_id = f"entity_id_{row_id}"
        uuid_str = str(uuid.uuid4())
        now = datetime.datetime.utcnow() + datetime.timedelta(days=random.randrange(-10, 10))  # [-10, 10)
        expire = now + datetime.timedelta(days=random.randrange(1, 8))  # [1, 8)

        tuple = (feature_def_ref, entity_id, "value_str", "true", uuid_str, now, expire)
        cursor.execute(query, (tuple, uuid_str, feature_def_ref, entity_id))
        conn.commit()

        if row % 100 == 0:
            print(f"Tuple {row + 1} has been inserted or updated.")

    end = time.time()
    print(f"{end - start} seconds are taken to update the table.")

def truncate_inactive_expired_tuples(cursor, days) -> None:
    start = time.time()

    query = '''
    CREATE TEMP TABLE temp_table AS
    SELECT * FROM test_schema.test_table
    WHERE active = true AND date_part('day', now()::timestamp - timestamp::timestamp) < %s;
    
    TRUNCATE test_schema.test_table;
    
    INSERT INTO test_schema.test_table SELECT * FROM temp_table;
    
    DROP TABLE temp_table;
    '''
    cursor.execute(query, str(days))
    conn.commit()

    end = time.time()
    print(f"{end - start} seconds are taken to truncate the table.")


def delete_inactive_expired_tuples(cursor, days) -> None:
    start = time.time()

    query = '''
    CREATE TEMP TABLE temp_table AS
    SELECT featureId FROM test_schema.test_table
    WHERE active = false AND date_part('day', now()::timestamp - timestamp::timestamp) >= %s;

    DELETE FROM test_schema.test_table t1 USING temp_table t2
    WHERE t1.featureId = t2.featureId;

    DROP TABLE temp_table;
    '''
    cursor.execute(query, str(days))
    conn.commit()

    end = time.time()
    print(f"{end - start} seconds are taken to delete from the table.")


if __name__ == '__main__':
    with conn.cursor() as cursor:

        insert_tuples(cursor)
        time.sleep(2 * 60) # sleep 5min

        update_tuples(cursor)
        time.sleep(2 * 60)  # sleep 5min

        truncate_inactive_expired_tuples(cursor, 3)
        time.sleep(2 * 60)  # sleep 5min

        insert_tuples(cursor)
        time.sleep(2 * 60)  # sleep 5min

        update_tuples(cursor)
        time.sleep(2 * 60)  # sleep 5min

        truncate_inactive_expired_tuples(cursor, 3)
        time.sleep(2 * 60)  # sleep 5min
# pip install psycopg2
import psycopg2
import psycopg2.extras
import random
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

def query(cursor, row_id) -> None:
    start = time.time()

    query = '''
    SELECT valueStr FROM test_schema.test_table 
    WHERE md5(featureDefRef::text || entityId::text) = md5(%s || %s)
        AND active = true
        AND expire > now()
    ORDER BY timestamp DESC
    LIMIT 1
    '''
    cursor.execute(query, (f"feature_def_ref_{row_id}", f"entity_id_{row_id}"))
    conn.commit()

    end = time.time()
    print(f"{end - start} seconds are taken for query.")


if __name__ == '__main__':
    max_row_id = 5000
    query_num = 1000
    with conn.cursor() as cursor:
        for query_idx in range(query_num):
            max_row_id = max_row_id * (1 + (query_idx // 10))
            row_id = random.randrange(1, max_row_id)
            query(cursor, row_id)
            time.sleep(5) # sleep 5 sec



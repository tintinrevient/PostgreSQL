#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Inspired by https://dzone.com/articles/redis-vs-memcached-2021-comparison

ROWS = [1000, 10000, 100000,
        1000000]  # 1mio rows will be 66 MB data size + 50 MB index size so make sure shared_buffers is 128MB+
LOOPS = 10
TEST_NAME = 'run1'

import psycopg2
from random import random
from time import time

# connstr = "host=localhost port=5433 dbname=postgres user=postgres sslmode=disable options='-c synchronous_commit=off'"
# conn = psycopg2.connect(connstr)

conn = psycopg2.connect(
    host="localhost",
    port="5433",
    dbname="postgres",
    user="postgres",
    password=""
)
conn.autocommit = True
cur = conn.cursor()

print('connection OK')

sql_setup = '''
  DROP TABLE IF EXISTS kv_test;
  CREATE UNLOGGED TABLE kv_test(key text NOT NULL, value text NOT NULL);
  ALTER TABLE kv_test SET (AUTOVACUUM_ENABLED=FALSE);
  CREATE INDEX ON kv_test (key);
  CREATE EXTENSION IF NOT EXISTS pg_prewarm;
  CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
  CREATE TABLE IF NOT EXISTS results AS SELECT '' test_name, 0::int test_rows, * FROM  pg_stat_statements WHERE false;
'''
# sql_ins = '''insert into kv_test values (%s, %s);'''
sql_truncate = 'truncate table kv_test;'
sql_discard_all = 'discard all'
sql_analyze = 'vacuum analyze kv_test'
sql_ins_prep = '''prepare kv_ins as insert into kv_test values ($1, $2);'''
sql_ins_exec = '''execute kv_ins (%s, %s);'''
# sql_sel = '''select key, value from kv_test where key = %s;'''
sql_sel_prep = '''prepare kv_sel as select key, value from kv_test where key = $1;'''
sql_sel_exec = '''execute kv_sel (%s)'''
sql_prewarm = '''select pg_prewarm('kv_test_key_idx'), pg_prewarm('kv_test');'''
sql_clear_results = '''delete from results where test_name = %s;'''
sql_pgss_reset = '''select pg_stat_statements_reset();'''
sql_pgss_store = '''insert into results select %s, %s, * from pg_stat_statements where query ~ 'prepare.*kv_test';'''
sql_pgss_get_means = '''
  select
    (select mean_exec_time from pg_stat_statements where query ~ 'insert.*kv_test') ins,
    (select mean_exec_time from pg_stat_statements where query ~ 'select.*kv_test') sel;
'''

start_time = time()

cur.execute(sql_setup)
print('DDL setup OK')
cur.execute(sql_clear_results, (TEST_NAME,))
print('cleared results for possible previous runs of TEST_NAME = {}'.format(TEST_NAME))

for rows in ROWS:
    print('\n*** testing with ROWS = {} ***\n'.format(rows))
    ins_total_time = 0
    sel_total_time = 0

    cur.execute(sql_pgss_reset)
    precreated_random_numbers = []
    print('pre-creating {} random floats...'.format(rows))
    t0 = time()
    for i in range(0, rows):
        precreated_random_numbers.append(str(random()))
    print('done in {}s...\n'.format(time() - t0))

    for loop in range(0, LOOPS):
        print('* loop {} *'.format(loop))

        cur.execute(sql_truncate)
        cur.execute(sql_discard_all)

        print('inserting {} rows one-by-one in async commit mode'.format(rows))
        cur.execute(sql_ins_prep)

        t_ins_start = time()
        for i in range(0, rows):
            cur.execute(sql_ins_exec, (precreated_random_numbers[i], precreated_random_numbers[i],))
        ins_total_time += (time() - t_ins_start)

        cur.execute(sql_analyze)

        print('reading {} rows one-by-one'.format(rows))
        cur.execute(sql_prewarm)
        cur.execute(sql_sel_prep)

        t_sel_start = time()
        for i in range(0, rows):
            cur.execute(sql_sel_exec, (precreated_random_numbers[i],))
            cur.fetchone()
        sel_total_time += (time() - t_sel_start)

    cur.execute(sql_pgss_store, (TEST_NAME, rows))

    cur.execute(sql_pgss_get_means)
    ins_mean, sel_mean = cur.fetchone()

    print('\n= Totals for {} rows as measured from app side ='.format(rows))
    print('ins total (s): {}'.format(ins_total_time / LOOPS))
    print('sel total (s): {}'.format(sel_total_time / LOOPS))
    print('\n= Averages for {} rows measured from DB side ='.format(rows))
    print('ins_mean (ms): {}, calculated total (s): {}'.format(ins_mean, ins_mean * rows / 1000))
    print('sel_mean (ms): {}, calculated total (s): {}'.format(sel_mean, sel_mean * rows / 1000))

print('\nDONE in {} s'.format(int(time() - start_time)))
print('''execute "select test_rows, mean_exec_time, stddev_exec_time, query from results where test_name = '{}';" for full results '''.format(TEST_NAME))
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 24 09:58:15 2021

@author: Michael Murphy
"""
from sqlalchemy import create_engine
import pandas as pd
import time

# Create database connection
engine = create_engine('sqlite:///hash_test.db')
conn = engine.connect()

DATA_FILE = 'test_data_hash.csv'
# Write table to database if it does not exist:
try:
    pd.read_csv(DATA_FILE).to_sql('test_table', conn)
except ValueError:
    pass

df_test_hash = pd.read_sql_query("SELECT hash FROM test_table", conn)
hash_set0 = set(pd.Series(df_test_hash.hash, dtype='string'))
hash_set = hash_set0.copy()

TEST_HASH_LEN = 100
RANDOM_STATE = 1

df1_test_hash = df_test_hash.sample(TEST_HASH_LEN, random_state=RANDOM_STATE)
hash_list1 = [i.replace('2019', '2021') for i in df1_test_hash.hash.unique().tolist()]

test_hash_set = set(hash_list1)

time_start1 = time.perf_counter()

# test each member in test_hash_set for membership in hash_set
for h in test_hash_set:
    test = h in hash_set
    print(test)
    if not test:
        hash_set.add(h)
    
    
time_end1 = time.perf_counter()
time_elapsed1 = time_end1 - time_start1

print("Initial number of records in hash_set: " + str(len(hash_set0)))
print("Final number of records in hash_set: " + str(len(hash_set)))
print("Number of new records: " + str(len(hash_set) - len(hash_set0)))
print("Time elapsed: " + str(time_elapsed1) + " seconds")
#from src.tools.dictionaries import *
import time
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv
import os

start_time = time.time()


load_dotenv(find_dotenv())
db4_host = os.getenv("DB4_PROD_HOST")
db4_user = os.getenv("DB4_PROD_USER")
db4_pass = os.getenv("DB4_PROD_PASS")
db4_schema = os.getenv("DB4_PROD_SCHEMA")
db4_creds = (db4_host,db4_user,db4_pass,db4_schema)
stage_host = os.getenv("DBSTAGE_HOST")
stage_user = os.getenv("DBSTAGE_USER")
stage_pass = os.getenv("DBSTAGE_PASS")
stage_schema = os.getenv("DBSTAGE_SCHEMA")
stage_creds = (stage_host,stage_user,stage_pass,stage_schema)


def sync_stage_insert(table, db4_creds, stage_creds):
    print(f"Loading stage {table}")
    host, user, pw, db = db4_creds
    db4_url = f"mysql+pymysql://{user}:{pw}@{host}:3306/{db}?local_infile=1"
    prod_engine = create_engine(db4_url)
    df = pd.read_sql(f'SELECT * FROM {table}', con=prod_engine)

    host2, user2, pw2, db2 = stage_creds
    stage_url = f"mysql+pymysql://{user2}:{pw2}@{host2}:3306/{db2}?local_infile=1"
    stage_engine = create_engine(stage_url)

    # create temp table, load pandas dataframe to temp table, insert ignore temp table data into table
    with stage_engine.connect().execution_options(autocommit=True) as connection:
        connection.execute(f'Drop temporary table if exists temp_{table};')
        connection.execute(f'Create temporary table if not exists temp_{table} as select * from {table}')
        df.to_sql(name=f'temp_{table}', con=stage_engine, if_exists='append', index=False)
        #connection.execute(f'INSERT IGNORE INTO {table} (SELECT * FROM temp_{table})')
        connection.execute(f'REPLACE INTO {table} (SELECT * FROM temp_{table})')
        # update_sql = f'INSERT INTO {table} (SELECT * FROM temp_{table}) ON DUPLICATE KEY UPDATE'

def sync_stage_replace(table, db4_creds, stage_creds):
    print(f"Loading stage {table}")
    host, user, pw, db = db4_creds
    db4_url = f"mysql+pymysql://{user}:{pw}@{host}:3306/{db}?local_infile=1"
    prod_engine = create_engine(db4_url)
    df = pd.read_sql(f'SELECT * FROM {table}', con=prod_engine)

    host2, user2, pw2, db2 = stage_creds
    stage_url = f"mysql+pymysql://{user2}:{pw2}@{host2}:3306/{db2}?local_infile=1"
    stage_engine = create_engine(stage_url)

    # create temp table, load pandas dataframe to temp table, insert ignore temp table data into table
    with stage_engine.connect().execution_options(autocommit=True) as connection:

        print(f'Deleting {table} data')
        connection.execute(f'DELETE FROM {table}')
        print(f'Inserting {table} data')
        connection.execute(f'INSERT INTO {table} (SELECT * FROM temp_{table})')
        df.to_sql(name=f'{table}', con=stage_engine, if_exists='append', index=False)



if __name__ == "__main__":
    #sync_stage_insert('homepage_content', db4_creds, stage_creds)
    #sync_stage_replace('homepage_content', db4_creds, stage_creds)
    sync_stage_replace('attributes_values', db4_creds, stage_creds)

t_sec = round(time.time() - start_time)
(t_min, t_sec) = divmod(t_sec, 60)
(t_hour, t_min) = divmod(t_min, 60)
print('Time passed: {}hour:{}min:{}sec'.format(t_hour, t_min, t_sec))

import os
import tempfile
import pandas as pd
import paramiko
from sqlalchemy import create_engine
import pymysql

def get_sftp(credentials, remote_directory, file_prefix, num_files=1):
    host, user, pw = credentials
    transport = paramiko.Transport((host, 22))
    transport.connect(None, user, pw)
    sftp = paramiko.SFTPClient.from_transport(transport)
    file_attr = sftp.listdir_attr(path=remote_directory)
    files = list(filter(lambda f: f.filename.startswith(file_prefix), file_attr))
    files.sort(key=lambda f: f.filename)  # assumes filename is file_prefix_yyyymmdd
    dfs = []
    for f in files[-num_files:]:  # gets the latest n files where n is num_files
        with sftp.file(remote_directory + f.filename, "r") as csv:
            dfs.append(pd.read_csv(csv, dtype="object"))
    sftp.close()
    transport.close()
    return dfs

def get_mysql(credentials, select):
    with tempfile.NamedTemporaryFile() as tmp:
        #sed command replaced tabs with commas; this is not ideal where string in field contains commas
        cmd = (
            f"""mysql -h {credentials[0]} -u {credentials[1]} -p{credentials[2]} -e "{select}" | sed 's/\t/,/g' > {tmp.name}.csv"""
            #f"""mysql -h {credentials[0]} -u {credentials[1]} -p{credentials[2]}  -e "{select}"  > {tmp.name}.csv"""
        )
        if os.system(cmd) == 0:
            return pd.read_csv(tmp.name + '.csv',sep=',')

def mysql_cursor(credentials):
    cursor = pymysql.connect(host=credentials[0], port=3306, user=credentials[1], password=credentials[2], db=credentials[3], autocommit=True)
    return cursor.cursor()


def put_mysql(credentials, df, table_name, cols=""):
    host, user, pw, db = credentials
    url = f"mysql+pymysql://{user}:{pw}@{host}:3306/{db}?local_infile=1"
    engine = create_engine(url)
    with engine.connect() as connection:
        with tempfile.NamedTemporaryFile() as tmp:
            df.to_csv(tmp.name + ".csv", index=False)
            connection.execute(
                f"""
                load data local infile '{tmp.name}.csv'
                replace into table {table_name}
                character set latin1
                fields terminated by ','
                enclosed by '"'
                lines terminated by '\\n'
                ignore 1 lines
                {cols}"""
            )

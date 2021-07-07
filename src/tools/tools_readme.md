# tools.py
This document contains universal functions to handle etl chores.


------------


###get_sftp
This function returns the most recent n files in a remote directory hosted on an sftp. Designed for static file name conventions `file_prefix_yyyymmdd.csv` for example `email_kpi_20200725.csv`.

#### parameters:
- **credentials** a tuple of the sftp hostname, username, and password
- **remote_directory** the file path of the file you are looking for
- **file_prefix** the name of the file
- **num_files** *(optional, default = 1)* set this variable to change the number of recent files to return

#### returns:
the csvs from sftp as a list of pandas dataframes. If num_files = 1, then the list has just one element.

------------
###get_mysql
This function returns the result of a query as a pandas dataframe. For performance, it run a mysql bash command and output it to csv and then use panda's read_csv, which is three times faster than panda's read_sql. 

#### parameters:
- **credentials** a tuple of the database hostname, user, password, and database name
- **select** the sql query as a string

#### returns:
the query results as a pandas dataframe.

------------
###put_mysql
This function uses mysql `replace into` functionality to import a dataframe into a table in dermstore_reporter. If the table doesn't exist, it gets created with (poorly) guessed data types.
NOTE: This should only be used if the destination table has a unique key. Otherwise, the 'Replace Into' statement could import duplicate records.

#### parameters:
- **credentials** a tuple of the database hostname, user, password, and database name
- **df** the dataframe that you are loading into the table
- **table_name** the name of the table in dermstore_reporter--if the table name doesn't exist, it gets created.
- **cols** *(optional, default = empty string)* set this variable to map the df columns to my sql. Example `"(@col1,@col2,@col3,@col4) set customer_email=@col1, customer_id=@col2, unsubscribe_reason=@col3, unsubscribe_date=@col4;"`

#### returns:
None

-------------
### mysql_cursor
This cursor function enables a MySQL working environment. This is useful for dropping and creating temp tables, setting variables, etc, whenever multiple MySQL operations are required.

#### parameters:
- **credentials** a tuple of the database hostname, user, password, and database name

#### returns:
MySQL cursor to execute a variety of MySQL statements.


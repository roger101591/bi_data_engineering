import string
import codecs
import csv
import pymysql
from re import split



def connect(credentials):
    cursor = pymysql.connect(host=credentials[0], port=3306, user=credentials[1],
                             password=credentials[2], db=credentials[3], autocommit=True)
    cursor.autocommit(True)
    return cursor



def getCursor(connection):
    """Get the Default cursor"""
    return getDictCursor(connection)


def getDictCursor(connection):
    return connection.cursor(pymysql.cursors.DictCursor)

def getTupleCursor(connection):
    return connection.cursor(pymysql.cursors.Cursor)

def getCol(connection, query, fieldname):
    cols = []
    rows = getRows(connection, query)
    for row in rows:
        cols.append(row[fieldname])
    return cols

def getRow(connection, query, cursor=None):
    if(cursor == None):
        cursor = getTupleCursor(connection)
    cursor.execute(query)
    return cursor.fetchone()

def getOne(connection, query):
    row = getRow(connection, query)
    if(row == None or len(row) == 0):
        return None
    return row[0]

def getRowsAndColNames(connection, query, cursor=None):
    if(cursor == None):
        cursor = getCursor(connection)
    cursor.execute(query)
    colNames = []
    for fld in cursor.description:
        colNames.append(fld[0])
    return [cursor.fetchall(), colNames]

def getRows(connection, query, cursor = None):
    if(cursor == None):
        cursor = getCursor(connection)
    cursor.execute(query)
    return cursor.fetchall()

def printQuery(connection, query, title=None, cursor=None):
    rows = getRows(connection, query, cursor)
    printRows(rows, title)

def printRow(row, title=None):
    rows = [row]
    printRows(rows, title)

def printRows(rows, title=None):
    if (title != None):
        title = title + " :: "
    else:
        title = ""
    title = title  + (str(len(rows)) + " Row")
    if(len(rows) > 1):
        title = title + "s"
    print("".rjust(len(title), "="))
    print(title)
    print("".rjust(len(title), "="))
    count_width = len(str(len(rows)))
    count = 1
    for row in rows:
        if(len(row) > 0):
            print("| [%s] |" % str(count).rjust(count_width, "0"),)
            for col in row:
                print(col, "=", row[col], " | ",)
            print("\n")
        count = count + 1

def quote(text):
    text = str(text)
    text = text.replace("'", "\\'");
    text = text.replace('"', '\\"');
    return text

def update(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()

def update_many(connection, query):
    cursor = connection.cursor()
    cursor.executemany(query, None)
    connection.commit()

def last_insert_id(connection):
	return getOne(connection, "select last_insert_id()")

def truncate(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()

def importCsv(connection, table_name, file_name, import_ids, row_skip = 0, convert_ids = []):

    colquery = 'select * from %s limit 1' % table_name
    [data, colNames] = getRowsAndColNames(connection,colquery)
    insert_prefix = 'insert ignore into %s (' % table_name
    col_count = 0
    for colName in colNames:
        if import_ids[col_count] == -1:
            pass
        else:
            insert_prefix += '%s,' % colName
        col_count += 1
    insert_prefix = insert_prefix[:-1] + ')\nvalues'

    print(insert_prefix)

    insert_values = ''

    row_count = 0


    with codecs.open(file_name,'r', encoding='ascii', errors='ignore') as f:
        import_file = csv.reader(f, quotechar='"', dialect='excel')
        import_list = list(import_file)
        print(len(import_list),len(import_list[0]))
        for row in import_list:
            if row is not None and row_count >= row_skip:
                new_row = '('
                for import_id in import_ids:
                    if import_id in convert_ids:
                        row[import_id - 1] = convertToDate(row[import_id - 1])

                    #print import_id
                    try:
                        if import_id > 0:
                            new_row += '"%s",' % str.replace(row[import_id - 1], '"', '')
                    except:
                        new_row += import_id + ','
                insert_values += new_row
                insert_values = insert_values[:-1] + '),\n'
            row_count += 1
            if row_count % 10000 == 0:
                print(row_count)

    return insert_prefix + ' ' + insert_values[:-3] + ');'

def convertToDate(value):
    #print value
    date_time_split = split(value,' ')
    date_split = split(date_time_split[0],'/')
    if len(date_split) == 1:
        date_split = split(date_time_split[0],'-')
    year = date_split[2]
    month = date_split[0]
    day = date_split[1]

    #print year,month,day

    if len(date_time_split) > 1:
        sql_date = '%s-%s-%s %s' % (year,month,day,date_time_split[1])
    else:
        sql_date = '%s-%s-%s' % (year,month,day)

    #print sql_date

    return sql_date

def executeMany(connection, query):
    temp_query = ''
    results = ''

    for row in query:
        if string.strip(row) != ';':
            temp_query += row + '\n'
        else:
            cursor = connection.cursor()
            cursor.execute(temp_query)
            results = cursor.fetchall()
            colNames = []
            if cursor.description != None:
                for fld in cursor.description:
                    colNames.append(fld[0])
            temp_query = ''


    return [results, colNames]
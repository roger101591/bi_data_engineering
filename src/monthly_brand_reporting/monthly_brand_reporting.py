import connector
import string
import sys
import wpp_auto.send_email as send_email
import datetime
import reportr_update as reportr
import string

def setBrand(connection, brand_id):
    brand_set_query = 'set @brand_id = %s\n;' % brand_id
    connector.update(connection, brand_set_query)
def setFiscalYear(connection, fy_custom=None):
    if fy_custom:
        fy_query = 'set @fiscal_year = %s;' % str(fy_custom)
    else:
        fy_query = 'set @fiscal_year = (select fiscal_year from fiscal_calendar where calendar_date = date(now()) - interval 1 day);'
    connector.update(connection, fy_query)
    get_fy_query = 'select @fiscal_year;'
    fy = connector.getRows(connection, get_fy_query, connection.cursor())
    return fy
def setCalendarYear(connection, custom_year=None):
    if custom_year:
        query = 'set @fiscal_year = %s;' % str(custom_year)
    else:
        query = 'set @fiscal_year = %s;' % str(calendar_year)
    connector.update(connection, query)
def getBrandInfo(connection):
    query = 'select m.att_id as brand_id,av.att_name,coalesce(m.merchant_id,0) as genuser_id from attribute_brand_values m inner join attributes_values av on m.att_id = av.att_id where av.att_status & 1 = 1;'
    brand_info = connector.getRows(connection, query, connection.cursor())
    return brand_info
def main():
    connection = connector.liveConnect()
    brand_info = getBrandInfo(connection)
    for i in range(0, len(brand_info)):
        # for i in range(0,1):
        setBrand(connection, brand_info[i][0])
        fy = 2020
        setCalendarYear(connection, fy)
        brand_name = brand_info[i][1]
        brand_name = brand_name.replace('.', '')
        brand_name = brand_name.replace(' ', '')
        brand_name = brand_name.replace('-', '')
        brand_name = brand_name.replace("'", '')
        brand_id = brand_info[i][0]
        print(fy)
        reportr.executeReportr(connection, 39, custom_file_name='%s_%s_%s' % (brand_name, brand_id, fy))
    if __name__ == '__main__':
        sys.exit(main())


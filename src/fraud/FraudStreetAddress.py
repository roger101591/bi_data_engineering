from dotenv import load_dotenv, find_dotenv
from tools.tools import *
import os
import usaddress
import json

load_dotenv(find_dotenv())
host = os.getenv("DB4_RPT_HOST")
user = os.getenv("DB4_RPT_USER")
passw = os.getenv("DB4_RPT_PASS")
schema = os.getenv("DB4_RPT_SCHEMA")
creds = (host, user, passw, schema)

mysql_cursor = mysql_cursor(creds)

def address_splice(address):
    try:
        return usaddress.tag(address)
    except:
        return [{'AddressConversion':'Error'}]

#function to explode addresses and merge to df
def explode_addresses(address_data,field):
    address_data['tag_response'] = address_data[field].apply(address_splice)
    address_data['tags'] = address_data.apply(lambda row: row['tag_response'][0], axis=1)
    tags_combined = json.dumps(list(address_data['tags']))
    df_parsed = pd.json_normalize(json.loads(tags_combined))
    exploded_addresses = address_data.join(df_parsed)
    return exploded_addresses


def metrics(exploded_addresses):
    condense_df = exploded_addresses[['order_id', 'cust_id', 'address_id', 'StreetName', 'StreetNamePostType',
                                      'address_city', 'address_state', 'address_zip', 'address_country']]
    unique_counts = condense_df.groupby(['StreetName', 'StreetNamePostType', 'address_city', 'address_state',
                                         'address_zip', 'address_country']).agg(['nunique']).reset_index()
    sorted_metrics = unique_counts.sort_values([('cust_id', 'nunique'), ('address_id', 'nunique'),
                                                ('order_id', 'nunique')], ascending=[False, False, False])

    return sorted_metrics

def address_data(days):
    query = '''
        select 
        o.order_id,
        o.order_date,
        a.address_id,
        a.cust_id,
        address_nickname,
        address_first_name,
        address_last_name,
        address_company,
        address_field_one,
        address_field_two,
        address_field_three,
        address_city,
        address_state,
        address_zip,
        address_country,
        address_phone_one,
        order_status
        from 
        orders o
        inner join addresses a on o.ship_address_id = a.address_id
        where
                o.order_source not in ('autorefill', 'reship', 'autorefill_sequ')
                and order_date >= now() - interval ''' + str(days) + ''' day
                and order_status in (100,110,400,500,700,800,900,1000,1300,1400)
                and o.cobrand_id not in (2,23,24,29)
        order by address_zip 
        '''

    mysql_cursor.execute(query)
    result = mysql_cursor.fetchall()
    address_data = pd.DataFrame(data=result, index=None,
                                columns = ['order_id',
                                            'order_date',
                                            'address_id',
                                            'cust_id',
                                            'address_nickname',
                                            'address_first_name',
                                            'address_last_name',
                                            'address_company',
                                            'address_field_one',
                                            'address_field_two',
                                            'address_field_three',
                                            'address_city',
                                            'address_state',
                                            'address_zip',
                                            'address_country',
                                            'address_phone_one',
                                            'order_status']
                               )
    return address_data

#def to create dataframe


def nlp_metrics_df(data_set,parsed_field):
    exploded_addresses3 = explode_addresses(data_set, parsed_field)
    metricsdf = metrics(exploded_addresses3)
    metricsdf.reset_index(level=0, inplace=True)

    metricsdf.columns = ['Index','StreetName','StreetNamePostType','address_city','address_state',
                            'address_zip','address_country', 'distinct_order_ids',
                            'distinct_cust_ids','distinct_address_ids']

    column_order = ['Index','StreetName','StreetNamePostType','address_city','address_state',
                    'address_zip','address_country',
                    'distinct_order_ids',
                    'distinct_cust_ids','distinct_address_ids']
    metricsdf = metricsdf.reindex(columns=column_order)
    return metricsdf

def truncate_table():
    query = 'delete from fraud_suspicious_addresses'
    mysql_cursor.execute(query)

def load_metrics(data):
<<<<<<< Updated upstream
    put_mysql(creds,data,'fraud_suspicious_addresses')
=======
    put_mysql(creds,data,'fraud_suspicious_addresses')
    print("Data load complete.")
>>>>>>> Stashed changes

if __name__ == "__main__":
    address_data = address_data(14)
    nlp_metrics_df = nlp_metrics_df(address_data,'address_field_one')
    truncate_table()
    load_metrics(nlp_metrics_df)
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 31 12:15:38 2018
@author: Z002PJ2
"""

import datetime
import pandas as pd
pd.set_option('display.width', 3000000)
from tools.tools import *
from dotenv import load_dotenv, find_dotenv
from ga_uploader.GA360 import GA360
import json
import time

import socket
socket.setdefaulttimeout(600)

load_dotenv(find_dotenv())
host = os.getenv("DB4_RPT_HOST")
user = os.getenv("DB4_RPT_USER")
passw = os.getenv("DB4_RPT_PASS")
schema = os.getenv("DB4_RPT_SCHEMA")
creds = (host,user,passw,schema)

mysql_cursor = mysql_cursor(creds)
from warnings import filterwarnings
filterwarnings('ignore', category = pymysql.Warning)

#Setup Dates
class dates:

    def string_date_to_seconds(x):
        x = datetime.datetime.strptime(x, '%Y-%m-%d')
        x = x.timetuple()
        x = int(time.mktime(x))
        return x

    def days_to_seconds(x):
        return int(x * 60 * 60 * 24)

    def date_plus_days_to_date(base_date,days_diff):
        x = dates.string_date_to_seconds(base_date) #uses current day at midnight plus days
        x = x + dates.days_to_seconds(days_diff)
        return dates.seconds_to_date(x)

    def datetime_object_to_seconds(x):
        x = x.timetuple()
        x = int(time.mktime(x))
        return x

    def date_add_ap(inputdate):
        return '\'' + inputdate + '\''

    def days_between(x):
        d1 = datetime.datetime.strptime(x[0], '%Y-%m-%d').date()
        d2 = datetime.datetime.strptime(x[1], '%Y-%m-%d').date()
        return (d2 - d1).days

    def create_date_list2(start_date, end_date, days_interval=1):
        date_list = []
        start_seconds = dates.string_date_to_seconds(start_date)
        date_counts = int(dates.days_between([start_date, end_date]))
        for i in range(0, date_counts + 1):
            date_list.append(dates.seconds_to_date(start_seconds + (i * dates.days_to_seconds(days_interval))))
        return date_list

    def seconds_to_date(x):
        x = datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d')
        return x

#end_date = dates.date_plus_days_to_date(dates.seconds_to_date(dates.datetime_object_to_seconds(datetime.date.today())),-3)
#start_date = dates.date_plus_days_to_date(end_date,-10)
end_date = ''
start_date = ''

def ga_secrets_env_to_json():
    gajson = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    ga_account_info = json.loads(gajson,strict=False)
    return ga_account_info

def get_sc_service(ga_account_info):
    webmasters_api_name = 'webmasters'
    webmasters_api_version = 'v3'
    webmasters_service = GA360.build_service(
        ga_account_info,webmasters_api_name,webmasters_api_version)
    return webmasters_service

def get_analytics_service(ga_account_info):
    analytics_api_name = 'analyticsreporting'
    analytics_api_version = 'v4'
    analytics = GA360.build_service(ga_account_info,
    api_name=analytics_api_name, api_version=analytics_api_version)
    return analytics

analytics_view_id = '3150449'
metrics = {"expression":'ga:transactionRevenue'}

dimensions = [
    {
        "name":'ga:landingPagePath'
    },
    {
        "name":'ga:medium'
    },
    {
        "name":'ga:segment'
    }
    ]
segmentVal = {
            "segmentId": 'gaid::-104'
            }
filter = {"dimensionName": "ga:medium",
         "operator": "PARTIAL",
         "expressions": ["organic"]
        }

def analytics_query(analytics,date):
    return GA360.get_report(analytics,
                     viewId=analytics_view_id,
                     startdate=date,
                     enddate=date,
                     metrics_list=metrics,
                     dimensions_list=dimensions,
                     segmentVal=segmentVal,
                     filter_dict=filter
                     )

def search_console_delete(date1,date2):
    try:
        sqlDeleteRows   = "delete from search_console where calendar_date between " + str(dates.date_add_ap(date1)) + " and " + str(dates.date_add_ap(date2))
        mysql_cursor.execute(sqlDeleteRows)
    except Exception as ex:
        print("Exception occured: %s"%ex)   

def translate_search_console_query_to_dataframe(qd):
    qd2 = pd.DataFrame.from_dict(qd)
    qd2['rows'].apply(pd.Series)
    qd3 = qd2['rows'].apply(pd.Series)
    qd3['landing_page'] = qd3['keys'].apply(lambda x: x[0])
    qd3['keyword'] = qd3['keys'].apply(lambda x: x[1])
    qd3['calendar_date'] = qd3['keys'].apply(lambda x: x[2])
    del qd3['keys']
    return qd3

def search_console_query(webmasters_service,thedate):
    return webmasters_service.searchanalytics().query(
        siteUrl='',
        body ={"startDate": thedate,"endDate": thedate,"dimensions":
            ["page","query","date"],"rowLimit":3000
               }).execute()

def search_console_write(qd3):
    table_name = 'search_console'
    put_mysql(creds,qd3,table_name)

def analytics_write(ga):
    table_name = 'landing_page_revenue'
    put_mysql(creds,ga,table_name)
    
def analytics_delete(date1,date2):
    try:
        sqlDeleteRows= "delete from landing_page_revenue where calendar_date between " + str(dates.date_add_ap(date1)) + " and " + str(dates.date_add_ap(date2))
        mysql_cursor.execute(sqlDeleteRows)
    except Exception as ex:
        print("Exception occured: %s"%ex)   
        
def lp_type_func(x):

    if  'beautyfix' in x:
        lp_type = 'beautyfix'        
    elif  'blog' in x:
        lp_type = 'blog'
    elif 'profile' in x:
        lp_type = 'profile'
#    elif 'about-us' in x:
#        lp_type = 'about-us'
    elif  x in ['','']:
        lp_type = 'homepage'
#    elif  'content' in x:
#        lp_type = 'content'
    elif  'review' in x:
        lp_type = 'review'
    elif  'product' in x:
        lp_type = 'product'
    elif  'article' in x:
        lp_type = 'article'
    elif  'all_Brands' in x:
        lp_type = 'all_brands'
    else:
        lp_type = 'other'
    return lp_type

print('Search Console Updates')

#current dates
def main():
    search_console_dates_query = 'select calendar_date from search_console group by calendar_date'
    search_console_dates = get_mysql(creds,search_console_dates_query)
    search_console_dates_current = search_console_dates['calendar_date'].astype(str).tolist()
    date_list = dates.create_date_list2(start_date,end_date)

    ga_account_info = ga_secrets_env_to_json()
    webmasters_service = get_sc_service(ga_account_info)
    analytics_service = get_analytics_service(ga_account_info)

    for i in date_list:
        print(i)
        qd = search_console_query(webmasters_service,i)
        qd3 = translate_search_console_query_to_dataframe(qd)
        qd3 = qd3[qd3['clicks']>=3]
        search_console_delete(i,i)
        qd3['lp_type'] = qd3['landing_page'].apply(lp_type_func)
        qd3['landing_page'] = qd3['landing_page'].apply(lambda x: x[8:])
        qd3 = qd3[['clicks','ctr','impressions','position','landing_page','lp_type','keyword','calendar_date']]
        search_console_write(qd3)


#%% Analytics Updates

    print('Analytics Updates')

    date_list = dates.create_date_list2(start_date,end_date)
    print(date_list)


    for i in date_list:
        print(i)
        gad = analytics_query(analytics_service,i)
        ga = GA360.load_to_dataframe(gad)
        ga['calendar_date'] = i
        ga['landing_page_simple'] = ga['ga:landingPagePath'].apply(lambda x: str(x).partition('?')[0])
        ga['landing_page'] = ga['ga:landingPagePath']
        ga = ga[['calendar_date','landing_page','landing_page_simple','ga:transactionRevenue']]
        analytics_delete(i,i)
        analytics_write(ga)


if __name__ == "__main__":
    main()

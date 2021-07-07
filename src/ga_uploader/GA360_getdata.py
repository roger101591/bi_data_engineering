from ga_uploader.GA360_uploader import *

load_dotenv(find_dotenv())
host = os.getenv("DB4_RPT_HOST")
user = os.getenv("DB4_RPT_USER")
passw = os.getenv("DB4_RPT_PASS")
schema = os.getenv("DB4_RPT_SCHEMA")
creds = (host, user, passw, schema)

api_name = 'analyticsreporting'
api_version = 'v4'

#reporting variables
session_rpt1_metrics = [
                            {"expression":'ga:sessions'},
                            {"expression":'ga:transactions'},
                            {"expression":'ga:transactionRevenue'},
                            {"expression":'ga:users'},
                            {"expression":'ga:newUsers'}
                            ]
session_rpt1_dimensions = [
                            {'name':'ga:date'},
                            {'name':'ga:channelGrouping'}
                          ]
session_rpt1_view_id = '3150449'
startdate = '2021-02-04'
enddate = 'yesterday'

session_rpt2_dimensions = [
                            {'name':'ga:date'}
                          ]

def main():
    analytics = GA360.build_service(ga_account_info,
                                    api_name=api_name, api_version=api_version)
    session_rpt1_response = GA360.get_report(analytics, session_rpt1_view_id, startdate,
                                       enddate, session_rpt1_metrics, session_rpt1_dimensions)
    session_rpt2_response = GA360.get_report(analytics, session_rpt1_view_id, startdate,
                                       enddate, session_rpt1_metrics, session_rpt2_dimensions)
    session_rpt1_df = GA360.load_to_dataframe(session_rpt1_response)
    session_rpt2_df = GA360.load_to_dataframe(session_rpt2_response)
    session_rpt1_df['Date'] = session_rpt1_df['ga:date'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
    column_order = ['Date', 'ga:channelGrouping', 'ga:sessions', 'ga:transactions', 'ga:transactionRevenue', 'ga:users',
                    'ga:newUsers']
    session_rpt1_df = session_rpt1_df.reindex(columns=column_order)

    session_rpt2_df['Date'] = session_rpt2_df['ga:date'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
    column_order = ['Date', 'ga:sessions', 'ga:transactions', 'ga:transactionRevenue', 'ga:users', 'ga:newUsers']
    session_rpt2_df = session_rpt2_df.reindex(columns=column_order)

    session_rpt1_tbl_name = 'ga360_channel_session_data'
    put_mysql(creds, session_rpt1_df, session_rpt1_tbl_name)

    session_rpt2_tbl_name = 'ga360_session_data'
    put_mysql(creds, session_rpt2_df, session_rpt2_tbl_name)
    print(session_rpt2_df)

if __name__ == "__main__":
    main()





from tools.tools import *
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import numpy as np
import os
import time
import datetime

load_dotenv(find_dotenv())
#get sftp creds
bc_host = os.getenv("BC_HOST")
bc_user = os.getenv("BC_USER")
bc_pass = os.getenv("BC_PASS")
sftp_creds = (bc_host, bc_user, bc_pass)
#get db creds
db_host = os.getenv("DB4_RPT_HOST")
db_user = os.getenv("DB4_RPT_USER")
db_pass = os.getenv("DB4_RPT_PASS")
db_schema = os.getenv("DB4_RPT_SCHEMA")
db_creds = (db_host,db_user,db_pass,db_schema)

today = datetime.date.today().strftime('%Y-%m-%d')
sftp_file = 'bluecore_customer_data_feed_jd_'+ today + '.csv'


def weekly_unsubscribes():
    remote_directory = "/exports_from_bluecore/automated/"
    for df in get_sftp(sftp_creds, remote_directory, "weekly_unsubscribes"):
        put_mysql(
            db_creds,
            df,
            "bc_unsubscribes",
            cols="(@col1,@col2,@col3,@col4) set customer_email=@col1, "
                 "customer_id=@col2, unsubscribe_reason=@col3, unsubscribe_date=@col4;",
        )
def kpi_report():
    # CHANGE FILEPATH AND FILENAME PREFIX FOR AUTOMATION
    # remote_directory = "/exports_from_bluecore/automated/"
    # kpi = get_sftp(sftp_creds,remote_directory, "_kpi")[0]
    remote_directory = "/exports_from_bluecore/automated/"
    kpi = get_sftp(sftp_creds, remote_directory, "kpi_report")[0]
    #kpi = get_sftp(sftp_creds, remote_directory, report_name)[0]
    #print(kpi)

    column_order = ['subaction', 'email_name', 'email_subject', 'email_send_date', 'sends', 'bounces', 'delivered',
                    'opens', 'clicks', 'unsubscribes', 'automated', 'orders', 'revenue', 'spam_complaints']
    kpi = kpi.reindex(columns=column_order)

    kpi["email_send_date"] = pd.to_datetime(kpi.email_send_date)



    kpi['automated'] = np.where(kpi['automated'] == 'true', 1, 0)
    put_mysql(db_creds, kpi, "bc_kpi")

    
    # Commenting out code below

    # orders = get_mysql(db_creds, sql()["bc_orders"])
    # session = get_mysql(db_creds, sql()["landings"])

    #session["subaction"] = session.landing_url.str.extract(r"(?<=subaction_)(.*?)(?=\b|&)")
    #session["email_send_date"] = session.apply(
    #    lambda x: re.search(r"(?<=utm_campaign=)([0-1][0-9][0-3][0-9][1-2][0-9])", x.landing_url).group(0)
    #    if re.search(r"(?<=utm_campaign=)([0-1][0-9][0-3][0-9][1-2][0-9])", x.landing_url)
    #    else (
    #        re.search(r"(?<=utm_campaign=)(20[1-2][0-9][0-3][0-9][0-9][0-9])", x.landing_url).group(0)
    #        if re.search(r"(?<=utm_campaign=)(20[1-2][0-9][0-3][0-9][0-9][0-9])", x.landing_url)
    #        else None
    #    ),
    #    axis=1,
    #)
    #session["email_send_date"] = pd.to_datetime(session.email_send_date)
    #session = session.merge(orders, how="left", on="session_id")
    #kpi = pd.concat([
    #    kpi.loc[kpi["automated"] == 0].merge(session, how="left", on="subaction"),
    #    kpi.loc[kpi["automated"] == 1].merge(session, how="left", on=["subaction", "email_send_date"])
    #]).reset_index()
    #kpi = kpi[kpi.columns[[1, 4, 17, 18, 20, 21, 22, 23, 24, 25]]]
    #kpi.session_date.fillna(datetime.today().date() - timedelta(days=1), inplace=True)
    #kpi[kpi.columns[-7]].fillna(0, inplace=True)
    #kpi = kpi.groupby(["subaction", "email_send_date_x", "session_date"]).sum().reset_index()
    #put_mysql(db_creds, kpi[kpi.sessions > 0], "bc_sessions")

def ingest_revenue_report_from_bc():
    revenue_rpt = 'monthly_30_day_deliver_revenue_report'
    remote_directory = "/exports_from_bluecore/automated/"
    revenue = get_sftp(sftp_creds,remote_directory,revenue_rpt)[0]
    revenue['date_of_first_email_since_optin'] = pd.to_datetime(revenue['date_of_first_email_since_optin'],
                                                               utc=True,
                                                              )
    #convert UTC to PT
    revenue['date_of_first_email_since_optin'] = revenue['date_of_first_email_since_optin'].dt.tz_convert('US/Pacific')
    #trim date string to YYYY-MM-DD HH:MM:SS
    revenue['date_of_first_email_since_optin'] = revenue['date_of_first_email_since_optin'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    put_mysql(
        db_creds,
        revenue,
        'bc_monthly_revenue',
        cols = "(@col1,@col2,@col3) set email=@col1,date_first_email_since_optin=@col2,attributed_revenue=@col3;"
    )

def clear_old_csvs(number_of_days, path):

    time_in_secs = time.time() - (number_of_days * 24 * 60 * 60)
    for file in os.listdir(path):
        if file.endswith('.csv'):
            if os.stat(os.path.join(path, file)).st_mtime < time_in_secs:
                os.remove(os.path.join(path, file))

    time_in_secs = time.time() - (number_of_days * 24 * 60 * 60)
    for file in os.listdir(path):
        if file.endswith('.csv'):
            if os.stat(os.path.join(path, file)).st_mtime < time_in_secs:
                print(os.stat(os.path.join(path, file)))
                os.remove(os.path.join(path, file))


def bc_customer_data_feed_sql():
    drop_tmp1 = "drop temporary table if exists active_codes;"
    create_tmp1 = """
                create temporary table if not exists active_codes (index(customer_id))
                select	
                    pc.customer_id,
                    pc.promotion_coupon_code,
                    pc.date_expire
                from promotion_coupons pc
                where
                    pc.date_expire > date(now()) and pc.promotion_coupon_source IN ('Winback', 'Internal Acquisition')
                    AND pc.promotion_code_status = 100
                    AND pc.customer_id NOT in (SELECT o.customer_id 
                    FROM orders o 
                    WHERE 
                    o.order_date >= DATE(NOW()) - interval 12 MONTH 
                    AND o.order_status in (100,110,400,500,700,800,900,1000,1300,1400)
                    and o.cobrand_id not in (2,23,24, 29)
                    and (o.order_source not in ('reship') or o.order_source is NULL)
                    AND o.customer_id IS NOT NULL
                    GROUP BY o.customer_id)
                GROUP BY pc.customer_id
                ;
                    """
    drop_tmp2 = "DROP TEMPORARY TABLE if EXISTS expired_codes;"
    create_tmp2 = """
                    create temporary table if not exists expired_codes (index(customer_id))
                    SELECT
                        pc.customer_id
                    FROM promotion_coupons pc
                    WHERE
                        pc.promotion_coupon_source IN ('Winback', 'Internal Acquisition')
                        AND pc.customer_id NOT IN (SELECT t.customer_id FROM active_codes t)
                        AND (pc.date_expire > DATE(NOW()) OR (pc.date_expire <= DATE(NOW()) AND pc.date_expire > DATE(NOW()) - interval 14 DAY))
                    GROUP BY pc.customer_id
                    """

    result_query = """
                SELECT
                    bc.customer_email AS email_address,
                    bc.customer_id AS customer_id,
                    coalesce(c.customer_first_name,'') AS customer_first_name,
                    coalesce(c.customer_last_name,'') AS customer_last_name,
                    if(bc.bc_status = 1,'Active','Unsubscribed') AS customer_email_status,
                    '' as email_send_preferences,
                    '' as survey_url,
                    if(t.promotion_coupon_code is NULL, '', t.promotion_coupon_code) as sus_code
                FROM bc_email_status bc
                inner join customers c on bc.customer_id = c.customer_id
                left join active_codes t on t.customer_id = bc.customer_id
                LEFT JOIN expired_codes t2 ON t2.customer_id = bc.customer_id
                where
                    bc.customer_email like '%@%.%' and bc.customer_email not like '@%' and bc.customer_email not like '%@.' and bc.customer_email not like '%@%.'
                    and (bc.date_changed >= now() - interval 5 day or bc.date_orig >= now() - interval 2 WEEK
                            or t.promotion_coupon_code IS NOT NULL OR t2.customer_id IS NOT null)
                group by
                    bc.customer_email
                ;
                """
    cursor = mysql_cursor(db_creds)
    cursor.execute(drop_tmp1)
    cursor.execute(create_tmp1)
    cursor.execute(drop_tmp2)
    cursor.execute(create_tmp2)
    cursor.execute(result_query)
    result = cursor.fetchall()
    result_df = pd.DataFrame(data=result, index=None,
                             columns=['email_address', 'customer_id', 'customer_first_name', 'customer_last_name',
                                      'customer_email_status', 'email_send_preferences', 'survey_url', 'sus_code'])

    return result_df

if __name__ == "__main__":
    kpi_report()
    weekly_unsubscribes()
    ingest_revenue_report_from_bc()
    result_df = bc_customer_data_feed_sql()
    result_df.to_csv(sftp_file)
    clear_old_csvs(3,'/')
    put_sftp(sftp_file,sftp_creds,'/customer_data/')


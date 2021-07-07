from datetime import *
from wpp_auto.send_email import send_mail
from tools.tools import *
from dotenv import load_dotenv, find_dotenv
import time

load_dotenv(find_dotenv())
#get db creds
db_host = os.getenv("DB4_PROD_HOST")
db_user = os.getenv("DB4_PROD_USER")
db_pass = os.getenv("DB4_PROD_PASS")
db_schema = os.getenv("DB4_PROD_SCHEMA")
db_creds = (db_host,db_user,db_pass,db_schema)
mysql_cursor = mysql_cursor(db_creds)

'''CREATE OUTPUT DIRECTORY'''
current_date = date.today()
all_records_f = 'All_Records_%s.csv' % current_date
path = 'attachments/'
path_to_file = path + all_records_f
os.makedirs(path,exist_ok=True)
columns = ['Order ID', 'Order Source', 'Number of Attempts', 'Transaction ID','Transaction Code',
                                    'Transaction Message','Transaction Date','Days Since Last Transaction',
                                    'Payment Problem Order Type','Transaction Type']

def clear_attachment_archive(number_of_days, path):
    time_in_secs = time.time() - (number_of_days * 24 * 60 * 60)
    for file in os.listdir(path):
        if os.stat(os.path.join(path, file)).st_mtime < time_in_secs:
            os.remove(os.path.join(path, file))


pp_orders_query = '''
            select
            o.order_id,
            o.order_source,
            count(pt.trans_id) as number_of_attempts,
            pt.trans_id as transaction_id,
            pt.trans_result_code as transaction_code,
            pt.trans_result_msg as transaction_message,
            date(pt.date_orig) as date_orig,
            datediff(date(now()),date(pt.date_orig)) as days_since_last_transaction,
            CASE WHEN 
                        trans_result_code not in (12, 24, 1000, 100, 101, 102, 110, 120, 121, 122, 123, 124, 125, 349, 350, 351, 126, 127, 130, 305, 320, 23, -100, 301)
                        and pt.trans_card_type != 'PP'
                 THEN	
                        'Payments that have been declined'
                 WHEN 
                        datediff(date(now()),date(pt.date_orig)) > 30
                        and pt.trans_card_type != 'PP'
                 THEN
                        'Orders to be cancelled'
                 WHEN
                        #in previous version 10 was the maximum transaction count number
                        count(pt.trans_id) < 10
                        and trans_result_code in (24, 305, 320)
                        and datediff(date(now()),date(pt.date_orig)) >= 1
                        and pt.trans_card_type != 'PP'
                 THEN 
                        'Potential Authorization - Expired Card'
                 WHEN
                        count(pt.trans_id) < 10
                        and trans_result_code in (12, 1000, 100, 101, 102, 110, 120, 121, 122, 123, 124, 125, 349, 350, 351) 
                        and datediff(date(now()),date(pt.date_orig)) >= 1
                        and pt.trans_card_type != 'PP'
                 THEN 
                        'Potential Authorization - Insufficient Funds'
                 WHEN 
                        count(pt.trans_id) < 10
                        and (trans_result_code in (23, 126, 127, 130, 301)
                        OR pt.trans_card_type = 'PP')
                        and datediff(date(now()),date(pt.date_orig)) >= 1
                 THEN
                        'Customer Service'
                 WHEN
                        count(pt.trans_id) = 1
                        and trans_result_code < 0
                        and datediff(date(now()),date(pt.date_orig)) >= 1
                        and datediff(date(now()),date(pt.date_orig)) < 8
                 THEN
                        'Research Status Orders'
                 ELSE   
                        'Orders to be cancelled'
                 END AS Payment_Problem_Order_Type,
            pt.trans_card_type AS 'Transaction_Type'
            
            FROM orders o
            inner join payment_transactions pt
            on o.order_id = pt.order_id
            #payment problem status
            where o.order_status = 600
            and o.cobrand_id in (1, 10, 25, 27)
            and o.order_date >= '2013-08-23'
            group by o.order_id
            order by o.order_id asc
            '''

def pp_orders(pp_orders_sql):
    mysql_cursor.execute(pp_orders_sql)
    pp_orders = mysql_cursor.fetchall()

    pp_orders_df = pd.DataFrame(data=pp_orders, index=None,
                           columns=columns
                           )
    print('Query Executed')
    return pp_orders_df

def split_pp_orders(dataframe):
    files = []
    for g in dataframe.groupby(by=dataframe['Payment Problem Order Type']):
        file = path + g[0] + '_%s.csv' % current_date
        g[1].to_csv(file,index=False)
        files.append(file)
    print(files)
    return files

'''EMAIL PARAMETERS'''
sender = 
rec = []
subject = 'Payment Problem Orders - %s' % date.today()
body = 'Attached is the most recent list of Payment Problem Orders'

def main():
    #Create CSV and store in folder
    pp_result = pp_orders(pp_orders_query)
    all_records_file = path + all_records_f
    pp_result.to_csv(all_records_file,index=False)

    files = split_pp_orders(pp_result)
    files.append(all_records_file)

    #Send multiple files - one for each payment problem order type
    send_mail(sender=sender, receiver=rec, subject=subject, body=body, files=files)

    #Send one file
    #send_mail(sender=sender, receiver=rec, subject=subject, body=body, files=[pp_result])

    #Clear files in attachment folder older than 5 days
    clear_attachment_archive(5, path)

if __name__ == "__main__":
    main()


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from dotenv import load_dotenv, find_dotenv
import os
import pymysql
import mplcursors as mplc
from matplotlib.pyplot import figure, show

#required to make matplotlib interactive with ipynb file
import matplotlib
matplotlib.use('nbagg')

#load db creds
load_dotenv(find_dotenv())
host = os.getenv("DB4_RPT_HOST")
user = os.getenv("DB4_RPT_USER")
passw = os.getenv("DB4_RPT_PASS")
schema = os.getenv("DB4_RPT_SCHEMA")
creds = (host, user, passw, schema)

def mysql_cursor(credentials):
    cursor = pymysql.connect(host=credentials[0], port=3306, user=credentials[1], password=credentials[2], db=credentials[3], autocommit=True)
    return cursor.cursor()

mysql_cursor = mysql_cursor(creds)

def zip_data():
    recent_tmp_drop = 'drop temporary table if exists recent_bad_trx;'
    recent_tmp_create = '''
    create temporary table if not exists recent_bad_trx (primary key(address_zip))
	select
	  
	    a.address_zip,
	    sum(if(trans_result_code=0,1,0)) as good,
	    sum(if(trans_result_code=0,0,1)) as bad,
	    if(sum(if(trans_result_code=0,1,0)) = 0, 0, sum(if(trans_result_code=0,0,1))/sum(if(trans_result_code=0,1,0))) as ratio_bad_good
	 from payment_transactions pt
	inner join orders o using (order_id)
	inner join addresses a on o.ship_address_id = a.address_id
	where
	    o.order_source not in ('autorefill', 'reship', 'autorefill_sequ') and
	    pt.trans_type = 'A' and
	    pt.trans_type <> 'PP' and
	    pt.date_orig >= now() - interval 14 day
	group by a.address_zip;
    '''
    historical_tmp_drop = 'drop temporary table if exists historical_bad_trx;'
    historical_tmp_create = '''
    create temporary table if not exists historical_bad_trx (primary key(address_zip))
    select
   
        a.address_zip,
        sum(if(trans_result_code=0,1,0)) as good,
        sum(if(trans_result_code=0,0,1)) as bad
        #sum(if(trans_result_code=0,0,1))/sum(if(trans_result_code=0,1,0)) as ratio_bad_good
     from payment_transactions pt
    inner join orders o using (order_id)
    inner join addresses a on o.ship_address_id = a.address_id
    where
        o.order_source not in ('autorefill', 'reship', 'autorefill_sequ') and
        pt.trans_type = 'A' and
        pt.trans_type <> 'PP' and
        pt.date_orig >= now() - interval 1 year
        and address_zip is not null
    group by a.address_zip
    '''
    data = '''
    select 
	   r.address_zip,
	   r.good,
	   r.bad,
	   r.ratio_bad_good,
	   h.good,
	   h.bad,
	   h.bad/h.good as ratio_historical_bad_good,
	   
	   r.ratio_bad_good / (h.bad/h.good) as ratio_recent_vs_historical
	   from recent_bad_trx r
	   left join historical_bad_trx h
	   on r.address_zip = h.address_zip
	   where r.bad > 0
	   order by ratio_bad_good DESC 
    '''
    mysql_cursor.execute(recent_tmp_drop)
    mysql_cursor.execute(recent_tmp_create)
    mysql_cursor.execute(historical_tmp_drop)
    mysql_cursor.execute(historical_tmp_create)
    mysql_cursor.execute(data)
    result = mysql_cursor.fetchall()
    zip_data = pd.DataFrame(data=result, index=None,
                        columns=['address_zip','recent_good','recent_bad','recent_ratio_bad_good',
                                 'hist_good','hist_bad','hist_ratio_bad_good','recent_vs_hist_ratio'])
    return zip_data


 zip_data = zip_data()

x = zip_data['hist_ratio_bad_good']
y = zip_data['recent_ratio_bad_good']
label = zip_data['address_zip']

fig,ax = plt.subplots()
ax.set_title("Zipcode Anomalies")
ax.scatter(x,y,label=label)
crs = mplc.cursor(ax,hover=True)
plt.xlabel("Historical Bad Trx Ratio 1 Year")
plt.ylabel("Recent Bad Trx Ratio Last 14 Days")

crs.connect("add", lambda sel: sel.annotation.set_text(
    'Zip {}: {},{}'.format(label[sel.target.index], sel.target[0], sel.target[1])),
           )

plt.show()

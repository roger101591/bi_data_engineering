from tools.tools import *
from datetime import datetime as dt
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

date_start = '2016-12-06'
date_end = dt.today().strftime('%Y-%m-%d')

#get db creds
db_host = os.getenv("DB4_RPT_HOST")
db_user = os.getenv("DB4_RPT_USER")
db_pass = os.getenv("DB4_RPT_PASS")
db_schema = os.getenv("DB4_RPT_SCHEMA")
db_creds = (db_host,db_user,db_pass,db_schema)

mysql_cursor = mysql_cursor(db_creds)

def loyalty_cust_credits():
    query_str = ("""
                select
                	ltc.customer_id,
                	date(ltc.date_orig) as calendar_date,
                	sum(ltc.loyalty_transaction_credit_amount) as credits_earned,
                	sum(if(ltd.loyalty_transaction_debit_amount is not null, ltd.loyalty_transaction_debit_amount, 0)) as debits_redeemed,
                	sum(if(ltc.date_expired is null, ltc.loyalty_transaction_credit_amount, 0))  - sum(if(ltc.date_expired is null and ltd.loyalty_transaction_debit_amount is not null, ltd.loyalty_transaction_debit_amount,0)) as remaining_credit,
                	sum(if(ltc.date_expired is not null, ltc.loyalty_transaction_credit_amount, 0))  - sum(if(ltc.date_expired is not null and ltd.loyalty_transaction_debit_amount is not null, ltd.loyalty_transaction_debit_amount,0)) as expired
                from dermstore_data.loyalty_transactions_credits ltc
                left join (select 
                		ltd.loyalty_transactions_credit_id,
                		sum(ltd.loyalty_transaction_debit_amount) as loyalty_transaction_debit_amount
                	from
                		loyalty_transactions_debits ltd
                	where ltd.date_orig >= '""" + date_start + """'
                	group by ltd.loyalty_transactions_credit_id) ltd on ltd.loyalty_transactions_credit_id = ltc.loyalty_transaction_credit_id
                where ltc.date_orig >= '""" + date_start + """'
                group by calendar_date, ltc.customer_id
                """)
    return get_mysql(db_creds, query_str)

#daily points debited -- Above debits are tied to credit date, this is tied to the date debited
def daily_debited():
    query_str = ("""
                 select
                     date(ltd.date_orig) as calendar_date,
                     sum(ltd.loyalty_transaction_debit_amount) as points_debited
                 from dermstore_data.loyalty_transactions_debits ltd
                 where
                     ltd.date_orig between '""" + date_start + """' and '""" + date_end + """' and ltd.loyalty_transaction_debit_description not in ('Accident', 'Error', '0', 'Rep Error')
                 group by date(ltd.date_orig)
                 """)
    return get_mysql(db_creds, query_str)

#Per customer per day, the number of orders, rewards redeemed, and net revenue since joining the loyalty program
<<<<<<< Updated upstream
def loyalty_cust_orders():
    query_str = ("""
                select
                	o.customer_id,
                	fc.calendar_date,
                	count(distinct o.order_id) as num_orders,
                	sum(oi.order_item_quantity * oi.order_item_credit) as redeemed_reward,
                	sum(oi.order_item_quantity * (oi.order_item_price - oi.order_item_discount - oi.order_item_credit)) as Net_Rev
                from dermstore_data.orders o 
                inner join dermstore_data.customers c on c.customer_id = o.customer_id
                inner join dermstore_data.fiscal_calendar fc on fc.calendar_date = date(o.order_date)
                inner join dermstore_data.orders_items oi on oi.order_id = o.order_id
                where
                	o.order_status in (100,110,400,500,700,800,900,1000,1300,1400)
                	and o.cobrand_id not in (2,23,24)
                	and (o.order_source not in ('reship') or o.order_source is null)
                	and date(o.order_date) >= date(c.customer_loyalty_enroll_date)
                	and c.customer_loyalty_status = 1
                	and o.order_date >= '""" + date_start + """'
                	and o.order_date <= '""" + date_end + """'
                group by fc.calendar_date, o.customer_id
                 """)
    return get_mysql(db_creds, query_str)
=======
def load_loyalty_cust_data():
    lc_df_query =   """
                    REPLACE INTO kpi_snapshot_loyalty_cust_orders(customer_id,calendar_date,
                    num_orders,redeemed_reward,net_rev)
                    select
                        o.customer_id,
                        fc.calendar_date,
                        count(distinct o.order_id) as num_orders,
                        sum(oi.order_item_quantity * oi.order_item_credit) as redeemed_reward,
                        sum(oi.order_item_quantity * (oi.order_item_price - oi.order_item_discount - oi.order_item_credit)) as net_rev
                    from orders o 
                    inner join customers c on c.customer_id = o.customer_id
                    inner join fiscal_calendar fc on fc.calendar_date = date(o.order_date)
                    inner join orders_items oi on oi.order_id = o.order_id
                    where
                        o.order_status in (100,110,400,500,700,800,900,1000,1300,1400)
                        and o.cobrand_id not in (2,23,24)
                        and (o.order_source not in ('reship') or o.order_source is null)
                        and date(o.order_date) >= date(c.customer_loyalty_enroll_date)
                        and c.customer_loyalty_status = 1
                        and o.order_date >= '""" + date_end + """' - interval 1 week
                        and o.order_date <= '""" + date_end + """'
                    group by fc.calendar_date, o.customer_id;
                    """
    mysql_cursor.execute(lc_df_query)

def loyalty_orders_metrics_daily():
    query = """
            select calendar_date, 
            sum(net_rev) as net_rev,
            sum(redeemed_reward) as redeemed_reward
            from kpi_snapshot_loyalty_cust_orders
            group by calendar_date
            """
    return get_mysql(db_creds,query)

def get_date_list():
    dates_query_var = """
            set @final_date := (select max(calendar_date) from kpi_daily_active_members);
            """
    dates_query = """
                select calendar_date from fiscal_calendar
                where calendar_date >= @final_date and calendar_date <= date(now());
                """
    mysql_cursor.execute(dates_query_var)
    mysql_cursor.execute(dates_query)
    dates_list_result = mysql_cursor.fetchall()

    dates_df = pd.DataFrame(data=dates_list_result, index=None,
                               columns=['calendar_date']
                           )
    dates = dates_df['calendar_date'].to_list()
    return dates

def active_members_and_redeemed_load(calendar_dates=[]):
    """
    arg: take date list
    :return: insert active_customers
    """
    for calendar_date in calendar_dates:
        # for each calendar_date, run query and append result to list
        active_sub_query = """
                            select count(distinct customer_id) as active_members
                            from kpi_snapshot_loyalty_cust_orders o
                            where o.calendar_date >= '""" + str(calendar_date) + """' - interval 365 day 
                            and (o.calendar_date + interval 365 day) <= '""" + str(calendar_date) + """' + interval 365 day
        """
        sub = get_mysql(db_creds, active_sub_query)
        value = sub.active_members.iloc[0]
        active_sub_query2 = """
                            select count(distinct customer_id) as active_members
                            from kpi_snapshot_loyalty_cust_orders o
                            where o.calendar_date >= '""" + str(calendar_date) + """' - interval 365 day 
                            and (o.calendar_date + interval 365 day) <= '""" + str(calendar_date) + """' + interval 365 day
                            and redeemed_reward > 0
        """
        sub2 = get_mysql(db_creds, active_sub_query2)
        value2 = sub2.active_members.iloc[0]
        replace_into_query = """
                        replace into kpi_daily_active_members(calendar_date,active_members,active_redeemed)
                        values('""" + str(calendar_date) + """',""" + str(value) + """,""" + str(value2) + """)
                        """
        mysql_cursor.execute(replace_into_query)

def get_active_members():
    query = """
            select calendar_date,active_members
            from kpi_daily_active_members
            """
    active_members_df = get_mysql(db_creds,query)
    active_members_df2 = active_members_df.set_index('calendar_date')
    active_members_df2.index = pd.to_datetime(active_members_df2.index)
    return active_members_df2

def get_active_redeemed():
    query = """
            select calendar_date,active_redeemed
            from kpi_daily_active_members
            """
    active_redeemed_df = get_mysql(db_creds,query)
    active_redeemed_df2 = active_redeemed_df.set_index('calendar_date')
    active_redeemed_df2.index = pd.to_datetime(active_redeemed_df2.index)
    return active_redeemed_df2
>>>>>>> Stashed changes

def opt_ins():
    opt_vars = 'set @cumulative_enrollment := 0;'
    opt_sql = '''
        select
    	date(c.customer_loyalty_enroll_date) as calendar_date,
    	 fc.fiscal_year,
         fc.fiscal_month_name,
         fc.fiscal_week,
    	count(distinct c.customer_id) as enrolled_today,
    	@cumulative_enrollment := @cumulative_enrollment + count(distinct c.customer_id) as 'total_enrolled'
        # fc.calendar_date 
    from customers c
    	inner join fiscal_calendar fc on fc.calendar_date = date(c.customer_loyalty_enroll_date)
    where
    	c.customer_loyalty_enroll_date between '2016-12-06' and date(now())
    	group by date(c.customer_loyalty_enroll_date)
        '''
    mysql_cursor.execute(opt_vars)
    mysql_cursor.execute(opt_sql)
    opt_result = mysql_cursor.fetchall()

    opt_ins =  pd.DataFrame(data=opt_result, index=None,
                           columns=['calendar_date', 'fiscal_year', 'fiscal_month_name', 'fiscal_week',
                                    'enrolled_today',
                                    'total_enrolled']
                           )
    return opt_ins

def ear_sql():
    ear_vars = 'set @cumulative_orders := 0, @id := NULL, @ear_cu_order := 0;'
    ear_sql = '''
               SELECT calendar_date , @ear_cu_order := cumulative_orders + @ear_cu_order
                FROM
                (
                    SELECT calendar_date,
                    sum(cumulative_orders) as cumulative_orders
            
                    FROM
                        (
                        SELECT customer_id,
                               calendar_date,
                               num_orders,
                              @cumulative_orders := 	IF( @id = customer_id
                                                    , 0
                                                    , 1
                                                    ) as cumulative_orders,
            
                               @id := 					IF( @id = customer_id
                                                    , @id 
                                                    , customer_id
                                                    )as id,
                              redeemed_reward,
                              Net_Rev
                        FROM
                        (
                        select
                            o.customer_id,
                            fc.calendar_date,
                            count(distinct o.order_id) as num_orders,
                            sum(oi.order_item_quantity * oi.order_item_credit) as redeemed_reward,
                            sum(oi.order_item_quantity * (oi.order_item_price - oi.order_item_discount - oi.order_item_credit)) as Net_Rev
                        from dermstore_data.orders o 
                        inner join customers c on c.customer_id = o.customer_id
                        inner join fiscal_calendar fc on fc.calendar_date = date(o.order_date)
                        inner join orders_items oi on oi.order_id = o.order_id
                        where
                            o.order_status in (100,110,400,500,700,800,900,1000,1300,1400)
                            and o.cobrand_id not in (2,23,24)
                            and (o.order_source not in ('reship') or o.order_source is null)
                            and date(o.order_date) >= date(c.customer_loyalty_enroll_date)
                            and c.customer_loyalty_status = 1
                            and o.order_date >= '2016-12-06'
                            and o.order_date <= date(now())
                        group by fc.calendar_date, o.customer_id
                        order by customer_id, calendar_date asc
                        ) as cust_orders_daily
                    ) as ear
                    GROUP BY calendar_date
                ) as ear2
              '''
    mysql_cursor.execute(ear_vars)
    mysql_cursor.execute(ear_sql)
    ear_result = mysql_cursor.fetchall()
    ear_sql =  pd.DataFrame(data=ear_result, index=None,
                       columns=['calendar_date', 'ever_active'])
    return ear_sql

def ever_redeemed():
    evr_set_var = 'set @cu_redeemed := 0,	@id := NULL, @ev_redeemed := 0;'
    evr_temp_tbl1_drop = 'drop temporary table if exists cust_redeemed_ever;'
    evr_temp_tbl1_create = '''
                create temporary table if not exists cust_redeemed_ever
                select customer_id,calendar_date, case when cu_redeemed = 0 then cu_redeemed else 1 end as ever_redeemed
                from
                    (
                    select customer_id,
                            calendar_date,
                            case when redeemed_reward > 1 then 1 else redeemed_reward end as redeemed_reward,
                            @cu_redeemed := 		IF( @id = customer_id
                                                        , @cu_redeemed + case when redeemed_reward > 1 then 1 else redeemed_reward end
                                                        , 0 + case when redeemed_reward > 1 then 1 else redeemed_reward end
                                                        ) as cu_redeemed,
                            @id := 					IF( @id = customer_id
                                                        , @id 
                                                        , customer_id
                                                        ) as id
                    from
                        (
                                SELECT customer_id,
                           calendar_date,
                           #case when redeemed_reward > 1 then 1 else redeemed_reward end as redeemed_reward
                           redeemed_reward
                        FROM
                        (
                        select
                            o.customer_id,
                            fc.calendar_date,
                            count(distinct o.order_id) as num_orders,
                            sum(oi.order_item_quantity * oi.order_item_credit) as redeemed_reward,
                            sum(oi.order_item_quantity * (oi.order_item_price - oi.order_item_discount - oi.order_item_credit)) as Net_Rev
                        from orders o 
                        inner join customers c on c.customer_id = o.customer_id
                        inner join fiscal_calendar fc on fc.calendar_date = date(o.order_date)
                        inner join orders_items oi on oi.order_id = o.order_id
                        where
                            o.order_status in (100,110,400,500,700,800,900,1000,1300,1400)
                            and o.cobrand_id not in (2,23,24)
                            and (o.order_source not in ('reship') or o.order_source is null)
                            and date(o.order_date) >= date(c.customer_loyalty_enroll_date)
                            and c.customer_loyalty_status = 1
                            and o.order_date >= '2016-12-06'
                            and o.order_date <= date(now())
                        group by fc.calendar_date, o.customer_id
                        order by customer_id, calendar_date asc
                        ) as cust_orders_daily
                        ) as redeemed
                    order by customer_id,calendar_date asc
                    ) as cust_redeemed_ever
            ;
                '''
    evr_temp_tbl2_drop = 'drop temporary table if exists cust_redeemed_ever2;'
    evr_temp_tbl2_create = 'create temporary table cust_redeemed_ever2 select * from cust_redeemed_ever;'
    evr_query = '''
                    SELECT calendar_date,
                       @ev_redeemed := @ev_redeemed + ever_redeemed 
                       FROM 
                       (
                        SELECT a.calendar_date, 
                               SUM(IFNULL(b.ever_redeemed,0)) as ever_redeemed
                        FROM
                            (
                            select distinct calendar_date from cust_redeemed_ever 
                            ) a
                            left join 
                            (
                            select 
                            *
                            from cust_redeemed_ever2
                            group by customer_id,ever_redeemed
                            having ever_redeemed > 0
                            order by calendar_date
                            ) b 
                            on a.calendar_date = b.calendar_date
                        group by calendar_date
                      ) as ev
                    '''
    mysql_cursor.execute(evr_set_var)
    mysql_cursor.execute(evr_temp_tbl1_drop)
    mysql_cursor.execute(evr_temp_tbl1_create)
    mysql_cursor.execute(evr_temp_tbl2_drop)
    mysql_cursor.execute(evr_temp_tbl2_create)
    mysql_cursor.execute(evr_query)
    evr_result = mysql_cursor.fetchall()
    ever_redeemed =  pd.DataFrame(data=evr_result, index=None,
                                 columns=['calendar_date', 'ever_redeemed']
                                 )
    return ever_redeemed

def load_table(kpi,table_name):
    put_mysql(db_creds,kpi,table_name)
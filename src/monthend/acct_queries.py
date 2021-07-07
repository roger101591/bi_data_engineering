from wpp_auto.send_email import send_mail
from tools.tools import mysql_cursor,get_mysql
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import os
from datetime import date

load_dotenv(find_dotenv())
db_host = os.getenv("DB4_PROD_HOST")
db_user = os.getenv("DB4_PROD_USER")
db_pass = os.getenv("DB4_PROD_PASS")
db_schema = os.getenv("DB4_PROD_SCHEMA")
db_creds = (db_host,db_user,db_pass,db_schema)
mysql_cursor = mysql_cursor(db_creds)

path = 'attachments/'
current_date = date.today()
test_rec = []
sender = ''
def send_reports(rec,subject,body,files):
    send_mail(sender=sender, receiver=rec, subject=subject, body=body, files=[files])

def report_67():
    query = """
                select
                    fc.fiscal_year as 'Fiscal Year',
                    fc.fiscal_month as 'Fiscal Month', 
                    i.id as 'Product ID',	
                    concat_ws('-',i.id,i.brand_name,i.description) as 'Product Description', 
                    count(distinct(s.ship_id)) as 'Orders', 
                    sum(si.ship_item_quantity) as 'Units', 
                    sum(si.ship_item_quantity * si.ship_item_price) as 'Gross Revenue'
                from shipments s
                inner join shipment_items si on s.ship_id = si.ship_id
                inner join item i on si.prod_id = i.id 
                inner join fiscal_calendar fc on date(s.ship_date) = fc.calendar_date
                where
                    s.cancel = 0
                    and s.ship_status = 1000
                    and i.brand_id = 500102
                    and fc.fiscal_year >= 2016
                group by
                    fc.fiscal_year,
                    fc.fiscal_month,
                    i.id 	
                ;    
        """
    result = get_mysql(db_creds,query)
    report_name = 'Giftcard_Month_End_Data_All_Sales'
    filename =  report_name + '_%s.csv' % current_date
    path_to_file = path + filename
    result.to_csv(path_to_file,index=False)

    prod_rec = []

    rec = prod_rec
    subject = report_name.replace('_',' ')
    body = 'Attached is the most recent report for ' + subject + '.'
    files = path_to_file
    send_reports(rec, subject, body, files)

def report_188():
    query = """
            SELECT
            fc.fiscal_year AS 'Credit Year',
            fc.fiscal_month AS 'Credit Month',
            CASE
                WHEN LENGTH(a.address_state) = 2 THEN a.address_state
                WHEN a.address_state = 'CALIFORNIA' THEN 'CA'
                WHEN a.address_state = 'Pennsylvania' THEN 'PA'
                WHEN a.address_state = 'Colorado' THEN 'CO'
                WHEN a.address_state = 'Georgia' THEN 'GA'
                WHEN a.address_state = 'NEW YORK' THEN 'NY'
                WHEN a.address_state = 'North Carolina' THEN 'NC'
                WHEN a.address_state = 'INDIANA' THEN 'IN'
                WHEN a.address_state = 'TEXAS' THEN 'TX'
                WHEN a.address_state = 'NEW JERSEY' THEN 'NJ'
                WHEN a.address_state = 'MARYLAND' THEN 'MD'
                WHEN a.address_state = 'KANSAS' THEN 'KS'
                WHEN a.address_state = 'MICHIGAN' THEN 'MI'
                WHEN a.address_state = 'MINNESOTA' THEN 'MN'
                WHEN a.address_state = 'VIRGINIA' THEN 'VA'
                ELSE NULL
                END
            AS State,
            sum(if(pt.trans_giftcard = 0,c.products_total_credit + c.general_credit,0)) AS 'Total Credit Card Product Refunds',
            sum(if(pt.trans_giftcard = 0,pt.trans_tax,0)) * -1 AS 'Total Credit Card Tax Refunds',
            sum(if(pt.trans_giftcard != 0,c.products_total_credit + c.general_credit,0)) as 'Total Giftcard Product Refunds',
            sum(if(pt.trans_giftcard != 0,pt.trans_tax,0)) * -1 as 'Total Giftcard Tax Refunds',
            sum(c.products_total_credit + c.general_credit) as 'Total Product Refunds',
            sum(pt.trans_tax) as 'Total Tax Refunds'
        FROM credits c
        inner join orders o on c.order_id = o.order_id and o.cobrand_id not in (2,23,24)
        inner join addresses a on o.ship_address_id = a.address_id
        INNER JOIN fiscal_calendar fc ON DATE(c.date_orig) = fc.calendar_date AND fc.fiscal_year >= 2017
        inner join payment_transactions pt on c.trans_id = pt.trans_id
        GROUP BY 
            fc.fiscal_year,
            fc.fiscal_month_num,
            CASE
                WHEN LENGTH(a.address_state) = 2 THEN a.address_state
                WHEN a.address_state = 'CALIFORNIA' THEN 'CA'
                WHEN a.address_state = 'Pennsylvania' THEN 'PA'
                WHEN a.address_state = 'Colorado' THEN 'CO'
                WHEN a.address_state = 'Georgia' THEN 'GA'
                WHEN a.address_state = 'NEW YORK' THEN 'NY'
                WHEN a.address_state = 'North Carolina' THEN 'NC'
                WHEN a.address_state = 'INDIANA' THEN 'IN'
                WHEN a.address_state = 'TEXAS' THEN 'TX'
                WHEN a.address_state = 'NEW JERSEY' THEN 'NJ'
                WHEN a.address_state = 'MARYLAND' THEN 'MD'
                WHEN a.address_state = 'KANSAS' THEN 'KS'
                WHEN a.address_state = 'MICHIGAN' THEN 'MI'
                WHEN a.address_state = 'MINNESOTA' THEN 'MN'
                WHEN a.address_state = 'VIRGINIA' THEN 'VA'
                ELSE NULL
                END
        order by
            fc.fiscal_year,
            fc.fiscal_month_num
            ;
    """
    result = get_mysql(db_creds, query)
    report_name = 'Monthly_Return_Reserve_Data_Credit_Date'
    filename = report_name + '_%s.csv' % current_date
    path_to_file = path + filename
    result.to_csv(path_to_file,index=False)

    prod_rec = []

    rec = prod_rec
    subject = report_name.replace('_', ' ')
    body = 'Attached is the most recent report for ' + subject + '.'
    files = path_to_file
    send_reports(rec, subject, body, files)

def report_196():

    set_var = "set @end_date = date(now());"
    drop_tmp1 = "drop temporary table if exists temp_loyalty_transactions_debits;"
    drop_tmp2 = "drop temporary table if exists temp_credit_balances;"
    add_tmp1 = """
            create temporary table if not exists temp_loyalty_transactions_debits (index(loyalty_transactions_credit_id))
            select
                *
            from loyalty_transactions_debits ltd
            where
                ltd.date_orig < @end_date
            ;
            """
    add_tmp2 = """
            create temporary table if not exists temp_credit_balances (index(cust_id))
            select
                ltc.id,
                ltc.cust_id,
                ltc.trans_amount as initital_credit_balance,
                coalesce(sum(ltd.trans_amount),0) as used_balance,
                ltc.trans_amount - coalesce(sum(ltd.trans_amount),0) as current_available_balance,
                ltc.status_id,
                ltc.date_orig as credit_issue_date
            from loyalty_transactions_credits ltc
            left join temp_loyalty_transactions_debits ltd on ltc.id = ltd.loyalty_transactions_credit_id
            where 
                (ltc.status_id = 1 and ltc.date_orig < @end_date) 
                or (ltc.date_expired >= @end_date and ltc.date_orig < @end_date)
                or (ltc.date_update >= @end_date and ltc.date_orig < @end_date)
            group by
                ltc.id
            ;
            """
    query = """
            select
                c.cust_id,
                sum(c.current_available_balance) as available_points_balance
            from temp_credit_balances c
            group by
                c.cust_id
            ;
            """
    mysql_cursor.execute(set_var)
    mysql_cursor.execute(drop_tmp1)
    mysql_cursor.execute(drop_tmp2)
    mysql_cursor.execute(add_tmp1)
    mysql_cursor.execute(add_tmp2)
    mysql_cursor.execute(query)
    result = mysql_cursor.fetchall()
    result_df = pd.DataFrame(data=result, index=None,
                       columns=['cust_id','available_points_balance']
                       )
    report_name = 'Loyalty_Points_By_Customer'
    filename = report_name + '_%s.csv' % current_date
    path_to_file = path + filename
    result_df.to_csv(path_to_file,index=False)

    prod_rec = []

    rec = prod_rec
    subject = report_name.replace('_', ' ')
    body = 'Attached is the most recent report for ' + subject + '.'
    files = path_to_file
    send_reports(rec, subject, body, files)

def report_210():

    drop_tmp1 = "drop temporary table if exists temp_loyalty_points_debits;"
    drop_tmp2 = "drop temporary table if exists temp_loyalty_points_expired;"
    add_tmp1 = """
            create temporary table if not exists temp_loyalty_points_debits
            select
                fcc.fiscal_year as credit_year,
                fcc.fiscal_month as credit_month,
                fcd.fiscal_year as debit_year,
                fcd.fiscal_month as debit_month,
                sum(ltd.trans_amount) as debits,
                case
                    when ltd.trans_type_id = 3 then 'Debits - Dollars'
                    when ltd.trans_type_id = 4 then 'Debits - CS Adjustment'
                end as debit_type
            
            from loyalty_transactions_debits ltd
            inner join loyalty_transactions_credits ltc on ltd.loyalty_transactions_credit_id = ltc.id
            inner join fiscal_calendar fcc on date(ltc.date_orig) = fcc.calendar_date
            inner join fiscal_calendar fcd on date(ltd.date_orig) = fcd.calendar_date
            group by
                fcc.fiscal_year,
                fcc.fiscal_month,
                fcd.fiscal_year,
                fcd.fiscal_month,
                ltd.trans_type_id
            ;
            """
    add_tmp2 = """
            create temporary table if not exists temp_loyalty_points_expired
            select
                fcc.fiscal_year as credit_year,
                fcc.fiscal_month as credit_month,
                fce.fiscal_year as debit_year,
                fce.fiscal_month as debit_month,
                sum(ltc.trans_amount - coalesce(debited,0)) as debits,
                'Expired' as debit_type
            from loyalty_transactions_credits ltc
            left join
            (
                select
                    ltd.loyalty_transactions_credit_id as credit_id,
                    sum(ltd.trans_amount) as debited
                from loyalty_transactions_debits ltd
                group by
                    ltd.loyalty_transactions_credit_id
            ) ltd on ltc.id = ltd.credit_id
            inner join customers_loyalty cl on ltc.cust_id = cl.cust_id
            inner join fiscal_calendar fcc on date(ltc.date_orig) = fcc.calendar_date
            inner join fiscal_calendar fce on coalesce(date(ltc.date_expired),date(ltc.date_update)) = fce.calendar_date
            where
                ltc.date_expired is not null
                or ltc.status_id = 0
            group by
                fcc.fiscal_year,
                fcc.fiscal_month,
                fce.fiscal_year,
                fce.fiscal_month
            ;
            """
    query = """
            select * from temp_loyalty_points_debits
            union
            select * from temp_loyalty_points_expired
            """
    mysql_cursor.execute(drop_tmp1)
    mysql_cursor.execute(drop_tmp2)
    mysql_cursor.execute(add_tmp1)
    mysql_cursor.execute(add_tmp2)
    mysql_cursor.execute(query)
    result = mysql_cursor.fetchall()
    result_df = pd.DataFrame(data=result, index=None,
                              columns=['credit_year', 'credit_month','debit_year', 'debit_month',
                                       'debits','debit_type'
                                       ]
                              )
    report_name = 'Loyalty_Waterfall_Data'
    filename =  report_name + '_%s.csv' % current_date
    path_to_file = path + filename
    result_df.to_csv(path_to_file,index=False)

    prod_rec = []

    rec = prod_rec
    subject = report_name.replace('_', ' ')
    body = 'Attached is the most recent report for ' + subject + '.'
    files = path_to_file
    send_reports(rec, subject, body, files)

def report_135():
    drop_tmp1 = "drop temporary table if exists temp_loyalty_points_credits;"
    drop_tmp2 = "drop temporary table if exists temp_loyalty_points_spend;"
    drop_tmp3 = "drop temporary table if exists temp_loyalty_points_debits;"
    drop_tmp4 = "drop temporary table if exists temp_loyalty_points_debits_ds_dollars;"
    drop_tmp5 = "drop temporary table if exists temp_loyalty_points_debits_cs_adjustment;"
    drop_tmp6 = "drop temporary table if exists temp_loyalty_points_expired;"
    drop_tmp7 = "drop temporary table if exists temp_ds_dollars_transfered;"
    drop_tmp8 = "drop temporary table if exists temp_ds_dollars_claimed;"
    drop_tmp9 = "drop temporary table if exists temp_ds_dollars_debited;"
    drop_tmp10 = "drop temporary table if exists temp_ds_dollars_expired;"
    add_tmp1 = """
            create temporary table if not exists temp_loyalty_points_credits (index(fiscal_year,fiscal_month))
            select
                fc.fiscal_year,
                fc.fiscal_month,
                
                sum(ltc.trans_amount) as points_earned,
                sum(ltc.trans_amount) * .01 as dollars_earned
                
            from loyalty_transactions_credits ltc
            inner join fiscal_calendar fc on date(ltc.date_orig) = fc.calendar_date
            group by
                fc.fiscal_year,
                fc.fiscal_month
            ;
            """
    add_tmp2 = """
            create temporary table if not exists temp_loyalty_points_spend (index(fiscal_year,fiscal_month))
            select
                fc.fiscal_year,
                fc.fiscal_month,
            
                sum(s.ship_total_products - s.ship_total_discount - s.ship_total_credit) as loyalty_dollars_spent
            
            from loyalty_transactions_credits ltc
            inner join shipments s on ltc.trans_entity_id = s.ship_id
            inner join fiscal_calendar fc on date(ltc.date_orig) = fc.calendar_date
            group by
                fc.fiscal_year,
                fc.fiscal_month
            ;
            """
    add_tmp3 = """
            create temporary table if not exists temp_loyalty_points_debits (index(fiscal_year,fiscal_month))
            select
                fc.fiscal_year,
                fc.fiscal_month,
            
                sum(ltd.trans_amount) as points_debited,
                sum(ltd.trans_amount) * .01 as dollars_debited
            
            from loyalty_transactions_debits ltd
            inner join fiscal_calendar fc on date(ltd.date_orig) = fc.calendar_date
            group by
                fc.fiscal_year,
                fc.fiscal_month
            ;
            """
    add_tmp4 = """
            create temporary table if not exists temp_loyalty_points_debits_ds_dollars (index(fiscal_year,fiscal_month))
            select
                fc.fiscal_year,
                fc.fiscal_month,
                
                sum(ltd.trans_amount) as points_debited,
                sum(ltd.trans_amount) * .01 as dollars_debited
                
            from loyalty_transactions_debits ltd
            inner join fiscal_calendar fc on date(ltd.date_orig) = fc.calendar_date
            where
                ltd.trans_type_id = 3
            group by
                fc.fiscal_year,
                fc.fiscal_month
            ;
            """
    add_tmp5 = """
            create temporary table if not exists temp_loyalty_points_debits_cs_adjustment (index(fiscal_year,fiscal_month))
            select
                fc.fiscal_year,
                fc.fiscal_month,
            
                sum(ltd.trans_amount) as points_debited,
                sum(ltd.trans_amount) * .01 as dollars_debited
            
            from loyalty_transactions_debits ltd
            inner join fiscal_calendar fc on date(ltd.date_orig) = fc.calendar_date
            where
                ltd.trans_type_id = 4
            group by
                fc.fiscal_year,
                fc.fiscal_month
            ;
            """
    add_tmp6 = """
            create temporary table if not exists temp_loyalty_points_expired (index(fiscal_year,fiscal_month))
            select
                fc.fiscal_year,
                fc.fiscal_month,
                sum(ltc.trans_amount - coalesce(debited,0)) as expired_points
            from loyalty_transactions_credits ltc
            left join
            (
                select
                    ltd.loyalty_transactions_credit_id as credit_id,
                    sum(ltd.trans_amount) as debited
                from loyalty_transactions_debits ltd
                group by
                    ltd.loyalty_transactions_credit_id
            ) ltd on ltc.id = ltd.credit_id
            inner join customers_loyalty cl on ltc.cust_id = cl.cust_id
            inner join fiscal_calendar fc on coalesce(date(ltc.date_expired),date(ltc.date_update)) = fc.calendar_date
            where 
                ltc.date_expired is not null 
                or ltc.status_id = 0
            group by
                fc.fiscal_year,
                fc.fiscal_month
            ;
            """
    add_tmp7 = """
            create temporary table if not exists temp_ds_dollars_transfered (index(fiscal_year,fiscal_month))
            select
                fc.fiscal_year,
                fc.fiscal_month,
                sum(mco.amount) as converted_dollars
            from marketing_credit_offer mco
            inner join fiscal_calendar fc on date(mco.date_orig) = fc.calendar_date
            where
                mco.marketing_credit_campaign_id = 36
            group by
                fc.fiscal_year,
                fc.fiscal_month	
            ;
            """
    add_tmp8 = """
            create temporary table if not exists temp_ds_dollars_claimed (index(fiscal_year,fiscal_month))
            select
                fc.fiscal_year,
                fc.fiscal_month,
                sum(mct.amount) as claimed_dollars
            from marketing_credit_transaction mct
            inner join fiscal_calendar fc on date(mct.date_orig) = fc.calendar_date
            where
                mct.marketing_credit_campaign_id = 36
                and mct.`type` = 1
            group by
                fc.fiscal_year,
                fc.fiscal_month	
            ;
            """
    add_tmp9 = """
            create temporary table if not exists temp_ds_dollars_debited (index(fiscal_year,fiscal_month))
            select
                fc.fiscal_year,
                fc.fiscal_month,
                sum(mct.amount) as debited_dollars
            from marketing_credit_transaction mct
            inner join fiscal_calendar fc on date(mct.date_orig) = fc.calendar_date
            where
                mct.marketing_credit_campaign_id = 36
                and mct.`type` = 0
            group by
                fc.fiscal_year,
                fc.fiscal_month	
            ;
            """
    drop_tmp11 = "drop temporary table if exists temp_marketing_credit_transaction;"

    add_tmp10 = """
            create temporary table if not exists temp_marketing_credit_transaction (index(marketing_credit_offer_id,type))
            select
                *
            from marketing_credit_transaction
            ;
            """
    drop_tmp12 = "drop temporary table if exists temp_marketing_credit_transaction2;"
    add_tmp11 = """      
            create temporary table if not exists temp_marketing_credit_transaction2 (index(marketing_credit_offer_id,type))
            select
                *
            from marketing_credit_transaction
            ;
            """
    add_tmp12 = """
            create temporary table if not exists temp_ds_dollars_expired (index(fiscal_year,fiscal_month))
            select
                fc.fiscal_year,
                fc.fiscal_month,
                sum(if(coalesce(mct.date_expiry,mco.date_expiry) < now(),coalesce(mct.amount,mco.amount) - coalesce(mct2.debit_amount,0),0)) as expired_dollars,
                coalesce(mct.date_expiry,mco.date_expiry) as date_expiry
            from marketing_credit_offer mco
            inner join customers c on mco.cust_id = c.cust_id
            left join temp_marketing_credit_transaction mct on mco.id = mct.marketing_credit_offer_id and mct.`type` = 1
            inner join fiscal_calendar fc on date(coalesce(mct.date_expiry,mco.date_expiry)) = fc.calendar_date
            left join 
            (
                select
                    cust_id,		
                    marketing_credit_offer_id,
                    count(distinct(id)) as debits,
                    sum(amount) as debit_amount
                from temp_marketing_credit_transaction2
                where
                    `type` = 0
                group by
                    cust_id,
                    marketing_credit_offer_id 
            ) mct2 on mco.cust_id = mct2.cust_id and mco.id = mct2.marketing_credit_offer_id
            where
                mco.marketing_credit_campaign_id = 36
            group by
                fc.fiscal_year,
                fc.fiscal_month	
            ;
            """
    query = """
                select
                tlp.fiscal_year,
                tlp.fiscal_month,
                tlps.loyalty_dollars_spent as loyalty_eligible_revenue,
                tlp.points_earned as loyalty_points_earned,
                tld.points_debited as loyalty_points_debited_total,
                tldds.points_debited as loyalty_points_debited_ds_dollars,
                tldcs.points_debited as loyalty_points_debited_cs_adjustments,
                tle.expired_points as loyalty_points_expired,
                tddt.converted_dollars,
                tddc.claimed_dollars,
                tddd.debited_dollars,
                tdde.expired_dollars
            from temp_loyalty_points_credits tlp
            inner join fiscal_calendar fc on tlp.fiscal_year = fc.fiscal_year and tlp.fiscal_month = fc.fiscal_month
            left join temp_loyalty_points_spend tlps on tlp.fiscal_year = tlps.fiscal_year and tlp.fiscal_month = tlps.fiscal_month
            left join temp_loyalty_points_debits tld on tlp.fiscal_year = tld.fiscal_year and tlp.fiscal_month = tld.fiscal_month
            left join temp_loyalty_points_debits_ds_dollars tldds on tlp.fiscal_year = tldds.fiscal_year and tlp.fiscal_month = tldds.fiscal_month
            left join temp_loyalty_points_debits_cs_adjustment tldcs on tlp.fiscal_year = tldcs.fiscal_year and tlp.fiscal_month = tldcs.fiscal_month
            left join temp_loyalty_points_expired tle on tlp.fiscal_year = tle.fiscal_year and tlp.fiscal_month = tle.fiscal_month
            left join temp_ds_dollars_transfered tddt on tlp.fiscal_year = tddt.fiscal_year and tlp.fiscal_month = tddt.fiscal_month
            left join temp_ds_dollars_claimed tddc on tlp.fiscal_year = tddc.fiscal_year and tlp.fiscal_month = tddc.fiscal_month
            left join temp_ds_dollars_debited tddd on tlp.fiscal_year = tddd.fiscal_year and tlp.fiscal_month = tddd.fiscal_month
            left join temp_ds_dollars_expired tdde on tlp.fiscal_year = tdde.fiscal_year and tlp.fiscal_month = tdde.fiscal_month
            group by
                fc.fiscal_year,
                fc.fiscal_month
            order by
                fc.calendar_date
            ;
            """
    mysql_cursor.execute(drop_tmp1)
    mysql_cursor.execute(drop_tmp2)
    mysql_cursor.execute(drop_tmp3)
    mysql_cursor.execute(drop_tmp4)
    mysql_cursor.execute(drop_tmp5)
    mysql_cursor.execute(drop_tmp6)
    mysql_cursor.execute(drop_tmp7)
    mysql_cursor.execute(drop_tmp8)
    mysql_cursor.execute(drop_tmp9)
    mysql_cursor.execute(drop_tmp10)
    mysql_cursor.execute(drop_tmp11)
    mysql_cursor.execute(drop_tmp12)
    mysql_cursor.execute(add_tmp1)
    mysql_cursor.execute(add_tmp2)
    mysql_cursor.execute(add_tmp3)
    mysql_cursor.execute(add_tmp4)
    mysql_cursor.execute(add_tmp5)
    mysql_cursor.execute(add_tmp6)
    mysql_cursor.execute(add_tmp7)
    mysql_cursor.execute(add_tmp8)
    mysql_cursor.execute(add_tmp9)
    mysql_cursor.execute(add_tmp10)
    mysql_cursor.execute(add_tmp11)
    mysql_cursor.execute(add_tmp12)
    mysql_cursor.execute(query)
    result = mysql_cursor.fetchall()
    result_df = pd.DataFrame(data=result, index=None,
                              columns=['fiscal_year', 'fiscal_month','loyalty_eligibility_revenue', 'loyalty_points_earned',
                                       'loyalty_points_debited_total','loyalty_points_debited_ds_dollars',
                                       'loyalty_points_debited_cs_adjustents','loyalty_points_expired',
                                       'converted_dollars','claimed_dollars',
                                       'debited_dollars','expired_dollars'
                                       ]
                              )
    report_name = 'Month_End_Loyalty_Points_Report'
    filename = report_name  + '_%s.csv' % current_date
    path_to_file = path + filename
    result_df.to_csv(path_to_file,index=False)

    prod_rec = []

    rec = prod_rec
    subject = report_name.replace('_', ' ')
    body = 'Attached is the most recent report for ' + subject + '.'
    files = path_to_file
    send_reports(rec, subject, body, files)

def report_63():
    query = """
            SELECT 
                DATE(o.order_date) AS calendar_date,
                fc.fiscal_year,
                fc.fiscal_month,
                 si.product_id,
                 p.product_description, 
                COUNT(DISTINCT(o.order_id)) AS total_shipments, 
                SUM(si.shipment_item_quantity) as total_units, 
                COUNT(DISTINCT(si.product_id)) as unique_products, 
                SUM(si.shipment_item_price * si.shipment_item_quantity) as gross_revenue, 
                SUM(si.shipment_item_discount * si.shipment_item_quantity) as discount,
                SUM(si.shipment_item_credit * si.shipment_item_quantity) as dollars,
                SUM(si.shipment_item_tax * si.shipment_item_quantity) as sales_tax,
                sum(si.shipment_item_quantity * (si.shipment_item_price - si.shipment_item_discount - si.shipment_item_credit)) as net_revenue
            FROM shipments_items si
            INNER JOIN shipments s ON si.shipment_id = s.shipment_id 
            INNER JOIN orders o ON s.order_id = o.order_id 
            INNER JOIN fiscal_calendar fc ON DATE(o.order_date) = fc.calendar_date
            INNER JOIN products p ON si.product_id = p.product_id
            WHERE 
                si.brand_id = 502561
                and o.order_status IN (100,110,1000,1300,1400) 
                AND o.cobrand_id NOT IN (2,23,24) 
                AND o.order_date >= '2018-01-28'
                AND (o.order_source != 'reship' OR o.order_source IS NULL) 
                AND si.shipment_id NOT IN (5174822)
                AND si.shipment_item_price > 0
                AND s.shipment_cancel_status = 0
            GROUP BY 
                DATE(o.order_date),
                si.product_id
            ;
    """
    result = get_mysql(db_creds, query)
    report_name = 'Harry_Josh_Month_End_All_Sales'
    filename =  report_name + '_%s.csv' % current_date
    path_to_file = path + filename
    result.to_csv(path_to_file,index=False)

    prod_rec = []

    rec = prod_rec
    subject = report_name.replace('_', ' ')
    body = 'Attached is the most recent report for ' + subject + '.'
    files = path_to_file
    send_reports(rec, subject, body, files)

def report_224():
    query = """
            SELECT 
                DATE(c.date_orig) AS calendar_date,
                fc.fiscal_year,
                fc.fiscal_month_num,
                i.id as product_id,
                i.description as product_description,
                COUNT(DISTINCT(c.credit_id)) AS total_credits, 
                SUM(si.ship_item_quantity) as total_units, 
                COUNT(DISTINCT(si.prod_id)) as unique_products, 
                SUM(si.ship_item_price * si.ship_item_quantity) as gross_revenue, 
                SUM(si.ship_item_discount * si.ship_item_quantity) as discount,
                SUM(si.ship_item_credit * si.ship_item_quantity) as dollars,
                SUM(si.ship_item_tax * si.ship_item_quantity) as sales_tax,
                sum(si.ship_item_quantity * (si.ship_item_price - si.ship_item_discount - si.ship_item_credit)) as net_revenue
            FROM credit_items ci
            INNER JOIN credits c ON ci.credit_id = c.credit_id
            INNER JOIN shipment_items si ON c.order_id = si.order_id AND ci.prod_id = si.prod_id
            INNER JOIN item i ON ci.prod_id = i.id
            INNER JOIN payment_transactions p ON c.trans_id = p.trans_id 
            INNER JOIN fiscal_calendar fc ON DATE(c.date_orig) = fc.calendar_date
            WHERE 
                fc.fiscal_year >= 2019
                AND p.trans_result_code = 0
                AND i.brand_id = 502561
            GROUP BY 
                DATE(c.date_orig),
                i.id
            ;
    """
    result = get_mysql(db_creds,query)
    report_name = 'Harry_Josh_Month_End_Credits'
    filename =  report_name + '_%s.csv' % current_date
    path_to_file = path + filename
    result.to_csv(path_to_file,index=False)

    prod_rec = []

    rec = prod_rec
    subject = report_name.replace('_', ' ')
    body = 'Attached is the most recent report for ' + subject + '.'
    files = path_to_file
    send_reports(rec, subject, body, files)

def report_66():
    query = """
            SELECT 
                DATE(c.date_orig) AS calendar_date,
                fiscal_year,
                fiscal_month,
                o.order_id,
                p.product_id,
                p.product_description,
                c.chargeback_amount as total_order_chargeback,
                SUM(oi.order_item_quantity * (oi.order_item_price - oi.order_item_discount - oi.order_item_credit)) as hj_chargeback_value
            FROM chargebacks c
            INNER JOIN orders o USING(order_id)
            INNER JOIN orders_items oi USING(order_id)
            INNER JOIN products p ON oi.product_id = p.product_id AND oi.order_item_price > 0
            INNER JOIN fiscal_calendar fc ON DATE(c.date_orig) = fc.calendar_date
            WHERE 
                p.brand_id = 502561 
                AND c.date_orig >= '2018-01-28'
            group by
               o.order_id,
               p.product_id
                ; 
    """
    result = get_mysql(db_creds,query)
    report_name = 'Harry_Josh_Month_End_Chargebacks'
    filename = report_name + '_%s.csv' % current_date
    path_to_file = path + filename
    result.to_csv(path_to_file,index=False)

    prod_rec = []

    rec = prod_rec
    subject = report_name.replace('_', ' ')
    body = 'Attached is the most recent report for ' + subject + '.'
    files = path_to_file
    send_reports(rec, subject, body, files)
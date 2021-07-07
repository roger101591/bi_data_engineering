import time
from warnings import filterwarnings
from kpi_loyalty.kpi_sql_v2 import *

filterwarnings('ignore', category = pymysql.Warning)
start_time = time.time()
table_name = 'kpi_snapshot_loyalty'
date_range = pd.date_range(date_start, date_end)
<<<<<<< Updated upstream
opt_ins = opt_ins()
ear_sql = ear_sql()
ever_redeemed_df = ever_redeemed()
=======


>>>>>>> Stashed changes

def opt_ins_total(opt_ins):
    opt_ins = opt_ins.set_index(opt_ins.calendar_date)
    opt_ins_total = opt_ins.drop(opt_ins.columns[0], axis=1)
    # vars ever active rolling
    ear = ear_sql.set_index(ear_sql.calendar_date)
    ear = ear.drop(ear.columns[0], axis=1)
    opt_ins_total = opt_ins_total.merge(ear, left_index = True, right_index = True, how = 'inner')
    opt_ins_total['never_purchased'] = opt_ins_total['total_enrolled'] - opt_ins_total['ever_active']
    return opt_ins_total

def cust_orders_rolling():
    print('Start Activity Buckets (recreate cust orders daily)')
    loyalty_customer_orders = loyalty_cust_orders()
    cust_orders_daily = loyalty_customer_orders.pivot(index = 'calendar_date', columns='customer_id', values='num_orders').fillna(0)
    cust_orders_rolling = cust_orders_daily.rolling(window = 365, min_periods=1).sum()
    del cust_orders_daily
    return cust_orders_rolling

def buckets_final(cust_orders_rolling):
    print('del cust_orders_daily pt 2')
    buckets = cust_orders_rolling.apply(pd.value_counts, axis = 1).fillna(0)
    del cust_orders_rolling
    buckets_trimmed = buckets.drop([buckets.columns[0]], axis=1)
    del buckets
    buckets_3_plus = buckets_trimmed.drop([buckets_trimmed.columns[0], buckets_trimmed.columns[1], buckets_trimmed.columns[2]], axis = 1)
    buckets_3_plus = buckets_3_plus.agg("sum", axis=1)
    buckets_trimmed = buckets_trimmed[[buckets_trimmed.columns[0], buckets_trimmed.columns[1], buckets_trimmed.columns[2]]]
    buckets_final = buckets_trimmed.merge(buckets_3_plus.to_frame(), left_index = True, right_index = True, how = 'inner')
    del buckets_trimmed
    del buckets_3_plus
    return buckets_final

def active_members(buckets_final):
    buckets_final = buckets_final.rename(index = str, columns={buckets_final.columns[3]: "advocated", buckets_final.columns[0]: "activated", buckets_final.columns[1]: "engaged", buckets_final.columns[2]: "grown"})
    active_members = buckets_final.agg("sum", axis = 1)
    active_members = active_members.to_frame()
    active_members.columns = ['active_members']
    return active_members

def member_buckets_final(opt_ins_total,active_members,buckets_final):
    opt_ins_total.index = pd.to_datetime(opt_ins_total.index)
    active_members.index = pd.to_datetime(active_members.index)
    active_members['active_members'] = active_members['active_members'].astype(int)
    buckets_final.index = pd.to_datetime(buckets_final.index)
    member_buckets_final = opt_ins_total.join(active_members)
    del active_members
    del opt_ins_total
    print('Del opt_ins_total and active_members')
    member_buckets_final['lapsed_members'] = member_buckets_final['ever_active'] - member_buckets_final['active_members']
    member_buckets_final = member_buckets_final.merge(buckets_final, left_index = True, right_index = True, how = 'inner')
    return member_buckets_final

def ever_redeemed(ever_redeemed_df):

    ever_redeemed = ever_redeemed_df.set_index(ever_redeemed_df['calendar_date'],drop=True)
    ever_redeemed = ever_redeemed.drop(ever_redeemed.columns[0],axis=1)
    return ever_redeemed

def kpi_first_df(member_buckets_final,ever_redeemed):
    kpi = member_buckets_final.merge(ever_redeemed.rename(index=str, columns={ever_redeemed.columns[0]: 'ever_redeemed'}), left_index = True, right_index = True, how = 'inner')
    kpi['total_percent_redeemed'] = kpi['ever_redeemed']/kpi['total_enrolled']
    del member_buckets_final
    del ever_redeemed
    print('deleted member_buckets_final')
    kpi_first_df = kpi
    return kpi_first_df

def has_redeemed():
    print('Creating redeemed rolling')
    cust_redeemed_daily = loyalty_cust_orders().pivot(index = 'calendar_date', columns='customer_id', values='redeemed_reward').fillna(0)
    cust_redeemed_daily = cust_redeemed_daily.where(cust_redeemed_daily==0, 1).astype(int)
    cust_redeemed_rolling = cust_redeemed_daily.rolling(window=365, min_periods=1).sum()
    cust_redeemed_rolling = cust_redeemed_rolling.where(cust_redeemed_rolling==0, 1)
    has_redeemed = cust_redeemed_rolling.apply(pd.value_counts, axis = 1).fillna(0)
    del cust_redeemed_rolling
    has_redeemed = has_redeemed.drop(has_redeemed.columns[0], axis=1)
    del cust_redeemed_daily
    return has_redeemed

def kpi_second_df(kpi_first_df,has_redeemed):
    kpi = kpi_first_df.merge(has_redeemed.rename(index = str, columns={has_redeemed.columns[0]: 'active_redeemed'}), left_index = True, right_index=True, how='inner')
    kpi['active_percent_redeemed'] = kpi['active_redeemed']/kpi['active_members']
    del has_redeemed
    print('Merged active redeemed column')
    kpi_second_df = kpi
    return kpi_second_df

def loyalty_orders_final(loyalty_customer_orders):
    #Customer Orders Roll Up - keep as is; executes quickly; no major memory usage
    print('Creating loyalty orders rollup')
    loyalty_orders_daily = loyalty_customer_orders.groupby(['calendar_date']).sum()[['Net_Rev','redeemed_reward']]
    del loyalty_customer_orders
    print('deleted loyalty_customer_orders')
    loyalty_rewards_rollup = loyalty_orders_daily.rolling(window=365, min_periods=1).sum()[['redeemed_reward']]
    loyalty_orders_final = pd.merge(loyalty_orders_daily, loyalty_rewards_rollup, on=['calendar_date'], how='inner')
    loyalty_orders_final = loyalty_orders_final.rename(index = str, columns={loyalty_orders_final.columns[0]: 'net_rev_earned', loyalty_orders_final.columns[1]: 'dates_ds_dollars_spent', loyalty_orders_final.columns[2]: 'rolling_year_ds_dollars'})
    del loyalty_orders_daily
    del loyalty_rewards_rollup
    print('Finish Customer Orders Rollup')
    return loyalty_orders_final

def loyalty_credits_daily_final():
    print('Pull loyalty credits')
    loyalty_credits = loyalty_cust_credits()
    #Loyalty Credits aggregate and roll up
    loyalty_credits_daily = loyalty_credits.groupby(['calendar_date']).sum()[['credits_earned','debits_redeemed','remaining_credit','expired']]
    del loyalty_credits
    loyalty_credits_daily_rolling = loyalty_credits_daily.expanding(min_periods=1).sum()[['remaining_credit']]
    loyalty_credits_daily_final = pd.merge(loyalty_credits_daily_rolling, loyalty_credits_daily, left_on = ['calendar_date'], right_on = ['calendar_date'], how='left')
    del loyalty_credits_daily_rolling
    del loyalty_credits_daily
    loyalty_credits_daily_final = loyalty_credits_daily_final.rename(index = str, columns={loyalty_credits_daily_final.columns[0]: 'outstanding_points', loyalty_credits_daily_final.columns[1]: 'dates_credits_earned', loyalty_credits_daily_final.columns[2]: 'debits_redeemed_from_dates_credits', loyalty_credits_daily_final.columns[3]: 'dates_remaining_credits', loyalty_credits_daily_final.columns[4]: 'dates_expired_credits'})
    return loyalty_credits_daily_final

def kpi_merge_credits(kpi_second_df,loyalty_orders_final,loyalty_credits_daily_final):
    print('finish loyalty credits and merging to final')
    kpi = kpi_second_df.merge(loyalty_credits_daily_final, left_index = True, right_index = True, how='inner')
    del loyalty_credits_daily_final
    kpi_merge_credits = kpi.merge(loyalty_orders_final, left_index=True,right_index=True,how='inner')
    del loyalty_orders_final
    print('merged credits and orders to final')
    return kpi_merge_credits

#Loyalty Debits Roll up
def daily_debits():
    print('Pulling daily debits')
    daily_debits = daily_debited()
    daily_debits.index = pd.DatetimeIndex(daily_debits.calendar_date)
    daily_debits = daily_debits[['points_debited']].reindex(date_range, fill_value=0)
    daily_debits.index = daily_debits.index.strftime('%Y-%m-%d')
    daily_debits = daily_debits.rename(index = str, columns={daily_debits.columns[0]: 'dates_points_debited'})
    return daily_debits

def loyalty_debits_rolling(daily_debits):
    loyalty_debits_rolling = daily_debits.rolling(window=365, min_periods=1).sum()
    loyalty_debits_rolling = loyalty_debits_rolling.rename(index=str,columns={loyalty_debits_rolling.columns[0]:'rolling_year_debits'})
    return loyalty_debits_rolling

def kpi_merge(kpi_merge_credits,daily_debits,loyalty_debits_rolling):
    kpi = kpi_merge_credits.merge(daily_debits, left_index=True,right_index=True,how='inner')
    kpi_merge = kpi.merge(loyalty_debits_rolling, left_index=True,right_index=True,how='inner')
    del daily_debits
    del loyalty_debits_rolling
    print('Merged and Deleted daily debits')
    return kpi_merge

def kpi(kpi_merge):
    print('Final adjustments and SQL Load')
    kpi_merge['CPP'] = kpi_merge['rolling_year_ds_dollars']/kpi_merge['rolling_year_debits']
    kpi_merge.reset_index(level=0, inplace=True)
    kpi = kpi_merge.rename(columns={kpi_merge.columns[0]: 'calendar_date'})
    kpi.calendar_date = pd.to_datetime(kpi.calendar_date).dt.date
    kpi.fiscal_year = kpi.fiscal_year.astype(int)
    return kpi

def kpi_df_load(kpi,table_name):
    load_table(kpi,table_name)

if __name__ == "__main__":
<<<<<<< Updated upstream
=======
    print("start opt ins dataset")
    opt_ins = opt_ins()
    print("end opt ins dataset")
    print("start new ear dataset")
    ear_sql = ear_sql()
    print("end ear dataset")
    print("start ever redeemed dataset")
    ever_redeemed_df = ever_redeemed()
    print("end ever redeemed dataset")
    load_loyalty_cust_data()
>>>>>>> Stashed changes
    opt_ins_total = opt_ins_total(opt_ins)
    cust_orders_rolling = cust_orders_rolling()
    buckets_final = buckets_final(cust_orders_rolling)
    active_members = active_members(buckets_final)
    member_buckets_final = member_buckets_final(opt_ins_total, active_members, buckets_final)
    ever_redeemed = ever_redeemed(ever_redeemed_df)
    kpi_first_df = kpi_first_df(member_buckets_final, ever_redeemed)
    has_redeemed = has_redeemed()
    kpi_second_df = kpi_second_df(kpi_first_df,has_redeemed)
    loyalty_orders_final = loyalty_orders_final(loyalty_cust_orders())
    loyalty_credits_daily_final = loyalty_credits_daily_final()
    kpi_merge_credits = kpi_merge_credits(kpi_second_df, loyalty_orders_final, loyalty_credits_daily_final)
    daily_debits = daily_debits()
    loyalty_debits_rolling = loyalty_debits_rolling(daily_debits)
    kpi_merge = kpi_merge(kpi_merge_credits,daily_debits,loyalty_debits_rolling)
    kpi = kpi(kpi_merge)
    kpi_df_load(kpi, table_name)


t_sec = round(time.time() - start_time)
(t_min, t_sec) = divmod(t_sec,60)
(t_hour,t_min) = divmod(t_min,60) 
print('Time passed: {}hour:{}min:{}sec'.format(t_hour,t_min,t_sec))
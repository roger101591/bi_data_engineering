from dictionaries import secrets
from tools import get_mysql


sql = """
select order_id, date_orig,
    case when lower(landing_request) like '%utm%' then landing_request
         when lower(landing_referrer) like '%utm%' then landing_referrer
    end as landing_url
from orders
where date_orig >= date(now()) - interval 1 day and 
(lower(landing_referrer) like '%utm%'
or lower(landing_referrer) like '%utm%')
"""

orders = get_mysql(secrets()["sl_creds"], sql)
orders["utm_source"] = orders.landing_url.str.extract(r"(?<=utm_source=)(.*?)(?=\b|&)")
orders["utm_medium"] = orders.landing_url.str.extract(r"(?<=utm_medium=)(.*?)(?=\b|&)")
orders["utm_campaign"] = orders.landing_url.str.extract(r"(?<=utm_campaign=)(.*?)(?=\b|&)")
orders["utm_term"] = orders.landing_url.str.extract(r"(?<=utm_term=)(.*?)(?=\b|&)")
orders["utm_content"] = orders.landing_url.str.extract(r"(?<=utm_content=)(.*?)(?=\b|&)")
orders

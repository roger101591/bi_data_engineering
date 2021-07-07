def sql():
    return {
        "landings": """
            select ml.session_id,
                case when ml.request regexp 'subaction' then ml.request
                    when ml.referrer regexp 'subaction' then ml.referrer
                    else ml.request
                end as landing_url,
                date(ml.date_orig) as session_date,
                1 as sessions
            from marketing_landings ml
            where ml.date_orig >= date(now()) - interval 1 day and ml.date_orig < date(now())
                and ml.source = 'email_bluecore'
        """,
        "bc_orders": """
            select o.session_id,
                1 as orders,
                oi.order_item_quantity * coalesce(
                    if(oi.order_item_msrp > 0,oi.order_item_msrp,null),oi.order_item_price
                    ) as gross_revenue,
                oi.order_item_quantity * oi.order_item_discount as discount,
                oi.order_item_quantity * (coalesce(
                    if(oi.order_item_msrp > 0,oi.order_item_msrp,null),oi.order_item_price
                    ) - oi.order_item_price) as sales_discount,
                oi.order_item_quantity * oi.order_item_credit as s_dollars,
                oi.order_item_quantity * (
                    oi.order_item_price - oi.order_item_discount - oi.order_item_credit
                ) as net_revenue
            from orders o
            inner join order_items oi using(order_id)
            where o.date_orig >= date(now()) - interval 1 day
                and o.order_source = 'email_bluecore'
                and o.order_status in (100,110,400,500,700,800,900,1000,1300,1400)
                and o.cobrand_id not in (2,23,24)
                and oi.order_item_price > 0
        """,
        "all_orders": """
            select o.customer_id,
                   o.order_id,
                   order_date,
                   brand_id,
                   product_id
            from orders o
            inner join orders_items oi on oi.order_id = o.order_id
            inner join customers c on c.customer_id = o.customer_id 
                and customer_last_order_date >= now() - interval 1 week
                

            where o.order_status in (100,110,400,500,700,800,900,1000,1300,1400)
                and o.cobrand_id not in (2,23,24, 29)
                and (o.order_source not in ('reship') or o.order_source is null)
        """,
    }

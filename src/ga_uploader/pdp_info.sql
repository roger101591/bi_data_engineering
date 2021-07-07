select
	p.product_id as 'ga:dimension18',
	b.brand_id as 'ga:dimension12',
	p.product_merchandising_first_attribute_name as 'ga:dimension31',
	p.product_merchandising_second_attribute_name as 'ga:dimension32',
	p.product_merchandising_third_attribute_name	as 'ga:dimension33'
From products p
inner join brands b on b.brand_id = p.brand_id
where
	p.product_status & 1 = 1
	and p.product_merchandising_first_attribute_name is not null
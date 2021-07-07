select
	p.product_id as 'ga:productSku',
	b.brand_id as 'ga:dimension4',
	b.brand_merchandising_business_unit as 'ga:dimension29',
	b.brand_merchandising_category as 'ga:dimension30',
	p.product_merchandising_first_attribute_name as 'ga:dimension34',
	p.product_merchandising_second_attribute_name as 'ga:dimension35',
	p.product_merchandising_third_attribute_name	as 'ga:dimension36',
	p.product_color as 'ga:dimension40'
From products p
inner join brands b on b.brand_id = p.brand_id
where
	p.product_status & 1 = 1
	and p.product_merchandising_first_attribute_name is not null
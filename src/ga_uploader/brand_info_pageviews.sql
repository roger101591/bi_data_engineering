select
	b.brand_id as 'ga:dimension12',
	b.brand_name as 'ga:dimension25',
	b.brand_merchandising_business_unit as 'ga:dimension26',
	b.brand_merchandising_category as 'ga:dimension27'
from brands b
where
b.brand_status_id & 1 = 1

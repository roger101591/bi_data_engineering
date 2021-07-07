select
	a.attribute_id as 'ga:dimension14',
	a1.attribute_name as 'ga:dimension37',
	a2.attribute_name as 'ga:dimension38',
	a3.attribute_name as 'ga:dimension39'
from attributes a
left join attributes a1 on a1.attribute_id = a.attribute_parent_id
left join attributes a2 on a2.attribute_id = a1.attribute_parent_id
left join attributes a3 on a3.attribute_id = a2.attribute_parent_id
where 
	a.attribute_status_id & 1 = 1
	and a1.attribute_name is not null
	and a.attribute_parent_id not in (select b.brand_id from dermstore_data.brands b)

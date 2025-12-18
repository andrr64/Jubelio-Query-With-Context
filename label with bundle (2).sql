-- SET SEARCH_PATH TO te06f6d63_0c61_48f6_922f_232b73db617c;

WITH SC AS (
 	SELECT * FROM jsonb_to_recordset(@list) as x(salesorder_id integer, city_code citext, district_code citext, shipping_service citext, logo_dropoff citext, logo_delivery citext, masked_customer_name citext, masked_customer_phone citext)
)
SELECT
	row_number, transaction_date, item_code, item_name, created_date, pick_date,
	variant, qty_in_base, dropshipper,  picklist_no, district_code, 
	source_name, source, type_shipping, sub_total, total, rack_no, note, username, 
	logo_store, store_id, store_name , shipper, shipping_cost, due_date, 
	district_cd, sort_code, insurance_cost, berat, tracking, customer_name, 
	phone, order_no, full_address, 
	buyer_shipping_cost, total_amount_mp, logo_shipper,
	-- bundle
	case when row_number() over (PARTITION BY order_no, item_code) = 1  
		then qty_in_base 
		else null 
	end as qty_in_base, -- qty in base master
	case when row_number() over (PARTITION BY order_no, item_code)=1 
		then berat 
		else null 
	end as berat, -- berat master
	itm.item_code_bundle, itm.item_name_bundle, 
	itm.variant_bundle, itm.qty*qty_in_base qty_bundle, 
	is_bundle,
	case when is_bundle = 1 
		then itm.qty * qty_in_base 
		else  qty_in_base 
	end as sum_qty
FROM  (
	SELECT 
		row_number() OVER (PARTITION BY order_no) AS row_number, transaction_date, item_code, item_name, created_date, pick_date,
		variant, SUM(qty_in_base) AS qty_in_base, case when is_dropshipper is true then cust else dropshipper end as dropshipper,  picklist_no,
		coalesce(district_cd, full_district_code) as district_code, 
		source_name, source, lower(type_shipping) as type_shipping, sub_total, total, rack_no, note, username, 
		case when lower(store_name) like '%gooroo%' then 'gooroo' else 'kalale' end as logo_store,
		store_id, store_name , shipper, shipping_cost, due_date, district_cd, sort_code,
		insurance_cost, SUM(berat) AS berat, COALESCE(tracking_number, tracking_no, ticket_no) AS tracking,
				customer_name, phone, order_no, concat_ws(' ',
				concat_ws(' ', shipping_address, shipping_area),
				concat_ws(' ', shipping_city, shipping_province, shipping_post_code),
				shipping_country ) AS full_address, 
		case when source=1 then shipping_cost else
		buyer_shipping_cost end buyer_shipping_cost, 
		case when source=1 then grand_total else
		case when total_amount_mp =0 or total_amount_mp is null then 0 else total_amount_mp end end as total_amount_mp, logo_shipper,
		item_id, is_bundle
	FROM (
			SELECT 
				substring(salesorder_no from position ('-' in salesorder_no) + 1) AS order_no,
				SC.masked_customer_name as customer_name,
				CASE WHEN h.source=0 THEN '' ELSE coalesce(h.shipping_address, c.s_address) END AS shipping_address,
				CASE WHEN h.source=64 THEN '' ELSE ', ' || coalesce(h.shipping_area, c.s_area) END AS shipping_area,
				CASE WHEN h.source=64 THEN '' ELSE coalesce(h.shipping_city, c.s_city) END AS shipping_city,
				CASE WHEN h.source=64 THEN '' ELSE ', ' || coalesce(h.shipping_province, c.s_province) END AS shipping_province,
				CASE WHEN h.source=64 THEN '' ELSE ', ' || coalesce(h.shipping_post_code, c.s_post_code) END AS shipping_post_code,
				CASE WHEN h.source=64 THEN '' ELSE coalesce(h.shipping_country) END AS shipping_country,
				SC.masked_customer_phone AS phone, 
				h.transaction_date, s.store_id,
				store_name, sd.shipper, 
				SC.logo_dropoff as logo_shipper,
				sd.tracking_no, sd.ticket_no, h.source, h.username, h.created_date, pr.created_date AS pick_date,
				i.item_code, i.item_name, coalesce(i.package_weight, g.package_weight) * sd.qty_in_base as berat, shipping_cost, insurance_cost, sd.qty_in_base, district_cd, sort_code,
				CASE WHEN i.variation_values <> '{}'
				THEN array_to_string(array(select unnest(i.variation_values) ->> 'value'), '-')
				ELSE ' ' END AS variant,
				CASE LENGTH(dropshipper)WHEN 0 THEN '' ELSE COALESCE(dropshipper, '') END AS dropshipper,
				lower(CASE
				WHEN h.payment_method like 'NON COD' THEN 'NON COD'
				WHEN h.payment_method like 'Cash on Delivery' THEN 'COD'
				WHEN h.payment_method like 'Cash_on_Delivery' THEN 'COD'
				WHEN h.payment_method like 'CashOnDelivery' THEN 'COD'
				WHEN h.payment_method like '%COD%' THEN 'COD'
				WHEN h.payment_method like '%Bayar di Tempat%' Then 'COD'
				WHEN h.is_cod is true then 'COD'
				ELSE 'NON COD' END) AS type_shipping, sub_total, due_date, destination_code, h.buyer_shipping_cost, h.total_amount_mp, h.grand_total,
				case when h.source IN (64,128, 131076) THEN total_amount_mp else grand_total end as total, i.rack_no, ph.picklist_no, h.note,
				concat_ws(E'\n', concat_ws('- ', city_code, district_code )) AS full_district_code,
				ch.channel_name AS source_name, c.is_dropshipper, c.contact_name as cust, h.tracking_number,
				i.item_id,
				case when i.is_bundle is true then 1 else 0 end as is_bundle
			FROM te06f6d63_0c61_48f6_922f_232b73db617c.salesorder_header h
			JOIN te06f6d63_0c61_48f6_922f_232b73db617c.salesorder_detail sd USING (salesorder_id)
			LEFT OUTER JOIN te06f6d63_0c61_48f6_922f_232b73db617c.store s ON h.store_id = s.store_id
			LEFT OUTER JOIN te06f6d63_0c61_48f6_922f_232b73db617c.contact c USING (contact_id)
			JOIN te06f6d63_0c61_48f6_922f_232b73db617c.item i ON sd.item_id = i.item_id
			JOIN te06f6d63_0c61_48f6_922f_232b73db617c.item_group g on g.item_group_id = i.item_group_id
			LEFT OUTER JOIN (
				SELECT min(salesorder_detail_id), pd.salesorder_id, picklist_id
				FROM te06f6d63_0c61_48f6_922f_232b73db617c.picklist_detail pd
				JOIN te06f6d63_0c61_48f6_922f_232b73db617c.salesorder_detail sd USING (salesorder_detail_id)
				where sd.salesorder_id in (@ids)
				GROUP BY pd.salesorder_id, picklist_id
			) pick ON pick.salesorder_id = sd.salesorder_id
			LEFT OUTER JOIN te06f6d63_0c61_48f6_922f_232b73db617c.picklist_header ph ON pick.picklist_id = ph.picklist_id
			LEFT OUTER JOIN te06f6d63_0c61_48f6_922f_232b73db617c.picklist_header pr ON h.picked_in = pr.picklist_id
			LEFT JOIN SC on SC.salesorder_id = h.salesorder_id
			LEFT JOIN te06f6d63_0c61_48f6_922f_232b73db617c.channel ch on ch.channel_id = h.source
			WHERE h.salesorder_id IN (@ids)
	) t 
	GROUP BY transaction_date, item_code, item_name, source_name, source, variant, store_name, shipper, logo_shipper,
      	insurance_cost, shipping_cost, berat, tracking, customer_name, phone, order_no, qty_in_base, due_date, username, store_id, district_cd, sort_code,
      	type_shipping, sub_total, total, rack_no, dropshipper, picklist_no, note, full_address, destination_code, created_date, pick_date,
      	district_code, buyer_shipping_cost, total_amount_mp, is_dropshipper, cust, grand_total, item_id, is_bundle
	order by order_no
) t2
left outer join (
	select i2.item_id as item_id, itm.item_id as item_id_bundle,
    itm.item_code as item_code_bundle, itm.item_name as item_name_bundle, bc.qty as qty,
    CASE WHEN itm.variation_values <> '{}' 
              THEN array_to_string(array(select unnest(itm.variation_values) ->> 'value'), '-')
            ELSE ' ' END AS variant_bundle
	from te06f6d63_0c61_48f6_922f_232b73db617c.item i2
	LEFT OUTER JOIN te06f6d63_0c61_48f6_922f_232b73db617c.bom b ON i2.item_id = b.item_id
    LEFT OUTER JOIN te06f6d63_0c61_48f6_922f_232b73db617c.bom_composition bc ON b.bom_id = bc.bom_id
    LEFT OUTER JOIN te06f6d63_0c61_48f6_922f_232b73db617c.item itm ON itm.item_id = bc.item_id
    where i2.is_bundle is true and itm.item_code is not null
)itm on itm.item_id = t2.item_id;
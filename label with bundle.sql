--SET SEARCH_PATH TO tsatbfs6itrqdaysjhahybw;
WITH SC AS (
  SELECT *
  FROM jsonb_to_recordset(@list) as x(
      salesorder_id integer,
      city_code citext,
      district_code citext,
      shipping_service citext,
      logo_dropoff citext,
      logo_delivery citext,
      masked_customer_name citext,
      masked_customer_phone citext
    )
)
SELECT row_number() OVER (PARTITION BY order_no) AS row_number,
  transaction_date,
  item_code,
  item_name,
  created_date,
  pick_date,
  variant,
  SUM(qty_in_base) AS qty_in_base,
  dropshipper,
  picklist_no,
  coalesce(district_cd, '') as district_code,
  source_name,
  source,
  lower(type_shipping) as type_shipping,
  sub_total,
  total,
  rack_no,
  regexp_replace(
    trim(
      both E'\u200B\u200C\u200D\u00A0 '
      FROM note
    ),
    '[\s\u200B\u200C\u200D\u00A0]+',
    ' ',
    'g'
  ) note,
  username,
  LEFT(store_name, 3) AS logo_store,
  store_id,
  store_name,
  shipper,
  logo_shipper,
  shipping_cost,
  due_date,
  district_cd,
  sort_code,
  insurance_cost,
  SUM(berat) AS berat,
  COALESCE(tracking_no, ticket_no) AS tracking,
  customer_name,
  phone,
  order_no,
  concat_ws(
    ' ',
    concat_ws(' ', shipping_address, shipping_area),
    concat_ws(
      ' ',
      shipping_city,
      shipping_province,
      shipping_post_code
    ),
    shipping_country
  ) AS full_address,
  buyer_shipping_cost,
  total_amount_mp,
  shipping_areaa,
  shipping_cityy,
  shipping_service,
  zone_name,
  zone_id,
  return_zone_id,
  shipping_subdistrict,
  store_phone,
  store_address,
  case
    when is_cod_check is true then 'Yes'
    else 'No'
  end as is_cod_check,
  pick_code,
  is_instant,
  store_logo,
  bin_final_code
FROM (
    SELECT substring(
        salesorder_no
        from position ('-' in salesorder_no) + 1
      ) AS order_no,
      SC.masked_customer_name as customer_name,
      CASE
        WHEN h.source = 0 THEN ''
        ELSE coalesce(h.shipping_address, c.s_address)
      END AS shipping_address,
      CASE
        WHEN h.source = 64 THEN ''
        ELSE ', ' || coalesce(h.shipping_area, c.s_area)
      END AS shipping_area,
      CASE
        WHEN h.source = 64 THEN ''
        ELSE coalesce(h.shipping_city, c.s_city)
      END AS shipping_city,
      CASE
        WHEN h.source = 64 THEN ''
        ELSE ', ' || coalesce(h.shipping_province, c.s_province)
      END AS shipping_province,
      CASE
        WHEN h.source = 64 THEN ''
        ELSE ', ' || coalesce(h.shipping_post_code, c.s_post_code)
      END AS shipping_post_code,
      CASE
        WHEN h.source = 64 THEN ''
        ELSE coalesce(h.shipping_country)
      END AS shipping_country,
      SC.masked_customer_phone AS phone,
      h.transaction_date,
      s.store_id,
      store_name,
      sd.shipper,
      SC.logo_delivery as logo_shipper,
      sd.tracking_no,
      sd.ticket_no,
      h.source,
      h.username,
      h.created_date,
      ph.created_date AS pick_date,
      i.item_code,
      i.item_name,
      coalesce(i.package_weight, g.package_weight) * sd.qty_in_base as berat,
      shipping_cost,
      insurance_cost,
      sd.qty_in_base,
      district_cd,
      sort_code,
      CASE
        WHEN i.variation_values <> '{}' THEN array_to_string(
          array(
            select unnest(i.variation_values)->>'value'
          ),
          '-'
        )
        ELSE ' '
      END AS variant,
      CASE
        LENGTH(dropshipper)
        WHEN 0 THEN ''
        ELSE COALESCE(dropshipper, '')
      END AS dropshipper,
      lower(
        CASE
          WHEN h.payment_method like 'NON COD' THEN 'NON COD'
          WHEN h.payment_method like 'Cash on Delivery' THEN 'COD'
          WHEN h.payment_method like 'Cash_on_Delivery' THEN 'COD'
          WHEN h.payment_method like 'CashOnDelivery' THEN 'COD'
          WHEN h.payment_method like '%COD%' THEN 'COD'
          WHEN h.payment_method like '%Bayar di Tempat%' Then 'COD'
          WHEN h.is_cod is true then 'COD'
          ELSE 'NON COD'
        END
      ) AS type_shipping,
      sub_total,
      due_date,
      destination_code,
      h.buyer_shipping_cost,
      coalesce(
        h.total_amount_mp,
        (h.grand_total + h.buyer_shipping_cost)
      ) as total_amount_mp,
      case
        h.source
        when 64 THEN total_amount_mp
        else grand_total
      end as total,
      i.rack_no,
      ph.picklist_no,
      h.note,
      concat_ws(
        E'\n',
        concat_ws('- ', city_code, district_code)
      ) AS full_district_code,
      ch.channel_name AS source_name,
      h.shipping_area as shipping_areaa,
      h.shipping_city as shipping_cityy,
      SC.shipping_service,
      zone_name,
      zone_id,
      h.return_zone_id,
      s.store_phone,
      coalesce(
        s.address,
        split_part(
          (setting->'shopee_default_pickup_address')->>'address',
          ' - ',
          2
        )
      ) as store_address,
      h.shipping_subdistrict,
      (h.extra_info->>'shopee_cod_mutual_check')::bool as is_cod_check,
      coalesce(h.extra_info->>'pickup_code', '') as pick_code,
      case
        when shipping_service ilike '%sameday%'
        or shipping_service ilike '%int%' then 1
        else 0
      end as is_instant,
      coalesce(
        (
          select unnest(logo)->>'thumbnail'
          from tsatbfs6itrqdaysjhahybw.store st
          where st.store_id = s.store_id
          limit 1
        ), ''
      ) as store_logo,
      pick.bin_final_code
    FROM tsatbfs6itrqdaysjhahybw.salesorder_header h
      JOIN tsatbfs6itrqdaysjhahybw.salesorder_detail sd on sd.salesorder_id = h.salesorder_id
      LEFT OUTER JOIN tsatbfs6itrqdaysjhahybw.store s ON h.store_id = s.store_id
      LEFT OUTER JOIN tsatbfs6itrqdaysjhahybw.contact c USING (contact_id)
      JOIN tsatbfs6itrqdaysjhahybw.item i ON sd.item_id = i.item_id
      JOIN tsatbfs6itrqdaysjhahybw.item_group g on g.item_group_id = i.item_group_id
      LEFT OUTER JOIN (
        SELECT salesorder_detail_id,
          pd.salesorder_id,
          picklist_id,
          max(bin_final_code) bin_final_code
        FROM tsatbfs6itrqdaysjhahybw.picklist_detail pd
          JOIN tsatbfs6itrqdaysjhahybw.salesorder_detail sd USING (salesorder_detail_id)
          left join tsatbfs6itrqdaysjhahybw.bin b on b.bin_id = pd.bin_id
        where sd.salesorder_id in (@ids)
        GROUP BY pd.salesorder_id,
          picklist_id,
          salesorder_detail_id
      ) pick ON pick.salesorder_detail_id = sd.salesorder_detail_id
      LEFT OUTER JOIN tsatbfs6itrqdaysjhahybw.picklist_header ph ON pick.picklist_id = ph.picklist_id
      LEFT JOIN SC on SC.salesorder_id = h.salesorder_id
      LEFT JOIN tsatbfs6itrqdaysjhahybw.channel ch ON ch.channel_id = h.source
    WHERE h.salesorder_id IN (@ids)
  ) t
GROUP BY transaction_date,
  item_code,
  item_name,
  source_name,
  source,
  variant,
  store_name,
  shipper,
  logo_shipper,
  store_logo,
  insurance_cost,
  shipping_cost,
  tracking,
  customer_name,
  phone,
  order_no,
  due_date,
  username,
  store_id,
  district_cd,
  sort_code,
  type_shipping,
  sub_total,
  total,
  rack_no,
  dropshipper,
  picklist_no,
  note,
  full_address,
  destination_code,
  created_date,
  pick_date,
  district_code,
  buyer_shipping_cost,
  total_amount_mp,
  shipping_areaa,
  shipping_cityy,
  shipping_service,
  zone_name,
  zone_id,
  return_zone_id,
  shipping_subdistrict,
  store_phone,
  store_address,
  is_cod_check,
  pick_code,
  is_instant,
  bin_final_code
order by order_no;
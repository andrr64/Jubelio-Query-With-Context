WITH 
    status_date AS 
        (
            SELECT 
                salesorder_id,
                MAX(CASE WHEN action_id = '900' THEN created_date END) AS rts_created_date,
                MAX(CASE WHEN action_id = '999' THEN created_date END) AS shp_created_date,
                MAX(CASE WHEN action_id = '50' THEN created_date END) AS cncl_created_date,
                MAX(CASE WHEN action_id = '912' THEN created_date END) AS cmplt_created_date,
                MAX(CASE WHEN action_id = '120' THEN created_date END) AS paid_created_date,
                MAX(CASE WHEN action_id = '300' THEN created_date END) AS start_pick_created_date,
                MAX(CASE WHEN action_id = '600' THEN created_date END) AS finish_pick_created_date,
                MAX(CASE WHEN action_id = '700' THEN created_date END) AS start_pack_created_date,
                MAX(CASE WHEN action_id = '800' THEN created_date END) AS finish_pack_created_date
            FROM 
                {0}.salesorder_status
            GROUP BY 
                salesorder_id
    )
SELECT 
    sh.transaction_date "Tanggal Pesanan",
    sh.salesorder_no "No Pesanan",
    finish_pack_created_date::timestamptz "Tanggal Finish Pack",
    paid_created_date::timestamptz "Tanggal Paid",
    rts_created_date::timestamptz "Tanggal RTS",
    start_pick_created_date::timestamptz "Tanggal Mulai Picking",
    s.store_name "Nama Toko",
    l.location_name "Lokasi Gudang",
    ch.channel_name "Channel", 
    cs.status "Status MP",
    sh.wms_status "Status WMS",
    coalesce(sh.customer_name, c.contact_name) as customer_name, 
    c.Email "Email Customer",
    i.item_code, 
    i.item_name,
    case when i.is_bundle then 'Ya' else 'Tidak' end as "Is Bundle",
    itm.item_code_bundle "SKU Bundle",
	  itm.item_name_bundle "Nama Barang Bundle",
    CASE WHEN 
        i.variation_values <> '{{}}'
      THEN 
        array_to_string(array(select unnest(i.variation_values) ->> 'value'), '-')
      ELSE 
        ' ' 
      END AS "Variant - Size", 
    round(coalesce(sd.price,0)) "Harga Reguler (Per Pcs)",
    round(coalesce(sd.qty_in_base,0)) "Qty",
    round(coalesce(sd.disc_amount,0)) "Diskon Per Item",
    round(coalesce(sd.tax_amount,0)) "Pajak",
    sh.sub_total "Sub Total",
    coalesce(sh.service_fee, 0) * ((sd.price * sd.qty_in_base) / (nullif(sum(sd.price * sd.qty_in_base) over(partition by sh.salesorder_no), 0))) "Biaya Lainnya",
    coalesce(sh.add_fee, 0) * ((sd.price * sd.qty_in_base) / (nullif(sum(sd.price * sd.qty_in_base) over(partition by sh.salesorder_no), 0))) "Potongan Biaya",
    coalesce(sh.shipping_cost, 0) * ((sd.price * sd.qty_in_base) / (nullif(sum(sd.price * sd.qty_in_base) over(partition by sh.salesorder_no), 0))) "Biaya Ongkir",
    coalesce(sh.insurance_cost, 0) * ((sd.price * sd.qty_in_base) / (nullif(sum(sd.price * sd.qty_in_base) over(partition by sh.salesorder_no), 0))) "Asuransi",
    round(coalesce(sd.price)*coalesce(sd.qty_in_base) - coalesce(sd.disc_amount,0)) "Total Harga",
    round(im.cogs,2) "HPP",
    ROUND((sd.price * sd.qty_in_base) - sd.disc_amount + sd.tax_amount
    - coalesce(sh.add_disc, 0) * ((sd.price * sd.qty_in_base) / (nullif(sum(sd.price * sd.qty_in_base) over(partition by sh.salesorder_no), 0)))
    - coalesce(sh.service_fee, 0) * ((sd.price * sd.qty_in_base) / (nullif(sum(sd.price * sd.qty_in_base) over(partition by sh.salesorder_no), 0)))
    + coalesce(sh.add_fee, 0) * ((sd.price * sd.qty_in_base) / (nullif(sum(sd.price * sd.qty_in_base) over(partition by sh.salesorder_no), 0)))
    + coalesce(sh.shipping_cost, 0) * ((sd.price * sd.qty_in_base) / (nullif(sum(sd.price * sd.qty_in_base) over(partition by sh.salesorder_no), 0)))
    + coalesce(sh.insurance_cost, 0) * ((sd.price * sd.qty_in_base) / (nullif(sum(sd.price * sd.qty_in_base) over(partition by sh.salesorder_no), 0))),2)
    AS "Nett Sales",
    ((sh.sub_total - sh.total_disc - coalesce (sh.add_disc, 0)) + coalesce (sh.add_fee, 0)) - im.cogs as gross_profit,
    c.s_address "Alamat Penerimaan",
    c.s_city "Kota Alamat Pelanggan"
    -- sh.salesorder_no doc_no,  
    -- sh.transaction_date, 
    -- c.phone, 
    -- round(sd.qty_in_base) AS qty_in_base, 
    -- round(sd.amount) amount, 
    -- cs.internal_status, 
    -- regexp_replace(coalesce(sh.note, ih.note), E'[\\n\\r]+', ' ', 'g' ) note
FROM 
    {0}.salesorder_header sh
JOIN
    {0}.salesorder_detail sd USING (salesorder_id)
JOIN
    {0}.item i USING (item_id)
LEFT JOIN 
    {0}.store s ON s.store_id = sh.store_id
LEFT OUTER JOIN 
    {0}.contact c on c.contact_id = sh.contact_id
LEFT JOIN 
    {0}.invoice_header ih on ih.ref_no = sh.salesorder_no
LEFT JOIN
    {0}.channel_status cs ON cs.channel_status = sh.channel_status AND cs.source = sh.source
LEFT JOIN
    {0}.channel ch ON ch.channel_id = sh.source
left join 
    status_date as s_date on s_date.salesorder_id = sh.salesorder_id
LEFT JOIN 
    {0}.location l ON l.location_id = sh.location_id
left join (
    select 
      trx_no, sum(amount)*-1 as cogs
    from 
      {0}.item_movement im
    Where 
      invoice_detail_id is not null
    group by 
      trx_no
  ) im on im.trx_no = sh.salesorder_no
left outer join (
  select 
    i2.item_id as item_id, i2.item_code, i2.item_name, itm.item_id as item_id_bundle,
    itm.item_code as item_code_bundle, itm.item_name as item_name_bundle, bc.qty as qty_bundle,
    CASE WHEN itm.variation_values <> '{{}}'
    THEN array_to_string(array(select unnest(itm.variation_values) ->> 'value'), '-')
    ELSE ' ' END AS variant_bundle, i2.is_bundle
  from 
    {0}.item i2
  LEFT OUTER JOIN 
    {0}.bom b ON i2.item_id = b.item_id
  LEFT OUTER JOIN 
    {0}.bom_composition bc ON b.bom_id = bc.bom_id
  LEFT OUTER JOIN 
    {0}.item itm ON itm.item_id = bc.item_id
  where 
    i2.is_bundle is true and itm.item_code is not null
) itm on itm.item_id = sd.item_id
left join (
select 
    salesorder_detail_id, sum(iqc.average_cost * sd.qty_in_base) as average_cost
    from 
    {0}.salesorder_detail sd
    left join 
    {0}.item_qty_cogs iqc on iqc.item_id = sd.item_id
    group by 
    1
) iqc on iqc.salesorder_detail_id = sd.salesorder_detail_id
WHERE 
    sh.transaction_date BETWEEN @date_from AND @date_to and sh.is_paid
    and (coalesce(cs.internal_status, '') <> 'CANCELED' AND sh.is_canceled is not true)
    and sh.location_id = any(@lds)
    and (CASE WHEN sh.source = 64 THEN sh.transaction_date >= current_date - interval '90 days' ELSE true END)
UNION ALL
SELECT 
    ih.transaction_date "Tanggal Pesanan",
    ih.invoice_no "No Pesanan",
    finish_pack_created_date::timestamptz "Tanggal Finish Pack",
    paid_created_date::timestamptz "Tanggal Paid",
    rts_created_date::timestamptz "Tanggal RTS",
    start_pick_created_date::timestamptz "Tanggal Mulai Picking",
    s.store_name "Nama Toko",
    l.location_name "Lokasi Gudang",
    ch.channel_name "Channel", 
    NULL "Status MP",
    NULL "Status WMS",
    coalesce(ih.customer_name, c.contact_name) as customer_name, 
    c.Email "Email Customer",
    case when i.is_bundle then 'Ya' else 'Tidak' end as "Is Bundle",
    itm.item_code_bundle "SKU Bundle",
	  itm.item_name_bundle "Nama Barang Bundle",
    i.item_code, 
    i.item_name,
    CASE WHEN 
        i.variation_values <> '{{}}'
      THEN 
        array_to_string(array(select unnest(i.variation_values) ->> 'value'), '-')
      ELSE 
        ' ' 
      END AS "Variant - Size", 
    round(coalesce(id.price,0)) "Harga Reguler (Per Pcs)",
    round(coalesce(id.qty_in_base,0)) "Qty",
    round(coalesce(id.disc_amount,0)) "Diskon Per Item",
    round(coalesce(id.tax_amount,0)) "Pajak",
    ih.sub_total as "Sub Total",
    coalesce(ih.service_fee, 0) * ((id.price * id.qty_in_base) / (nullif(sum(id.price * id.qty_in_base) over(partition by ih.invoice_no), 0))) "Biaya Lainnya",
    coalesce(ih.add_fee, 0) * ((id.price * id.qty_in_base) / (nullif(sum(id.price * id.qty_in_base) over(partition by ih.invoice_no), 0))) "Potongan Biaya",
    coalesce(ih.shipping_cost, 0) * ((id.price * id.qty_in_base) / (nullif(sum(id.price * id.qty_in_base) over(partition by ih.invoice_no), 0))) "Biaya Ongkir",
    coalesce(ih.insurance_cost, 0) * ((id.price * id.qty_in_base) / (nullif(sum(id.price * id.qty_in_base) over(partition by ih.invoice_no), 0))) "Asuransi",
    round(coalesce(id.price)*coalesce(id.qty_in_base) - coalesce(id.disc_amount,0)) "Total Harga",
    round(im.cogs,2) "HPP",
    ROUND((id.price * id.qty_in_base) - id.disc_amount + id.tax_amount
    - coalesce(ih.add_disc, 0) * ((id.price * id.qty_in_base) / (nullif(sum(id.price * id.qty_in_base) over(partition by ih.invoice_no), 0)))
    - coalesce(ih.service_fee, 0) * ((id.price * id.qty_in_base) / (nullif(sum(id.price * id.qty_in_base) over(partition by ih.invoice_no), 0)))
    + coalesce(ih.add_fee, 0) * ((id.price * id.qty_in_base) / (nullif(sum(id.price * id.qty_in_base) over(partition by ih.invoice_no), 0)))
    + coalesce(ih.shipping_cost, 0) * ((id.price * id.qty_in_base) / (nullif(sum(id.price * id.qty_in_base) over(partition by ih.invoice_no), 0)))
    + coalesce(ih.insurance_cost, 0) * ((id.price * id.qty_in_base) / (nullif(sum(id.price * id.qty_in_base) over(partition by ih.invoice_no), 0))),2)
    AS "Nett Sales",
    ((ih.sub_total - ih.total_disc - coalesce (ih.add_disc, 0)) + coalesce (ih.add_fee, 0)) - im.cogs as gross_profit,
    c.s_address "Alamat Penerimaan",
    c.s_city "Kota Alamat Pelanggan"
FROM 
    {0}.invoice_header ih
JOIN
    {0}.invoice_detail id USING (invoice_id)
JOIN
    {0}.item i USING (item_id)
LEFT JOIN 
    {0}.store s ON s.store_id = ih.store_id
LEFT OUTER JOIN 
    {0}.contact c on c.contact_id = ih.contact_id
LEFT JOIN
    {0}.channel ch ON ch.channel_id = ih.source
left join 
    status_date as s_date on s_date.salesorder_id = ih.invoice_id
LEFT JOIN 
    {0}.location l ON l.location_id = ih.location_id
left join (
    select 
      trx_no, sum(amount)*-1 as cogs
    from 
      {0}.item_movement im
    Where 
      invoice_detail_id is not null
    group by 
      trx_no
  ) im on im.trx_no = ih.invoice_no
 left join (
    select 
      invoice_detail_id, sum(iqc.average_cost * id.qty_in_base) as average_cost
      from 
        {0}.invoice_detail id
      left join 
        {0}.item_qty_cogs iqc on iqc.item_id = id.item_id
      group by 
        1
  ) iqc on iqc.invoice_detail_id = id.invoice_detail_id
left outer join (
  select 
    i2.item_id as item_id, i2.item_code, i2.item_name, itm.item_id as item_id_bundle,
    itm.item_code as item_code_bundle, itm.item_name as item_name_bundle, bc.qty as qty_bundle,
    CASE WHEN itm.variation_values <> '{{}}'
    THEN array_to_string(array(select unnest(itm.variation_values) ->> 'value'), '-')
    ELSE ' ' END AS variant_bundle, i2.is_bundle
  from 
    {0}.item i2
  LEFT OUTER JOIN 
    {0}.bom b ON i2.item_id = b.item_id
  LEFT OUTER JOIN 
    {0}.bom_composition bc ON b.bom_id = bc.bom_id
  LEFT OUTER JOIN 
    {0}.item itm ON itm.item_id = bc.item_id
  where 
    i2.is_bundle is true and itm.item_code is not null
) itm on itm.item_id = id.item_id
WHERE 
    ih.transaction_date BETWEEN @date_from AND @date_to and id.salesorder_detail_id is null
    and ih.location_id = any(@lds)
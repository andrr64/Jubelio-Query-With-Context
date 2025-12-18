-- is shopee kilat
case when sh.booking_no is null or sh.booking_no ='' then 'Tidak' else 'Ya' end as is_kilat,
sh.booking_no
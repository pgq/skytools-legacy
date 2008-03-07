create or replace function londiste.find_column_types(tbl text)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: londiste.find_column_types(1)
--
--      Returns columnt type string for logtriga().
--
-- Parameters:
--      tbl - fqname
--
-- Returns:
--      String of 'kv'.
-- ----------------------------------------------------------------------
declare
    res      text;
    col      record;
    tbl_oid  oid;
begin
    tbl_oid := londiste.find_table_oid(tbl);
    res := '';
    for col in 
        SELECT CASE WHEN k.attname IS NOT NULL THEN 'k' ELSE 'v' END AS type
            FROM pg_attribute a LEFT JOIN (
                SELECT k.attname FROM pg_index i, pg_attribute k
                 WHERE i.indrelid = tbl_oid AND k.attrelid = i.indexrelid
                   AND i.indisprimary AND k.attnum > 0 AND NOT k.attisdropped
                ) k ON (k.attname = a.attname)
            WHERE a.attrelid = tbl_oid AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
    loop
        res := res || col.type;
    end loop;

    return res;
end;
$$ language plpgsql strict stable;


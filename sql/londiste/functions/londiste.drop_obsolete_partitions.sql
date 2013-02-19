
create or replace function londiste.drop_obsolete_partitions
(
  in i_parent_table text,
  in i_retention_period interval
)
  returns setof text
as $$
-------------------------------------------------------------------------------
--  Function: londiste.drop_obsolete_partitions(2)
--
--    Drop obsolete partitions of partition-by-date parent table.
--
--  Parameters:
--    i_parent_table      Master table from which partitions are inherited
--    i_retention_period  How long to keep partitions around
--
--  Returns:
--    Names of partitions dropped
-------------------------------------------------------------------------------
declare
  _schema text not null := lower (split_part (i_parent_table, '.', 1));
  _table  text not null := lower (split_part (i_parent_table, '.', 2));
  _part   text;
begin
  for _part in
    select quote_ident (t.schemaname) ||'.'|| quote_ident (t.tablename)
      from pg_catalog.pg_tables t
     where t.schemaname = _schema
       and t.tablename ~ ('^'|| _table ||'_[0-9]{4}_[0-9]{2}_[0-9]{2}$')
       and t.tablename < _table || to_char (now() - i_retention_period, '_YYYY_MM_DD')
  loop
    execute 'drop table '|| _part;
    return next _part;
  end loop;
end;
$$ language plpgsql;

-- vim:et:sw=2:ts=2:nowrap:

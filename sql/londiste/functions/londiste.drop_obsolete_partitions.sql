
create or replace function londiste.drop_obsolete_partitions
(
    in i_parent_table text,
    in i_retention_period interval,
    in i_partition_period text
)
    returns setof text
as $$
-------------------------------------------------------------------------------
--  Function: londiste.drop_obsolete_partitions(3)
--
--    Drop obsolete partitions of partition-by-date parent table.
--
--  Parameters:
--    i_parent_table        Master table from which partitions are inherited
--    i_retention_period    How long to keep partitions around
--    i_partition_period    One of: year, month, day, hour
--
--  Returns:
--    Names of partitions dropped
-------------------------------------------------------------------------------
declare
    _schema text not null := lower (split_part (i_parent_table, '.', 1));
    _table  text not null := lower (split_part (i_parent_table, '.', 2));
    _part   text;
    _expr   text;
    _dfmt   text;
begin
    if i_partition_period in ('year', 'yearly') then
        _expr := '_[0-9]{4}';
        _dfmt := '_YYYY';
    elsif i_partition_period in ('month', 'monthly') then
        _expr := '_[0-9]{4}_[0-9]{2}';
        _dfmt := '_YYYY_MM';
    elsif i_partition_period in ('day', 'daily') then
        _expr := '_[0-9]{4}_[0-9]{2}_[0-9]{2}';
        _dfmt := '_YYYY_MM_DD';
    elsif i_partition_period in ('hour', 'hourly') then
        _expr := '_[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}';
        _dfmt := '_YYYY_MM_DD_HH24';
    else
        raise exception 'not supported i_partition_period: %', i_partition_period;
    end if;

    if length (_table) = 0 then
        _table := _schema;
        _schema := 'public';
    end if;

    for _part in
        select quote_ident (t.schemaname) ||'.'|| quote_ident (t.tablename)
          from pg_catalog.pg_tables t
         where t.schemaname = _schema
           and t.tablename ~ ('^'|| _table || _expr ||'$')
           and t.tablename < _table || to_char (now() - i_retention_period, _dfmt)
         order by 1
    loop
        execute 'drop table '|| _part;
        return next _part;
    end loop;
end;
$$ language plpgsql;


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
    _part text;
begin
    for _part in
        select londiste.list_obsolete_partitions (i_parent_table, i_retention_period, i_partition_period)
    loop
        execute 'drop table '|| _part;
        return next _part;
    end loop;
end;
$$ language plpgsql;

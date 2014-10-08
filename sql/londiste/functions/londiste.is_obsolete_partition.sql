
create or replace function londiste.is_obsolete_partition
(
    in i_partition_table text,
    in i_retention_period interval,
    in i_partition_period text
)
    returns boolean
as $$
-------------------------------------------------------------------------------
--  Function: londiste.is_obsolete_partition(3)
--
--    Test partition name of partition-by-date parent table.
--
--  Parameters:
--    i_partition_table     Partition table name we want to check
--    i_retention_period    How long to keep partitions around
--    i_partition_period    One of: year, month, day, hour
--
--  Returns:
--    True if partition is too old, false if it is not,
--    null if its name does not match expected pattern.
-------------------------------------------------------------------------------
declare
    _expr text;
    _dfmt text;
    _base text;
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

    _expr = '^(.+)' || _expr || '$';
    _base = substring (i_partition_table from _expr);

    if _base is null then
        return null;
    elsif i_partition_table < _base || to_char (now() - i_retention_period, _dfmt) then
        return true;
    else
        return false;
    end if;
end;
$$ language plpgsql;

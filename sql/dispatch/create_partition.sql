-- drop old function with timestamp
DROP FUNCTION IF EXISTS public.create_partition(
    text,
    text,
    text,
    text,
    timestamp,
    text
);

CREATE OR REPLACE FUNCTION public.create_partition(
    i_table text,
    i_part  text,
    i_pkeys text,
    i_part_field text,
    i_part_time timestamptz,
    i_part_period text
) RETURNS int
AS $$

------------------------------------------------------------------------
-- Function: public.create_partition
--
--      Creates child table for aggregation function for either monthly or daily if it does not exist yet.
--      Locks parent table for child table creating.
--
-- Parameters:
--      i_table - name of parent table
--      i_part - name of partition table to create
--      i_pkeys - primary key fields (comma separated, used to create constraint).
--      i_part_field - field used to partition table (when not partitioned by field, value is NULL)
--      i_part_time - partition time
--      i_part_period -  period of partitioned data, current possible values are 'hour', 'day', 'month' and 'year'
--
-- Example:
--      select public.create_partition('aggregate.user_call_monthly', 'aggregate.user_call_monthly_2010_01', 'key_user', 'period_start', '2010-01-10 11:00'::timestamptz, 'month');
--
------------------------------------------------------------------------
declare
    chk_start       text;
    chk_end         text;
    part_start      timestamptz;
    table_schema    text;
    table_name      text;
    part_schema     text;
    part_name       text;
    pos             int4;
    fq_table        text;
    fq_part         text;
begin
    -- parent table schema and name + quoted name
    pos := position('.' in i_table);
    if pos > 0 then
        table_schema := substring(i_table for pos - 1);
        table_name := substring(i_table from pos + 1);
    else
        table_schema := 'public';
        table_name := i_table;
    end if;
    fq_table := quote_ident(table_schema) || '.' || quote_ident(table_name);

    -- part table schema and name + quoted name
    pos := position('.' in i_part);
    if pos > 0 then
        part_schema := substring(i_part for pos - 1);
        part_name := substring(i_part from pos + 1);
    else
        part_schema := 'public';
        part_name := i_part;
    end if;
    fq_part := quote_ident(part_schema) || '.' || quote_ident(part_name);

    -- allow only single creation at a time, without affecting DML operations
    execute 'lock table ' || fq_table || ' in share update exclusive mode';

    -- check if part table exists
    perform 1 from pg_class t, pg_namespace s
        where t.relnamespace = s.oid
          and s.nspname = part_schema
          and t.relname = part_name;
    if found then
        return 0;
    else
        -- need to use 'like' to get indexes
        execute 'create table ' || fq_part
            || ' (like ' || fq_table || ' including indexes including constraints)'
            -- || ' () '
            || ' inherits (' || fq_table || ')';

        if i_part_field != '' then
            part_start := date_trunc(i_part_period, i_part_time);
            chk_start := to_char(part_start, 'YYYY-MM-DD HH24:MI:SS');
            chk_end := to_char(part_start + ('1 '||i_part_period)::interval,
			       'YYYY-MM-DD HH24:MI:SS');
            execute 'alter table '|| fq_part ||' add check(' || i_part_field || ' >= '''
            || chk_start ||''' and ' || i_part_field || ' < ''' || chk_end || ''')';
        end if;
    end if;
    return 1;
end;

$$
LANGUAGE plpgsql;

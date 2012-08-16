
create or replace function public.create_partition(
    i_table text,
    i_part  text,
    i_pkeys text,
    i_part_field text,
    i_part_time timestamptz,
    i_part_period text
) returns int as $$
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
    part_end        timestamptz;
    parent_schema   text;
    parent_name     text;
    part_schema     text;
    part_name       text;
    pos             int4;
    fq_table        text;
    fq_part         text;
    q_grantee       text;
    g               record;
    r               record;
    sql             text;
    pgver           integer;
begin
    if i_table is null or i_part is null then
        raise exception 'need table and part';
    end if;

    -- load postgres version (XYYZZ).
    show server_version_num into pgver;

    -- parent table schema and name + quoted name
    pos := position('.' in i_table);
    if pos > 0 then
        parent_schema := substring(i_table for pos - 1);
        parent_name := substring(i_table from pos + 1);
    else
        parent_schema := 'public';
        parent_name := i_table;
    end if;
    fq_table := quote_ident(parent_schema) || '.' || quote_ident(parent_name);

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
    end if;

    -- need to use 'like' to get indexes
    sql := 'create table ' || fq_part || ' (like ' || fq_table;
    if pgver >= 90000 then
        sql := sql || ' including all';
    else
        sql := sql || ' including indexes including constraints including defaults';
    end if;
    sql := sql || ') inherits (' || fq_table || ')';
    execute sql;

    -- extra check constraint
    if i_part_field != '' then
        part_start := date_trunc(i_part_period, i_part_time);
        part_end := part_start + ('1 ' || i_part_period)::interval;
        chk_start := quote_literal(to_char(part_start, 'YYYY-MM-DD HH24:MI:SS'));
        chk_end := quote_literal(to_char(part_end, 'YYYY-MM-DD HH24:MI:SS'));
        sql := 'alter table '|| fq_part || ' add check ('
            || quote_ident(i_part_field) || ' >= ' || chk_start || ' and '
            || quote_ident(i_part_field) || ' < ' || chk_end || ')';
        execute sql;
    end if;

    -- load grants from parent table
    for g in
        select grantor, grantee, privilege_type, is_grantable
            from information_schema.table_privileges
            where table_schema = parent_schema
                and table_name = parent_name
    loop
        if g.grantee = 'PUBLIC' then
            q_grantee = 'public';
        else
            q_grantee := quote_ident(g.grantee);
        end if;
        sql := 'grant ' || g.privilege_type || ' on ' || fq_part || ' to ' || q_grantee;
        if g.is_grantable = 'YES' then
            sql := sql || ' with grant option';
        end if;
        execute sql;
    end loop;

    return 1;
end;
$$ language plpgsql;

-- drop old function with timestamp
drop function if exists public.create_partition(text, text, text, text, timestamp, text);


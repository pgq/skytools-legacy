
create or replace function londiste.create_partition(
    i_table text,
    i_part  text,
    i_pkeys text,
    i_part_field text,
    i_part_time timestamptz,
    i_part_period text
) returns int as $$
------------------------------------------------------------------------
-- Function: londiste.create_partition
--
--      Creates inherited child table if it does not exist by copying parent table's structure.
--      Locks londiste.table_info table to avoid parallel creation of any partitions.
--
-- Elements that are copied over by "LIKE x INCLUDING ALL":
--      * Defaults
--      * Constraints
--      * Indexes
--      * Storage options (9.0+)
--      * Comments (9.0+)
--
-- Elements that are copied over manually because LIKE ALL does not support them:
--      * Grants
--      * Triggers
--      * Rules
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
--      select londiste.create_partition('aggregate.user_call_monthly', 'aggregate.user_call_monthly_2010_01', 'key_user', 'period_start', '2010-01-10 11:00'::timestamptz, 'month');
--
------------------------------------------------------------------------
declare
    chk_start       text;
    chk_end         text;
    part_start      timestamptz;
    part_end        timestamptz;
    parent_schema   text;
    parent_name     text;
    parent_oid      oid;
    part_schema     text;
    part_name       text;
    owner           name;
    pos             int4;
    fq_table        text;
    fq_part         text;
    q_grantee       text;
    g               record;
    r               record;
    tg              record;
    sql             text;
    pgver           integer;
    r_oldtbl        text;
    r_extra         text;
    r_sql           text;
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
    -- (changed from locking parent table to avoid deadlocks from concurrent workers)
    execute 'lock table londiste.table_info in share update exclusive mode';
    parent_oid := fq_table::regclass::oid;

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

    -- find out parent table owner
    select o.rolname into owner
      from pg_class t, pg_namespace s, pg_roles o
     where t.relnamespace = s.oid
       and s.nspname = parent_schema
       and t.relname = parent_name
       and t.relowner = o.oid;

    -- set proper part table ownership
    if owner != user then
        sql = 'alter table ' || fq_part || ' owner to ' || quote_ident(owner);
        execute sql;
    end if;

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

    -- generate triggers info query
    sql := 'SELECT tgname, tgenabled,'
        || '   pg_catalog.pg_get_triggerdef(oid) as tgdef'
        || ' FROM pg_catalog.pg_trigger '
        || ' WHERE tgrelid = ' || parent_oid::text
        || ' AND ';
    if pgver >= 90000 then
        sql := sql || ' NOT tgisinternal';
    else
        sql := sql || ' NOT tgisconstraint';
    end if;

    -- copy triggers
    for tg in execute sql
    loop
        sql := regexp_replace(tg.tgdef, E' ON ([[:alnum:]_.]+|"([^"]|"")+")+ ', ' ON ' || fq_part || ' ');
        if sql = tg.tgdef then
            raise exception 'Failed to reconstruct the trigger: %', sql;
        end if;
        execute sql;
        if tg.tgenabled = 'O' then
            -- standard mode
            r_extra := NULL;
        elsif tg.tgenabled = 'D' then
            r_extra := ' DISABLE TRIGGER ';
        elsif tg.tgenabled = 'A' then
            r_extra := ' ENABLE ALWAYS TRIGGER ';
        elsif tg.tgenabled = 'R' then
            r_extra := ' ENABLE REPLICA TRIGGER ';
        else
            raise exception 'Unknown trigger mode: %', tg.tgenabled;
        end if;
        if r_extra is not null then
            sql := 'ALTER TABLE ' || fq_part || r_extra || quote_ident(tg.tgname);
            execute sql;
        end if;
    end loop;

    -- copy rules
    for r in
        select rw.rulename, rw.ev_enabled, pg_catalog.pg_get_ruledef(rw.oid) as definition
          from pg_catalog.pg_rewrite rw
         where rw.ev_class = parent_oid
           and rw.rulename <> '_RETURN'::name
    loop
        -- try to skip rule name
        r_extra := 'CREATE RULE ' || quote_ident(r.rulename) || ' AS';
        r_sql := substr(r.definition, 1, char_length(r_extra));
        if r_sql = r_extra then
            r_sql := substr(r.definition, char_length(r_extra));
        else
            raise exception 'failed to match rule name';
        end if;

        -- no clue what name was used in defn, so find it from sql
        r_oldtbl := substring(r_sql from ' TO (([[:alnum:]_.]+|"([^"]+|"")+")+)[[:space:]]');
        if char_length(r_oldtbl) > 0 then
            sql := replace(r.definition, r_oldtbl, fq_part);
        else
            raise exception 'failed to find original table name';
        end if;
        execute sql;

        -- rule flags
        r_extra := NULL;
        if r.ev_enabled = 'R' then
            r_extra = ' ENABLE REPLICA RULE ';
        elsif r.ev_enabled = 'A' then
            r_extra = ' ENABLE ALWAYS RULE ';
        elsif r.ev_enabled = 'D' then
            r_extra = ' DISABLE RULE ';
        elsif r.ev_enabled <> 'O' then
            raise exception 'unknown rule option: %', r.ev_enabled;
        end if;
        if r_extra is not null then
            sql := 'ALTER TABLE ' || fq_part || r_extra
                || quote_ident(r.rulename);
            execute sql;
        end if;
    end loop;

    return 1;
end;
$$ language plpgsql;

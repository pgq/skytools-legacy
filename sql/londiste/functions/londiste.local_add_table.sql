create or replace function londiste.local_add_table(
    in i_queue_name     text,
    in i_table_name     text,
    in i_trg_args       text[],
    out ret_code        int4,
    out ret_note        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_add_table(3)
--
--      Register table on Londiste node, with customizable trigger args.
--
-- Parameters:
--      i_queue_name - queue name
--      i_table_name - table name
--      i_trg_args   - args to trigger, or magic parameters.
--
-- Trigger args:
--      See documentation for pgq triggers.
--
-- Magic parameters:
--      no_triggers     - skip trigger creation
--      skip_truncate   - set 'skip_truncate' table attribute
--      expect_sync     - set table state to 'ok'
--      tgflags=X       - trigger creation flags
--
-- Trigger creation flags (default: AIUDL):
--      I - ON INSERT
--      U - ON UPDATE
--      D - ON DELETE
--      Q - use pgq.sqltriga() as trigger function
--      L - use pgq.logutriga() as trigger function
--      B - BEFORE
--      A - AFTER
--
-- Example:
--      > londiste.local_add_table('q', 'tbl', array['tgflags=BI', 'SKIP', 'pkey=col1,col2'])
--
-- Returns:
--      200 - Ok
--      301 - Warning, trigger exists that will fire before londiste one
--      400 - No such set
-- ----------------------------------------------------------------------
declare
    col_types text;
    fq_table_name text;
    new_state text;

    trunctrg_name text;
    pgversion int;
    logtrg_previous text;
    lg_name text;
    lg_func text;
    lg_pos text;
    lg_event text;
    lg_args text;
    tbl record;
    i integer;
    sql text;
    arg text;
    _node record;
    _tbloid oid;
    _extra_args text;
    _skip_prefix text := 'zzzkip';
    _skip_trg_count integer;
    _skip_trg_name text;
begin
    _extra_args := '';
    fq_table_name := londiste.make_fqname(i_table_name);
    _tbloid := londiste.find_table_oid(fq_table_name);
    if _tbloid is null then
        select 404, 'Table does not exist: ' || fq_table_name into ret_code, ret_note;
        return;
    end if;
    col_types := londiste.find_column_types(fq_table_name);
    if position('k' in col_types) < 1 then
        -- allow missing primary key in case of combined table where
        -- pkey was removed by londiste
        perform 1 from londiste.table_info t,
            pgq_node.node_info n_this,
            pgq_node.node_info n_other
          where n_this.queue_name = i_queue_name
            and n_other.combined_queue = n_this.combined_queue
            and n_other.queue_name <> n_this.queue_name
            and t.queue_name = n_other.queue_name
            and t.table_name = fq_table_name
            and t.dropped_ddl is not null;
        if not found then
            select 400, 'Primary key missing on table: ' || fq_table_name into ret_code, ret_note;
            return;
        end if;
    end if;

    select * from pgq_node.get_node_info(i_queue_name) into _node;
    if not found or _node.ret_code >= 400 then
        select 400, 'No such set: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;

    select merge_state, local into tbl
        from londiste.table_info
        where queue_name = i_queue_name and table_name = fq_table_name;
    if not found then
        -- add to set on root
        if _node.node_type = 'root' then
            select f.ret_code, f.ret_note into ret_code, ret_note
                from londiste.global_add_table(i_queue_name, i_table_name) f;
            if ret_code <> 200 then
                return;
            end if;
        else
            select 404, 'Table not available on queue: ' || fq_table_name
                into ret_code, ret_note;
            return;
        end if;

        -- reload info
        select merge_state, local into tbl
            from londiste.table_info
            where queue_name = i_queue_name and table_name = fq_table_name;
    end if;

    if tbl.local then
        select 200, 'Table already added: ' || fq_table_name into ret_code, ret_note;
        return;
    end if;

    if _node.node_type = 'root' then
        new_state := 'ok';
        perform londiste.root_notify_change(i_queue_name, 'londiste.add-table', fq_table_name);
    elsif _node.node_type = 'leaf' and _node.combined_type = 'branch' then
        new_state := 'ok';
    elsif 'expect_sync' = any (i_trg_args) then
        new_state := 'ok';
    else
        new_state := NULL;
    end if;

    update londiste.table_info
        set local = true,
            merge_state = new_state
        where queue_name = i_queue_name and table_name = fq_table_name;
    if not found then
        raise exception 'lost table: %', fq_table_name;
    end if;

    -- skip triggers on leaf node
    if _node.node_type = 'leaf' then
        -- on weird leafs the trigger funcs may not exist
        perform 1 from pg_proc p join pg_namespace n on (n.oid = p.pronamespace)
            where n.nspname = 'pgq' and p.proname in ('logutriga', 'sqltriga');
        if not found then
            select 200, 'Table added with no triggers: ' || fq_table_name into ret_code, ret_note;
            return;
        end if;
        -- on regular leaf, install deny trigger
        _extra_args := ', ' || quote_literal('deny');
    end if;

    -- create Ins/Upd/Del trigger if it does not exists already
    lg_name := '_londiste_' || i_queue_name;
    perform 1 from pg_catalog.pg_trigger
        where tgrelid = londiste.find_table_oid(fq_table_name)
            and tgname = lg_name;
    if not found then
        -- new trigger
        lg_func := 'pgq.logutriga';
        lg_event := '';
        lg_args := quote_literal(i_queue_name);
        lg_pos := 'after';

        -- parse extra args
        if array_lower(i_trg_args, 1) is not null then
            for i in array_lower(i_trg_args, 1) .. array_upper(i_trg_args, 1) loop
                arg := i_trg_args[i];
                if arg like 'tgflags=%' then
                    -- special flag handling
                    arg := upper(substr(arg, 9));
                    if position('B' in arg) > 0 then
                        lg_pos := 'before';
                    end if;
                    if position('A' in arg) > 0 then
                        lg_pos := 'after';
                    end if;
                    if position('Q' in arg) > 0 then
                        lg_func := 'pgq.sqltriga';
                    end if;
                    if position('L' in arg) > 0 then
                        lg_func := 'pgq.logutriga';
                    end if;
                    if position('I' in arg) > 0 then
                        lg_event := lg_event || ' or insert';
                    end if;
                    if position('U' in arg) > 0 then
                        lg_event := lg_event || ' or update';
                    end if;
                    if position('D' in arg) > 0 then
                        lg_event := lg_event || ' or delete';
                    end if;
                    if position('S' in arg) > 0 then
                        -- get count and name of existing skip triggers
                        select count(*), min(t.tgname)
                        into _skip_trg_count, _skip_trg_name
                        from pg_catalog.pg_trigger t
                        where t.tgrelid = londiste.find_table_oid(fq_table_name)
                            and position(E'\\000skip\\000' in lower(tgargs::text)) > 0;
                        -- if no previous skip triggers, prefix name and add SKIP to args
                        if _skip_trg_count = 0 then
                            lg_name := _skip_prefix || lg_name;
                            lg_args := lg_args || ', ' || quote_literal('SKIP');
                        -- if one previous skip trigger, check it's prefix and
                        -- do not use SKIP on current trigger
                        elsif _skip_trg_count = 1 then
                            -- if not prefixed then rename
                            if position(_skip_prefix in _skip_trg_name) != 1 then
                                sql := 'alter trigger ' || _skip_trg_name
                                    || ' on ' || londiste.quote_fqname(fq_table_name)
                                    || ' rename to ' || _skip_prefix || _skip_trg_name;
                                execute sql;
                            end if;
                        else
                            select 405, 'Multiple SKIP triggers in table: ' || fq_table_name
                            into ret_code, ret_note;
                            return;
                        end if;
                    end if;
                elsif arg = 'expect_sync' then
                    -- already handled
                elsif arg = 'skip_truncate' then
                    perform 1 from londiste.local_set_table_attrs(i_queue_name, fq_table_name, 'skip_truncate=1');
                elsif arg = 'no_triggers' then
                    select 200, 'Table added with no triggers: ' || fq_table_name into ret_code, ret_note;
                    return;
                else
                    -- ordinary arg
                    lg_args := lg_args || ', ' || quote_literal(arg);
                end if;
            end loop;
        end if;

        -- finalize event
        lg_event := substr(lg_event, 4);
        if lg_event = '' then
            lg_event := 'insert or update or delete';
        end if;

        -- create trigger
        sql := 'create trigger ' || quote_ident(lg_name)
            || ' ' || lg_pos || ' ' || lg_event
            || ' on ' || londiste.quote_fqname(fq_table_name)
            || ' for each row execute procedure '
            || lg_func || '(' || lg_args || _extra_args || ')';
        execute sql;
    end if;

    -- create tRuncate trigger if it does not exists already
    show server_version_num into pgversion;
    if pgversion >= 80400 then
        trunctrg_name  := '_londiste_' || i_queue_name || '_truncate';
        perform 1 from pg_catalog.pg_trigger
          where tgrelid = londiste.find_table_oid(fq_table_name)
            and tgname = trunctrg_name;
        if not found then
            sql := 'create trigger ' || quote_ident(trunctrg_name)
                || ' after truncate on ' || londiste.quote_fqname(fq_table_name)
                || ' for each statement execute procedure pgq.sqltriga(' || quote_literal(i_queue_name)
                || _extra_args || ')';
            execute sql;
        end if;
    end if;

    -- Check that no trigger exists on the target table that will get fired
    -- before londiste one (this could have londiste replicate data
    -- out-of-order
    --
    -- Don't report all the trigger names, 8.3 does not have array_accum
    -- available

    if pgversion >= 90000 then
        select tg.tgname into logtrg_previous
        from pg_class r join pg_trigger tg on (tg.tgrelid = r.oid)
        where r.oid = londiste.find_table_oid(fq_table_name)
          and not tg.tgisinternal
          and tg.tgname < lg_name::name
          -- per-row AFTER trigger
          and (tg.tgtype & 3) = 1   -- bits: 0:ROW, 1:BEFORE
          -- current londiste
          and not londiste.is_replica_func(tg.tgfoid)
          -- old londiste
          and substring(tg.tgname from 1 for 10) != '_londiste_'
          and substring(tg.tgname from char_length(tg.tgname) - 6) != '_logger'
        order by 1 limit 1;
    else
        select tg.tgname into logtrg_previous
        from pg_class r join pg_trigger tg on (tg.tgrelid = r.oid)
        where r.oid = londiste.find_table_oid(fq_table_name)
          and not tg.tgisconstraint
          and tg.tgname < lg_name::name
          -- per-row AFTER trigger
          and (tg.tgtype & 3) = 1   -- bits: 0:ROW, 1:BEFORE
          -- current londiste
          and not londiste.is_replica_func(tg.tgfoid)
          -- old londiste
          and substring(tg.tgname from 1 for 10) != '_londiste_'
          and substring(tg.tgname from char_length(tg.tgname) - 6) != '_logger'
        order by 1 limit 1;
    end if;

    if logtrg_previous is not null then
       select 301,
              'Table added: ' || fq_table_name
                              || ', but londiste trigger is not first: '
                              || logtrg_previous
         into ret_code, ret_note;
        return;
    end if;

    select 200, 'Table added: ' || fq_table_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql;

create or replace function londiste.local_add_table(
    in i_queue_name     text,
    in i_table_name     text,
    out ret_code        int4,
    out ret_note        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_add_table(2)
--
--      Register table on Londiste node.
--
-- Returns:
--      200 - Ok
--      301 - Warning, trigger exists that will fire before londiste one
--      400 - No such set
-- ----------------------------------------------------------------------
begin
    select f.ret_code, f.ret_note into ret_code, ret_note
      from londiste.local_add_table(i_queue_name, i_table_name, null) f;
    return;
end;
$$ language plpgsql strict;



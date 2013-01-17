create or replace function londiste.create_trigger(
    in i_queue_name     text,
    in i_table_name     text,
    in i_trg_args       text[],
    in i_dest_table     text,
    in i_node_type      text,
    out ret_code        int4,
    out ret_note        text,
    out trigger_name    text)
as $$
------------------------------------------------------------------------
-- Function: londiste.create_trigger(5)
--
--     Create or replace londiste trigger(s)
--
-- Parameters:
--      i_queue_name - queue name
--      i_table_name - table name
--      i_trg_args   - args to trigger
--      i_dest_table - actual name of destination table (NULL if same as src)
--      i_node_type  - l3 node type
--
-- Trigger args:
--      See documentation for pgq triggers.
--
-- Trigger creation flags (default: AIUDL):
--      I - ON INSERT
--      U - ON UPDATE
--      D - ON DELETE
--      Q - use pgq.sqltriga() as trigger function
--      L - use pgq.logutriga() as trigger function
--      B - BEFORE
--      A - AFTER
--      S - SKIP
--
-- Returns:
--      200 - Ok
--      201 - Trigger not created
--      405 - Multiple SKIP triggers
--
------------------------------------------------------------------------
declare
    trigger_name text;
    lg_func text;
    lg_pos text;
    lg_event text;
    lg_args text[];
    _old_tgargs bytea;
    _new_tgargs bytea;
    trunctrg_name text;
    pgversion int;
    sql text;
    arg text;
    i integer;
    _extra_args text[] := '{}';
    -- skip trigger
    _skip_prefix text := 'zzz_';
    _skip_trg_count integer;
    _skip_trg_name text;
    -- given tgflags array
    _tgflags char[];
    -- ordinary argument array
    _args text[];
    -- array with all tgflags values
    _check_flags char[] := array['B','A','Q','L','I','U','D','S'];
    -- argument flags
    _skip boolean := false;
    _no_triggers boolean := false;
    _got_extra1 boolean := false;
begin
    -- parse trigger args
    if array_lower(i_trg_args, 1) is not null then
        for i in array_lower(i_trg_args, 1) .. array_upper(i_trg_args, 1) loop
            arg := i_trg_args[i];
            if arg like 'tgflags=%' then
                -- special flag handling
                arg := upper(substr(arg, 9));
                for j in array_lower(_check_flags, 1) .. array_upper(_check_flags, 1) loop
                    if position(_check_flags[j] in arg) > 0 then
                        _tgflags := array_append(_tgflags, _check_flags[j]);
                    end if;
                end loop;
            elsif arg = 'no_triggers' then
                _no_triggers := true;
            elsif lower(arg) = 'skip' then
                _skip := true;
            elsif arg = 'virtual_table' then
                _no_triggers := true;   -- do not create triggers
            elsif arg not in ('expect_sync', 'skip_truncate', 'merge_all', 'no_merge') then -- ignore add-table args
                if arg like 'ev_extra1=%' then
                    _got_extra1 := true;
                end if;
                -- ordinary arg
                _args = array_append(_args, quote_literal(arg));
            end if;
        end loop;
    end if;

    if i_dest_table <> i_table_name and not _got_extra1 then
        -- if renamed table, enforce trigger to put
        -- global table name into extra1
        arg := 'ev_extra1=' || quote_literal(i_table_name);
        _args := array_append(_args, quote_literal(arg));
    end if;

    trigger_name := '_londiste_' || i_queue_name;
    lg_func := 'pgq.logutriga';
    lg_event := '';
    lg_args := array[i_queue_name];
    lg_pos := 'after';

    if array_lower(_args, 1) is not null then
        lg_args := lg_args || _args;
    end if;

    if 'B' = any(_tgflags) then
        lg_pos := 'before';
    end if;
    if 'A' = any(_tgflags)  then
        lg_pos := 'after';
    end if;
    if 'Q' = any(_tgflags) then
        lg_func := 'pgq.sqltriga';
    end if;
    if 'L' = any(_tgflags) then
        lg_func := 'pgq.logutriga';
    end if;
    if 'I' = any(_tgflags) then
        lg_event := lg_event || ' or insert';
    end if;
    if 'U' = any(_tgflags) then
        lg_event := lg_event || ' or update';
    end if;
    if 'D' = any(_tgflags) then
        lg_event := lg_event || ' or delete';
    end if;
    if 'S' = any(_tgflags) then
        _skip := true;
    end if;

    if i_node_type = 'leaf' then
        -- on weird leafs the trigger funcs may not exist
        perform 1 from pg_proc p join pg_namespace n on (n.oid = p.pronamespace)
            where n.nspname = 'pgq' and p.proname in ('logutriga', 'sqltriga');
        if not found then
            select 201, 'Trigger not created' into ret_code, ret_note;
            return;
        end if;
        -- on regular leaf, install deny trigger
        _extra_args := array_append(_extra_args, quote_literal('deny'));
    end if;

    -- if skip param given, rename previous skip triggers and prefix current
    if _skip then
        -- get count and name of existing skip triggers
        select count(*), min(t.tgname)
        into _skip_trg_count, _skip_trg_name
        from pg_catalog.pg_trigger t
        where t.tgrelid = londiste.find_table_oid(i_dest_table)
            and position(E'\\000skip\\000' in lower(tgargs::text)) > 0;
        -- if no previous skip triggers, prefix name and add SKIP to args
        if _skip_trg_count = 0 then
            trigger_name := _skip_prefix || trigger_name;
            lg_args := array_append(lg_args, quote_literal('SKIP'));
        -- if one previous skip trigger, check it's prefix and
        -- do not use SKIP on current trigger
        elsif _skip_trg_count = 1 then
            -- if not prefixed then rename
            if position(_skip_prefix in _skip_trg_name) != 1 then
                sql := 'alter trigger ' || _skip_trg_name
                    || ' on ' || londiste.quote_fqname(i_dest_table)
                    || ' rename to ' || _skip_prefix || _skip_trg_name;
                execute sql;
            end if;
        else
            select 405, 'Multiple SKIP triggers'
            into ret_code, ret_note;
            return;
        end if;
    end if;

    -- create Ins/Upd/Del trigger if it does not exists already
    select t.tgargs
        from pg_catalog.pg_trigger t
        where t.tgrelid = londiste.find_table_oid(i_dest_table)
            and t.tgname = trigger_name
        into _old_tgargs;

    if found then
        _new_tgargs := lg_args[1];
        for i in 2 .. array_upper(lg_args, 1) loop
            _new_tgargs := _new_tgargs || E'\\000'::bytea || decode(lg_args[i], 'escape');
        end loop;

        if _old_tgargs is distinct from _new_tgargs then
            sql := 'drop trigger if exists ' || quote_ident(trigger_name)
                || ' on ' || londiste.quote_fqname(i_dest_table);
            execute sql;
        end if;
    end if;

    if not found or _old_tgargs is distinct from _new_tgargs then
        if _no_triggers then
            select 201, 'Trigger not created'
            into ret_code, ret_note;
            return;
        end if;

        -- finalize event
        lg_event := substr(lg_event, 4); -- remove ' or '
        if lg_event = '' then
            lg_event := 'insert or update or delete';
        end if;

        -- create trigger
        lg_args := lg_args || _extra_args;
        sql := 'create trigger ' || quote_ident(trigger_name)
            || ' ' || lg_pos || ' ' || lg_event
            || ' on ' || londiste.quote_fqname(i_dest_table)
            || ' for each row execute procedure '
            || lg_func || '(' || array_to_string(lg_args, ', ') || ')';
        execute sql;
    end if;

    -- create truncate trigger if it does not exists already
    show server_version_num into pgversion;
    if pgversion >= 80400 then
        trunctrg_name  := '_londiste_' || i_queue_name || '_truncate';
        perform 1 from pg_catalog.pg_trigger
          where tgrelid = londiste.find_table_oid(i_dest_table)
            and tgname = trunctrg_name;
        if not found then
            _extra_args := i_queue_name || _extra_args;
            sql := 'create trigger ' || quote_ident(trunctrg_name)
                || ' after truncate on ' || londiste.quote_fqname(i_dest_table)
                || ' for each statement execute procedure pgq.sqltriga('
                || array_to_string(_extra_args, ', ') || ')';
            execute sql;
        end if;
    end if;

    select 200, 'OK'
    into ret_code, ret_note;
    return;
end;
$$ language plpgsql;

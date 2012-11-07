
drop function if exists londiste.find_table_fkeys(text);



-- ----------------------------------------------------------------------
-- Section: Londiste internals
--
--      Londiste storage: tables/seqs/fkeys/triggers/events.
--
-- Londiste event types:
--      I/U/D                   - partial SQL event from pgq.sqltriga()
--      I:/U:/D: <pk>           - urlencoded event from pgq.logutriga()
--      EXECUTE                 - SQL script execution
--      TRUNCATE                - table truncation
--      londiste.add-table      - global table addition
--      londiste.remove-table   - global table removal
--      londiste.update-seq     - sequence update
--      londiste.remove-seq     - global sequence removal
--
-- pgq.sqltriga() event:
--      ev_type     - I/U/D which means insert/update/delete
--      ev_data     - partial SQL
--      ev_extra1   - table name
--
--      Insert: ev_type = "I", ev_data = "(col1, col2) values (2, 'foo')", ev_extra1 = "public.tblname"
--
--      Update: ev_type = "U", ev_data = "col2 = null where col1 = 2", ev_extra1 = "public.tblname"
--
--      Delete: ev_type = "D", ev_data = "col1 = 2", ev_extra1 = "public.tblname"
--
-- pgq.logutriga() event:
--      ev_type     - I:/U:/D: plus comma separated list of pkey columns
--      ev_data     - urlencoded row columns
--      ev_extra1   - table name
--
--      Insert: ev_type = "I:col1", ev_data = ""
--
-- Truncate trigger event:
--      ev_type     - TRUNCATE
--      ev_extra1   - table name
--
-- Execute SQL event:
--      ev_type     - EXECUTE
--      ev_data     - SQL script
--      ev_extra1   - Script ID
--
-- Global table addition:
--      ev_type     - londiste.add-table
--      ev_data     - table name
--
-- Global table removal:
--      ev_type     - londiste.remove-table
--      ev_data     - table name
--
-- Global sequence update:
--      ev_type     - londiste.update-seq
--      ev_data     - seq value
--      ev_extra1   - seq name
--5)
-- Global sequence removal:
--      ev_type     - londiste.remove-seq
--      ev_data     - seq name
-- ----------------------------------------------------------------------

set default_with_oids = 'off';


-- ----------------------------------------------------------------------
-- Table: londiste.table_info
--
--      Info about registered tables.
--
-- Columns:
--      nr              - number for visual ordering
--      queue_name      - Cascaded queue name
--      table_name      - fully-qualified table name
--      local           - Is used locally
--      merge_state     - State for tables
--      custom_snapshot - remote snapshot for COPY command
--      dropped_ddl     - temp place to store ddl
--      table_attrs     - urlencoded dict of extra attributes
--
-- Tables merge states:
--      NULL            - copy has not yet happened
--      in-copy         - ongoing bulk copy
--      catching-up     - copy process applies events that happened during copy
--      wanna-sync:%    - copy process caught up, wants to hand table over to replay
--      do-sync:%       - replay process is ready to accept the table
--      ok              - in sync, replay applies events
-- ----------------------------------------------------------------------
create table londiste.table_info (
    nr                  serial not null,
    queue_name          text not null,
    table_name          text not null,
    local               boolean not null default false,
    merge_state         text,
    custom_snapshot     text,
    dropped_ddl         text,
    table_attrs         text,
    dest_table          text,

    primary key (queue_name, table_name),
    foreign key (queue_name)
      references pgq_node.node_info (queue_name)
      on delete cascade,
    check (dropped_ddl is null or merge_state in ('in-copy', 'catching-up'))
);


-- ----------------------------------------------------------------------
-- Table: londiste.seq_info
--
--      Sequences available on this queue.
--
-- Columns:
--      nr          - number for visual ordering
--      queue_name  - cascaded queue name
--      seq_name    - fully-qualified seq name
--      local       - there is actual seq on local node
--      last_value  - last published value from root
-- ----------------------------------------------------------------------
create table londiste.seq_info (
    nr                  serial not null,
    queue_name          text not null,
    seq_name            text not null,
    local               boolean not null default false,
    last_value          int8 not null,

    primary key (queue_name, seq_name),
    foreign key (queue_name)
      references pgq_node.node_info (queue_name)
      on delete cascade
);


-- ----------------------------------------------------------------------
-- Table: londiste.applied_execute
--
--      Info about EXECUTE commands that are ran.
--
-- Columns:
--      queue_name      - cascaded queue name
--      execute_file    - filename / unique id
--      execute_time    - the time execute happened
--      execute_sql     - contains SQL for EXECUTE event (informative)
-- ----------------------------------------------------------------------
create table londiste.applied_execute (
    queue_name          text not null,
    execute_file        text not null,
    execute_time        timestamptz not null default now(),
    execute_sql         text not null,
    execute_attrs       text,
    primary key (execute_file)
);


-- ----------------------------------------------------------------------
-- Table: londiste.pending_fkeys
--
--      Details on dropped fkeys.  Global, not specific to any set.
--
-- Columns:
--      from_table      - fully-qualified table name
--      to_table        - fully-qualified table name
--      fkey_name       - name of constraint
--      fkey_def        - full fkey definition
-- ----------------------------------------------------------------------
create table londiste.pending_fkeys (
    from_table          text not null,
    to_table            text not null,
    fkey_name           text not null,
    fkey_def            text not null,
    
    primary key (from_table, fkey_name)
);




-- Section: Londiste functions

-- upgrade schema


create or replace function londiste.upgrade_schema()
returns int4 as $$
-- updates table structure if necessary
declare
    cnt int4 = 0;
begin

    -- table_info: check (dropped_ddl is null or merge_state in ('in-copy', 'catching-up'))
    perform 1 from information_schema.check_constraints
      where constraint_schema = 'londiste'
        and constraint_name = 'table_info_check'
        and position('in-copy' in check_clause) > 0
        and position('catching' in check_clause) = 0;
    if found then
        alter table londiste.table_info drop constraint table_info_check;
        alter table londiste.table_info add constraint table_info_check
            check (dropped_ddl is null or merge_state in ('in-copy', 'catching-up'));
        cnt := cnt + 1;
    end if;

    -- table_info.dest_table
    perform 1 from information_schema.columns
      where table_schema = 'londiste'
        and table_name = 'table_info'
        and column_name = 'dest_table';
    if not found then
        alter table londiste.table_info add column dest_table text;
    end if;

    -- applied_execute.dest_table
    perform 1 from information_schema.columns
      where table_schema = 'londiste'
        and table_name = 'applied_execute'
        and column_name = 'execute_attrs';
    if not found then
        alter table londiste.applied_execute add column execute_attrs text;
    end if;

    -- applied_execute: drop queue_name from primary key
    perform 1 from pg_catalog.pg_indexes
      where schemaname = 'londiste'
        and tablename = 'applied_execute'
        and indexname = 'applied_execute_pkey'
        and indexdef like '%queue_name%';
    if found then
        alter table londiste.applied_execute
            drop constraint applied_execute_pkey;
        alter table londiste.applied_execute
            add constraint applied_execute_pkey
            primary key (execute_file);
    end if;

    -- applied_execute: drop fkey to pgq_node
    perform 1 from information_schema.table_constraints
      where constraint_schema = 'londiste'
        and table_schema = 'londiste'
        and table_name = 'applied_execute'
        and constraint_type = 'FOREIGN KEY'
        and constraint_name = 'applied_execute_queue_name_fkey';
    if found then
        alter table londiste.applied_execute
            drop constraint applied_execute_queue_name_fkey;
    end if;

    -- create roles
    perform 1 from pg_catalog.pg_roles where rolname = 'londiste_writer';
    if not found then
        create role londiste_writer in role pgq_admin;
        cnt := cnt + 1;
    end if;
    perform 1 from pg_catalog.pg_roles where rolname = 'londiste_reader';
    if not found then
        create role londiste_reader in role pgq_reader;
        cnt := cnt + 1;
    end if;

    return cnt;
end;
$$ language plpgsql;


select londiste.upgrade_schema();

-- Group: Information


create or replace function londiste.get_seq_list(
    in i_queue_name text,
    out seq_name text,
    out last_value int8,
    out local boolean)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: londiste.get_seq_list(1)
--
--      Returns registered seqs on this Londiste node.
--
-- Result fiels:
--      seq_name    - fully qualified name of sequence
--      last_value  - last globally published value
--      local       - is locally registered
-- ----------------------------------------------------------------------
declare
    rec record;
begin
    for seq_name, last_value, local in
        select s.seq_name, s.last_value, s.local from londiste.seq_info s
            where s.queue_name = i_queue_name
            order by s.nr, s.seq_name
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql strict;




drop function if exists londiste.get_table_list(text);

create or replace function londiste.get_table_list(
    in i_queue_name text,
    out table_name text,
    out local boolean,
    out merge_state text,
    out custom_snapshot text,
    out table_attrs text,
    out dropped_ddl text,
    out copy_role text,
    out copy_pos int4,
    out dest_table text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: londiste.get_table_list(1)
--
--      Return info about registered tables.
--
-- Parameters:
--      i_queue_name - cascaded queue name
--
-- Returns:
--      table_name      - fully-quelified table name
--      local           - does events needs to be applied to local table
--      merge_state     - show phase of initial copy
--      custom_snapshot - remote snapshot of COPY transaction
--      table_attrs     - urlencoded dict of table attributes
--      dropped_ddl     - partition combining: temp place to put DDL
--      copy_role       - partition combining: how to handle copy
--      copy_pos        - position in parallel copy working order
--
-- copy_role = lead:
--      on copy start, drop indexes and store in dropped_ddl
--      on copy finish change state to catching-up, then wait until copy_role turns to NULL
--      catching-up: if dropped_ddl not NULL, restore them
-- copy_role = wait-copy:
--      on copy start wait, until role changes (to wait-replay)
-- copy_role = wait-replay:
--      on copy finish, tag as 'catching-up'
--      wait until copy_role is NULL, then proceed
-- ----------------------------------------------------------------------
begin
    for table_name, local, merge_state, custom_snapshot, table_attrs,
        dropped_ddl, dest_table
    in
        select t.table_name, t.local, t.merge_state, t.custom_snapshot, t.table_attrs,
               t.dropped_ddl, t.dest_table
            from londiste.table_info t
            where t.queue_name = i_queue_name
            order by t.nr, t.table_name
    loop
        copy_role := null;
        copy_pos := 0;

        if merge_state in ('in-copy', 'catching-up') then
            select f.copy_role, f.copy_pos
                from londiste._coordinate_copy(i_queue_name, table_name) f
                into copy_role, copy_pos;
        end if;

        return next;
    end loop;
    return;
end;
$$ language plpgsql strict stable;


create or replace function londiste._coordinate_copy(
    in i_queue_name text, in i_table_name text,
    out copy_role text, out copy_pos int4)
as $$
-- if the table is in middle of copy from multiple partitions,
-- the copy processes need coordination.
declare
    q_part1     text;
    q_part_ddl  text;
    n_parts     int4;
    n_done      int4;
    _table_name text;
    n_combined_queue text;
    merge_state text;
    dest_table  text;
    dropped_ddl text;
begin
    copy_pos := 0;
    copy_role := null;

    select t.merge_state, t.dest_table, t.dropped_ddl,
           min(case when t2.local then t2.queue_name else null end) as _queue1,
           min(case when t2.local and t2.dropped_ddl is not null then t2.queue_name else null end) as _queue1ddl,
           count(case when t2.local then t2.table_name else null end) as _total,
           count(case when t2.local then nullif(t2.merge_state, 'in-copy') else null end) as _done,
           min(n.combined_queue) as _combined_queue,
           count(nullif(t2.queue_name < i_queue_name and t.merge_state = 'in-copy' and t2.merge_state = 'in-copy', false)) as _copy_pos
        from londiste.table_info t
        join pgq_node.node_info n on (n.queue_name = t.queue_name)
        left join pgq_node.node_info n2 on (n2.combined_queue = n.combined_queue or
            (n2.combined_queue is null and n.combined_queue is null))
        left join londiste.table_info t2 on
           (coalesce(t2.dest_table, t2.table_name) = coalesce(t.dest_table, t.table_name) and
            t2.queue_name = n2.queue_name and
            (t2.merge_state is null or t2.merge_state != 'ok'))
        where t.queue_name = i_queue_name and t.table_name = i_table_name
        group by t.nr, t.table_name, t.local, t.merge_state, t.custom_snapshot, t.table_attrs, t.dropped_ddl, t.dest_table
        into merge_state, dest_table, dropped_ddl, q_part1, q_part_ddl, n_parts, n_done, n_combined_queue, copy_pos;

    -- q_part1, q_part_ddl, n_parts, n_done, n_combined_queue, copy_pos, dest_table

    -- be more robust against late joiners
    q_part1 := coalesce(q_part_ddl, q_part1);

    -- turn the logic off if no merge is happening
    if n_parts = 1 then
        q_part1 := null;
    end if;

    if q_part1 is not null then
        if i_queue_name = q_part1 then
            -- lead
            if merge_state = 'in-copy' then
                if dropped_ddl is null and n_done > 0 then
                    -- seems late addition, let it copy with indexes
                    copy_role := 'wait-replay';
                elsif n_done < n_parts then
                    -- show copy_role only if need to drop ddl or already did drop ddl
                    copy_role := 'lead';
                end if;

                -- make sure it cannot be made to wait
                copy_pos := 0;
            end if;
            if merge_state = 'catching-up' and dropped_ddl is not null then
                -- show copy_role only if need to wait for others
                if n_done < n_parts then
                    copy_role := 'wait-replay';
                end if;
            end if;
        else
            -- follow
            if merge_state = 'in-copy' then
                if q_part_ddl is not null then
                    -- can copy, wait in replay until lead has applied ddl
                    copy_role := 'wait-replay';
                elsif n_done > 0 then
                    -- ddl is not dropped, others are active, copy without touching ddl
                    copy_role := 'wait-replay';
                else
                    -- wait for lead to drop ddl
                    copy_role := 'wait-copy';
                end if;
            elsif merge_state = 'catching-up' then
                -- show copy_role only if need to wait for lead
                if q_part_ddl is not null then
                    copy_role := 'wait-replay';
                end if;
            end if;
        end if;
    end if;

    return;
end;
$$ language plpgsql strict stable;




create or replace function londiste.local_show_missing(
    in i_queue_name text,
    out obj_kind text, out obj_name text)
returns setof record as $$ 
-- ----------------------------------------------------------------------
-- Function: londiste.local_show_missing(1)
--
--      Return info about missing tables.  On root show tables
--      not registered on set, on branch/leaf show tables
--      in set but not registered locally.
-- ----------------------------------------------------------------------
begin
    if pgq_node.is_root_node(i_queue_name) then
        for obj_kind, obj_name in
            select r.relkind, n.nspname || '.' || r.relname
                from pg_catalog.pg_class r, pg_catalog.pg_namespace n
                where n.oid = r.relnamespace
                  and r.relkind in ('r', 'S')
                  and n.nspname not in ('pgq', 'pgq_ext', 'pgq_node', 'londiste', 'pg_catalog', 'information_schema')
                  and n.nspname !~ '^pg_(toast|temp)'
                  and not exists (select 1 from londiste.table_info
                                   where queue_name = i_queue_name and local
                                     and coalesce(dest_table, table_name) = (n.nspname || '.' || r.relname))
                order by 1, 2
        loop
            return next;
        end loop;
    else
        for obj_kind, obj_name in
            select 'S', s.seq_name from londiste.seq_info s
                where s.queue_name = i_queue_name
                  and not s.local
            union all
            select 'r', t.table_name from londiste.table_info t
                where t.queue_name = i_queue_name
                  and not t.local
            order by 1, 2
        loop
            return next;
        end loop;
    end if;
    return;
end; 
$$ language plpgsql strict stable;



-- Group: Local object registration (setup tool)


create or replace function londiste.local_add_seq(
    in i_queue_name text, in i_seq_name text,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_add_seq(2)
--
--      Register sequence.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_seq_name      - seq name
--
-- Returns:
--      200 - OK
--      400 - Not found
-- ----------------------------------------------------------------------
declare
    fq_seq_name text;
    lastval int8;
    seq record;
begin
    fq_seq_name := londiste.make_fqname(i_seq_name);

    perform 1 from pg_class
        where oid = londiste.find_seq_oid(fq_seq_name);
    if not found then
        select 400, 'Sequence not found: ' || fq_seq_name into ret_code, ret_note;
        return;
    end if;

    if pgq_node.is_root_node(i_queue_name) then
        select local, last_value into seq
            from londiste.seq_info
            where queue_name = i_queue_name
                and seq_name = fq_seq_name
            for update;
        if found and seq.local then
            select 201, 'Sequence already added: ' || fq_seq_name
                into ret_code, ret_note;
            return;
        end if;
        if not seq.local then
            update londiste.seq_info set local = true
                where queue_name = i_queue_name and seq_name = fq_seq_name;
        else
            insert into londiste.seq_info (queue_name, seq_name, local, last_value)
                values (i_queue_name, fq_seq_name, true, 0);
        end if;
        perform * from londiste.root_check_seqs(i_queue_name);
    else
        select local, last_value into seq
            from londiste.seq_info
            where queue_name = i_queue_name
                and seq_name = fq_seq_name
            for update;
        if not found then
            select 404, 'Unknown sequence: ' || fq_seq_name
                into ret_code, ret_note;
            return;
        end if;
        if seq.local then
            select 201, 'Sequence already added: ' || fq_seq_name
                into ret_code, ret_note;
            return;
        end if;
        update londiste.seq_info set local = true
            where queue_name = i_queue_name and seq_name = fq_seq_name;
        perform pgq.seq_setval(fq_seq_name, seq.last_value);
    end if;

    select 200, 'Sequence added: ' || fq_seq_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql;



create or replace function londiste.local_add_table(
    in i_queue_name     text,
    in i_table_name     text,
    in i_trg_args       text[],
    in i_table_attrs    text,
    in i_dest_table     text,
    out ret_code        int4,
    out ret_note        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_add_table(5)
--
--      Register table on Londiste node, with customizable trigger args.
--
-- Parameters:
--      i_queue_name    - queue name
--      i_table_name    - table name
--      i_trg_args      - args to trigger, or magic parameters.
--      i_table_attrs   - args to python handler
--      i_dest_table    - actual name of destination table (NULL if same)
--
-- Trigger args:
--      See documentation for pgq triggers.
--
-- Magic parameters:
--      no_triggers     - skip trigger creation
--      skip_truncate   - set 'skip_truncate' table attribute
--      expect_sync     - set table state to 'ok'
--      tgflags=X       - trigger creation flags
--      merge_all       - merge table from all sources. required for
--                        multi-source table
--      no_merge        - do not merge tables from different sources
--      skip            - create skip trigger. same as S flag
--      virtual_table   - skips structure check and trigger creation
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
-- Example:
--      > londiste.local_add_table('q', 'tbl', array['tgflags=BI', 'SKIP', 'pkey=col1,col2'])
--
-- Returns:
--      200 - Ok
--      301 - Warning, trigger exists that will fire before londiste one
--      400 - No such set
------------------------------------------------------------------------
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
    _extra_args text;
    tbl record;
    i integer;
    j integer;
    sql text;
    arg text;
    _node record;
    _tbloid oid;
    _combined_queue text;
    _combined_table text;
    -- skip trigger
    _skip_prefix text := 'zzz_';
    _skip_trg_count integer;
    _skip_trg_name text;
    -- check local tables from all sources
    _queue_name text;
    _local boolean;
    -- array with all tgflags values
    _check_flags char[] := array['B','A','Q','L','I','U','D','S'];
    -- given tgflags array
    _tgflags char[];
    -- ordinary argument array
    _args text[];
    -- argument flags
    _expect_sync boolean := false;
    _merge_all boolean := false;
    _no_merge boolean := false;
    _skip_truncate boolean := false;
    _no_triggers boolean := false;
    _skip boolean := false;
    _virtual_table boolean := false;
    _dest_table text;
    _got_extra1 boolean := false;
    _table_name2 text;
    _desc text;
begin

    -------- i_trg_args ARGUMENTS PARSING

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
            elsif arg = 'expect_sync' then
                _expect_sync := true;
            elsif arg = 'skip_truncate' then
                _skip_truncate := true;
            elsif arg = 'no_triggers' then
                _no_triggers := true;
            elsif arg = 'merge_all' then
                _merge_all = true;
            elsif arg = 'no_merge' then
                _no_merge = true;
            elsif lower(arg) = 'skip' then
                _skip := true;
            elsif arg = 'virtual_table' then
                _virtual_table := true;
                _expect_sync := true;   -- do not copy
                _no_triggers := true;   -- do not create triggers
            else
                if arg like 'ev_extra1=%' then
                    _got_extra1 := true;
                end if;
                -- ordinary arg
                _args = array_append(_args, quote_literal(arg));
            end if;
        end loop;
    end if;

    if _merge_all and _no_merge then
        select 405, 'Cannot use merge-all and no-merge together'
        into ret_code, ret_note;
        return;
    end if;

    fq_table_name := londiste.make_fqname(i_table_name);
    _dest_table := londiste.make_fqname(coalesce(i_dest_table, i_table_name));

    if _dest_table <> fq_table_name and not _got_extra1 then
        -- if renamed table, enforce trigger to put
        -- global table name into extra1
        arg := 'ev_extra1=' || quote_literal(fq_table_name);
        _args := array_append(_args, quote_literal(arg));
    end if;

    if _dest_table = fq_table_name then
        _desc := fq_table_name;
    else
        _desc := fq_table_name || '(' || _dest_table || ')';
    end if;

    -------- TABLE STRUCTURE CHECK

    if not _virtual_table then
        _tbloid := londiste.find_table_oid(_dest_table);
        if _tbloid is null then
            select 404, 'Table does not exist: ' || _desc into ret_code, ret_note;
            return;
        end if;
        col_types := londiste.find_column_types(_dest_table);
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
                and coalesce(t.dest_table, t.table_name) = _dest_table
                and t.dropped_ddl is not null;
            if not found then
                select 400, 'Primary key missing on table: ' || _desc into ret_code, ret_note;
                return;
            end if;
        end if;
    end if;

    -------- TABLE REGISTRATION LOGIC

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
            select 404, 'Table not available on queue: ' || _desc
                into ret_code, ret_note;
            return;
        end if;

        -- reload info
        select merge_state, local into tbl
            from londiste.table_info
            where queue_name = i_queue_name and table_name = fq_table_name;
    end if;

    if tbl.local then
        select 200, 'Table already added: ' || _desc into ret_code, ret_note;
        return;
    end if;

    if _node.node_type = 'root' then
        new_state := 'ok';
        perform londiste.root_notify_change(i_queue_name, 'londiste.add-table', fq_table_name);
    elsif _node.node_type = 'leaf' and _node.combined_type = 'branch' then
        new_state := 'ok';
    elsif _expect_sync then
        new_state := 'ok';
    else
        new_state := NULL;
    end if;

    update londiste.table_info
        set local = true,
            merge_state = new_state,
            table_attrs = coalesce(i_table_attrs, table_attrs),
            dest_table = nullif(_dest_table, fq_table_name)
        where queue_name = i_queue_name and table_name = fq_table_name;
    if not found then
        raise exception 'lost table: %', fq_table_name;
    end if;

    -- merge all table sources on leaf
    if _node.node_type = 'leaf' and not _no_merge then
        for _queue_name, _table_name2, _local in
            select t2.queue_name, t2.table_name, t2.local
            from londiste.table_info t
            join pgq_node.node_info n on (n.queue_name = t.queue_name)
            left join pgq_node.node_info n2 on (n2.combined_queue = n.combined_queue or
                    (n2.combined_queue is null and n.combined_queue is null))
            left join londiste.table_info t2
              on (t2.queue_name = n2.queue_name and
                  coalesce(t2.dest_table, t2.table_name) = coalesce(t.dest_table, t.table_name))
            where t.queue_name = i_queue_name
              and t.table_name = fq_table_name
              and t2.queue_name != i_queue_name -- skip self
        loop
            -- if table from some other source is already marked as local,
            -- raise error
            if _local and coalesce(new_state, 'x') <> 'ok' then
                select 405, 'Found local table '|| _desc
                        || ' in queue ' || _queue_name
                        || ', use remove-table first to remove all previous '
                        || 'table subscriptions'
                into ret_code, ret_note;
                return;
            end if;

           -- when table comes from multiple sources, merge_all switch is
           -- required
           if not _merge_all and coalesce(new_state, 'x') <> 'ok' then
               select 405, 'Found multiple sources for table '|| _desc
                       || ', use merge-all or no-merge to continue'
               into ret_code, ret_note;
               return;
           end if;

            update londiste.table_info
               set local = true,
                   merge_state = new_state,
                   table_attrs = coalesce(i_table_attrs, table_attrs)
               where queue_name = _queue_name and table_name = _table_name2;
            if not found then
                raise exception 'lost table: % on queue %', _table_name2, _queue_name;
            end if;
        end loop;

        -- if this node has combined_queue, add table there too
        -- note: we need to keep both table_name/dest_table values
        select n2.queue_name, t.table_name
            from pgq_node.node_info n1
            join pgq_node.node_info n2
                on (n2.queue_name = n1.combined_queue)
            left join londiste.table_info t
                on (t.queue_name = n2.queue_name and t.table_name = fq_table_name and t.local)
            where n1.queue_name = i_queue_name and n2.node_type = 'root'
            into _combined_queue, _combined_table;
        if found and _combined_table is null then
            select f.ret_code, f.ret_note
                from londiste.local_add_table(_combined_queue, fq_table_name, i_trg_args, i_table_attrs, _dest_table) f
                into ret_code, ret_note;
            if ret_code >= 300 then
                return;
            end if;
        end if;
    end if;

    if _skip_truncate then
        perform 1
        from londiste.local_set_table_attrs(i_queue_name, fq_table_name,
            coalesce(i_table_attrs || '&skip_truncate=1', 'skip_truncate=1'));
    end if;

    -------- TRIGGER LOGIC

    -- new trigger
    _extra_args := '';
    lg_name := '_londiste_' || i_queue_name;
    lg_func := 'pgq.logutriga';
    lg_event := '';
    lg_args := quote_literal(i_queue_name);
    lg_pos := 'after';

    if array_lower(_args, 1) is not null then
        lg_args := lg_args || ', ' || array_to_string(_args, ', ');
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

    if _node.node_type = 'leaf' then
        -- on weird leafs the trigger funcs may not exist
        perform 1 from pg_proc p join pg_namespace n on (n.oid = p.pronamespace)
            where n.nspname = 'pgq' and p.proname in ('logutriga', 'sqltriga');
        if not found then
            select 200, 'Table added with no triggers: ' || _desc into ret_code, ret_note;
            return;
        end if;
        -- on regular leaf, install deny trigger
        _extra_args := ', ' || quote_literal('deny');
    end if;

    -- if skip param given, rename previous skip triggers and prefix current
    if _skip then
        -- get count and name of existing skip triggers
        select count(*), min(t.tgname)
        into _skip_trg_count, _skip_trg_name
        from pg_catalog.pg_trigger t
        where t.tgrelid = londiste.find_table_oid(_dest_table)
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
                    || ' on ' || londiste.quote_fqname(_dest_table)
                    || ' rename to ' || _skip_prefix || _skip_trg_name;
                execute sql;
            end if;
        else
            select 405, 'Multiple SKIP triggers in table: ' || _desc
            into ret_code, ret_note;
            return;
        end if;
    end if;

    -- create Ins/Upd/Del trigger if it does not exists already
    perform 1 from pg_catalog.pg_trigger
        where tgrelid = londiste.find_table_oid(_dest_table)
            and tgname = lg_name;
    if not found then

        if _no_triggers then
            select 200, 'Table added with no triggers: ' || _desc
            into ret_code, ret_note;
            return;
        end if;

        -- finalize event
        lg_event := substr(lg_event, 4);
        if lg_event = '' then
            lg_event := 'insert or update or delete';
        end if;

        -- create trigger
        sql := 'create trigger ' || quote_ident(lg_name)
            || ' ' || lg_pos || ' ' || lg_event
            || ' on ' || londiste.quote_fqname(_dest_table)
            || ' for each row execute procedure '
            || lg_func || '(' || lg_args || _extra_args || ')';
        execute sql;
    end if;

    -- create truncate trigger if it does not exists already
    show server_version_num into pgversion;
    if pgversion >= 80400 then
        trunctrg_name  := '_londiste_' || i_queue_name || '_truncate';
        perform 1 from pg_catalog.pg_trigger
          where tgrelid = londiste.find_table_oid(_dest_table)
            and tgname = trunctrg_name;
        if not found then
            sql := 'create trigger ' || quote_ident(trunctrg_name)
                || ' after truncate on ' || londiste.quote_fqname(_dest_table)
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
        where r.oid = londiste.find_table_oid(_dest_table)
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
        where r.oid = londiste.find_table_oid(_dest_table)
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
              'Table added: ' || _desc
                              || ', but londiste trigger is not first: '
                              || logtrg_previous
         into ret_code, ret_note;
        return;
    end if;

    select 200, 'Table added: ' || _desc into ret_code, ret_note;
    return;
end;
$$ language plpgsql;

create or replace function londiste.local_add_table(
    in i_queue_name     text,
    in i_table_name     text,
    in i_trg_args       text[],
    in i_table_attrs    text,
    out ret_code        int4,
    out ret_note        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_add_table(4)
--
--      Register table on Londiste node.
-- ----------------------------------------------------------------------
begin
    select f.ret_code, f.ret_note into ret_code, ret_note
      from londiste.local_add_table(i_queue_name, i_table_name, i_trg_args, i_table_attrs, null) f;
    return;
end;
$$ language plpgsql;

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
--      Register table on Londiste node.
-- ----------------------------------------------------------------------
begin
    select f.ret_code, f.ret_note into ret_code, ret_note
      from londiste.local_add_table(i_queue_name, i_table_name, i_trg_args, null) f;
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
-- ----------------------------------------------------------------------
begin
    select f.ret_code, f.ret_note into ret_code, ret_note
      from londiste.local_add_table(i_queue_name, i_table_name, null) f;
    return;
end;
$$ language plpgsql strict;





create or replace function londiste.local_remove_seq(
    in i_queue_name text, in i_seq_name text,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_remove_seq(2)
--
--      Remove sequence.
--
-- Parameters:
--      i_queue_name      - set name
--      i_seq_name      - sequence name
--
-- Returns:
--      200 - OK
--      404 - Sequence not found
-- ----------------------------------------------------------------------
declare
    fqname text;
begin
    fqname := londiste.make_fqname(i_seq_name);
    if pgq_node.is_root_node(i_queue_name) then
        select f.ret_code, f.ret_note
            into ret_code, ret_note
            from londiste.global_remove_seq(i_queue_name, fqname) f;
        return;
    end if;
    update londiste.seq_info
        set local = false
        where queue_name = i_queue_name
          and seq_name = fqname
          and local;
    if not found then
        select 404, 'Sequence not found: '||fqname into ret_code, ret_note;
        return;
    end if;

    select 200, 'Sequence removed: '||fqname into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;




create or replace function londiste.local_remove_table(
    in i_queue_name text, in i_table_name text,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_remove_table(2)
--
--      Remove table.
--
-- Parameters:
--      i_queue_name      - set name
--      i_table_name      - table name
--
-- Returns:
--      200 - OK
--      404 - Table not found
-- ----------------------------------------------------------------------
declare
    fq_table_name   text;
    qtbl            text;
    seqname         text;
    tbl             record;
    tbl_oid         oid;
    pgver           integer;
begin
    fq_table_name := londiste.make_fqname(i_table_name);
    qtbl := londiste.quote_fqname(fq_table_name);
    tbl_oid := londiste.find_table_oid(i_table_name);
    show server_version_num into pgver;

    select local, dropped_ddl, merge_state into tbl
        from londiste.table_info
        where queue_name = i_queue_name
          and table_name = fq_table_name
        for update;
    if not found then
        select 400, 'Table not found: ' || fq_table_name into ret_code, ret_note;
        return;
    end if;

    if tbl.local then
        perform londiste.drop_table_triggers(i_queue_name, fq_table_name);

        -- restore dropped ddl
        if tbl.dropped_ddl is not null then
            -- table is not synced, drop data to make restore faster
            if pgver >= 80400 then
                execute 'TRUNCATE ONLY ' || qtbl;
            else
                execute 'TRUNCATE ' || qtbl;
            end if;
            execute tbl.dropped_ddl;
        end if;

        -- reset data
        update londiste.table_info
            set local = false,
                custom_snapshot = null,
                table_attrs = null,
                dropped_ddl = null,
                merge_state = null,
                dest_table = null
            where queue_name = i_queue_name
                and table_name = fq_table_name;

        -- drop dependent sequence
        for seqname in
            select n.nspname || '.' || s.relname
                from pg_catalog.pg_class s,
                     pg_catalog.pg_namespace n,
                     pg_catalog.pg_attribute a
                where a.attrelid = tbl_oid
                    and a.atthasdef
                    and a.atttypid::regtype::text in ('integer', 'bigint')
                    and s.oid = pg_get_serial_sequence(qtbl, a.attname)::regclass::oid
                    and n.oid = s.relnamespace
        loop
            perform londiste.local_remove_seq(i_queue_name, seqname);
        end loop;
    else
        if not pgq_node.is_root_node(i_queue_name) then
            select 400, 'Table not registered locally: ' || fq_table_name into ret_code, ret_note;
            return;
        end if;
    end if;

    if pgq_node.is_root_node(i_queue_name) then
        perform londiste.global_remove_table(i_queue_name, fq_table_name);
        perform londiste.root_notify_change(i_queue_name, 'londiste.remove-table', fq_table_name);
    end if;

    select 200, 'Table removed: ' || fq_table_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;



-- Group: Global object registrations (internal)


create or replace function londiste.global_add_table(
    in i_queue_name     text,
    in i_table_name     text,
    out ret_code        int4,
    out ret_note        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.global_add_table(2)
--
--      Register table on Londiste set.
--
--      This means its available from root, events for it appear
--      in queue and nodes can attach to it.
--
-- Called by:
--      on root - londiste.local_add_table()
--      elsewhere - londiste consumer when receives new table event
--
-- Returns:
--      200 - Ok
--      400 - No such set
-- ----------------------------------------------------------------------
declare
    fq_table_name text;
    _cqueue text;
begin
    fq_table_name := londiste.make_fqname(i_table_name);

    select combined_queue into _cqueue
        from pgq_node.node_info
        where queue_name = i_queue_name
        for update;
    if not found then
        select 400, 'No such queue: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;

    perform 1 from londiste.table_info where queue_name = i_queue_name and table_name = fq_table_name;
    if found then
        select 200, 'Table already added: ' || fq_table_name into ret_code, ret_note;
        return;
    end if;

    insert into londiste.table_info (queue_name, table_name)
        values (i_queue_name, fq_table_name);
    select 200, 'Table added: ' || i_table_name
        into ret_code, ret_note;

    -- let the combined node know about it too
    if _cqueue is not null then
        perform londiste.global_add_table(_cqueue, i_table_name);
    end if;

    return;
exception
    -- seems the row was added from parallel connection (setup vs. replay)
    when unique_violation then
        select 200, 'Table already added: ' || i_table_name
            into ret_code, ret_note;
        return;
end;
$$ language plpgsql strict;




create or replace function londiste.global_remove_table(
    in i_queue_name text, in i_table_name text,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.global_remove_table(2)
--
--      Removes tables registration in set.
--
--      Means that nodes cannot attach to this table anymore.
--
-- Called by:
--      - On root by londiste.local_remove_table()
--      - Elsewhere by consumer receiving table remove event
--
-- Returns:
--      200 - OK
--      400 - not found
-- ----------------------------------------------------------------------
declare
    fq_table_name text;
begin
    fq_table_name := londiste.make_fqname(i_table_name);
    if not pgq_node.is_root_node(i_queue_name) then
        perform londiste.local_remove_table(i_queue_name, fq_table_name);
    end if;
    delete from londiste.table_info
        where queue_name = i_queue_name
          and table_name = fq_table_name;
    if not found then
        select 400, 'Table not found: ' || fq_table_name
            into ret_code, ret_note;
        return;
    end if;
    select 200, 'Table removed: ' || i_table_name
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;




create or replace function londiste.global_update_seq(
    in i_queue_name text, in i_seq_name text, in i_value int8,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.global_update_seq(3)
--
--      Update seq.
--
-- Parameters:
--      i_queue_name  - set name
--      i_seq_name  - seq name
--      i_value     - new published value
--
-- Returns:
--      200 - OK
-- ----------------------------------------------------------------------
declare
    n record;
    fqname text;
    seq record;
begin
    select node_type, node_name into n
        from pgq_node.node_info
        where queue_name = i_queue_name;
    if not found then
        select 404, 'Set not found: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;
    if n.node_type = 'root' then
        select 402, 'Must not run on root node' into ret_code, ret_note;
        return;
    end if;

    fqname := londiste.make_fqname(i_seq_name);
    select last_value, local from londiste.seq_info
        into seq
        where queue_name = i_queue_name and seq_name = fqname
        for update;
    if not found then
        insert into londiste.seq_info
            (queue_name, seq_name, last_value)
        values (i_queue_name, fqname, i_value);
    else
        update londiste.seq_info
            set last_value = i_value
            where queue_name = i_queue_name and seq_name = fqname;
        if seq.local then
            perform pgq.seq_setval(fqname, i_value);
        end if;
    end if;
    select 200, 'Sequence updated' into ret_code, ret_note;
    return;
end;
$$ language plpgsql;




create or replace function londiste.global_remove_seq(
    in i_queue_name text, in i_seq_name text,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.global_remove_seq(2)
--
--      Removes sequence registration in set.
--
-- Called by:
--      - On root by londiste.local_remove_seq()
--      - Elsewhere by consumer receiving seq remove event
--
-- Returns:
--      200 - OK
--      400 - not found
-- ----------------------------------------------------------------------
declare
    fq_name text;
begin
    fq_name := londiste.make_fqname(i_seq_name);
    delete from londiste.seq_info
        where queue_name = i_queue_name
          and seq_name = fq_name;
    if not found then
        select 400, 'Sequence not found: '||fq_name into ret_code, ret_note;
        return;
    end if;
    if pgq_node.is_root_node(i_queue_name) then
        perform londiste.root_notify_change(i_queue_name, 'londiste.remove-seq', fq_name);
    end if;
    select 200, 'Sequence removed: '||fq_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;



-- Group: FKey handling


create or replace function londiste.get_table_pending_fkeys(i_table_name text) 
returns setof londiste.pending_fkeys as $$
-- ----------------------------------------------------------------------
-- Function: londiste.get_table_pending_fkeys(1)
--
--      Return dropped fkeys for table.
--
-- Parameters:
--      i_table_name - fqname
--
-- Returns:
--      desc
-- ----------------------------------------------------------------------
declare
    fkeys   record;
begin
    for fkeys in
        select *
        from londiste.pending_fkeys
        where from_table = i_table_name or to_table = i_table_name
        order by 1,2,3
    loop
        return next fkeys;
    end loop;
    return;
end;
$$ language plpgsql strict stable;


create or replace function londiste.get_valid_pending_fkeys(i_queue_name text)
returns setof londiste.pending_fkeys as $$
-- ----------------------------------------------------------------------
-- Function: londiste.get_valid_pending_fkeys(1)
--
--      Returns dropped fkeys where both sides are in sync now.
--
-- Parameters:
--      i_queue_name - cascaded queue name
--
-- Returns:
--      desc
-- ----------------------------------------------------------------------
declare
    fkeys   record;
begin
    for fkeys in
        select pf.*
        from londiste.pending_fkeys pf
        order by 1, 2, 3
    loop
        perform 1
           from londiste.table_info st_from
          where coalesce(st_from.dest_table, st_from.table_name) = fkeys.from_table
            and st_from.merge_state = 'ok'
            and st_from.custom_snapshot is null
            and st_from.queue_name = i_queue_name;
        if not found then
            continue;
        end if;
        perform 1
           from londiste.table_info st_to
          where coalesce(st_to.dest_table, st_to.table_name) = fkeys.to_table
            and st_to.merge_state = 'ok'
            and st_to.custom_snapshot is null
            and st_to.queue_name = i_queue_name;
        if not found then
            continue;
        end if;
        return next fkeys;
    end loop;
    
    return;
end;
$$ language plpgsql strict stable;


create or replace function londiste.drop_table_fkey(i_from_table text, i_fkey_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.drop_table_fkey(2)
--
--      Drop one fkey, save in pending table.
-- ----------------------------------------------------------------------
declare
    fkey       record;
begin        
    select * into fkey
    from londiste.find_table_fkeys(i_from_table) 
    where fkey_name = i_fkey_name and from_table = i_from_table;
    
    if not found then
        return 0;
    end if;
            
    insert into londiste.pending_fkeys values (fkey.from_table, fkey.to_table, i_fkey_name, fkey.fkey_def);
        
    execute 'alter table only ' || londiste.quote_fqname(fkey.from_table)
            || ' drop constraint ' || quote_ident(i_fkey_name);
    
    return 1;
end;
$$ language plpgsql strict;


create or replace function londiste.restore_table_fkey(i_from_table text, i_fkey_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.restore_table_fkey(2)
--
--      Restore dropped fkey.
--
-- Parameters:
--      i_from_table - source table
--      i_fkey_name  - fkey name
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    fkey    record;
begin
    select * into fkey
    from londiste.pending_fkeys 
    where fkey_name = i_fkey_name and from_table = i_from_table;
    
    if not found then
        return 0;
    end if;

    execute fkey.fkey_def;

    delete from londiste.pending_fkeys where fkey_name = fkey.fkey_name;
        
    return 1;
end;
$$ language plpgsql strict;



-- Group: Execute handling

create or replace function londiste.execute_start(
    in i_queue_name     text,
    in i_file_name      text,
    in i_sql            text,
    in i_expect_root    boolean,
    in i_attrs          text,
    out ret_code        int4,
    out ret_note        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.execute_start(5)
--
--      Start execution of DDL.  Should be called at the
--      start of the transaction that does the SQL execution.
--
-- Called-by:
--      Londiste setup tool on root, replay on branches/leafs.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_file_name     - Unique ID for SQL
--      i_sql           - Actual script (informative, not used here)
--      i_expect_root   - Is this on root?  Setup tool sets this to avoid
--                        execution on branches.
--      i_attrs         - urlencoded dict of extra attributes.
--                        The value will be put into ev_extra2
--                        field of outgoing event.
--
-- Returns:
--      200 - Proceed.
--      201 - Already applied
--      401 - Not root.
--      404 - No such queue
-- ----------------------------------------------------------------------
declare
    is_root boolean;
begin
    is_root := pgq_node.is_root_node(i_queue_name);
    if i_expect_root then
        if not is_root then
            select 401, 'Node is not root node: ' || i_queue_name
                into ret_code, ret_note;
            return;
        end if;
    end if;

    perform 1 from londiste.applied_execute
        where execute_file = i_file_name;
    if found then
        select 201, 'EXECUTE: "' || i_file_name || '" already applied, skipping'
            into ret_code, ret_note;
        return;
    end if;

    -- this also lock against potetial parallel execute
    insert into londiste.applied_execute (queue_name, execute_file, execute_sql, execute_attrs)
        values (i_queue_name, i_file_name, i_sql, i_attrs);

    select 200, 'Executing: ' || i_file_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql;

create or replace function londiste.execute_start(
    in i_queue_name     text,
    in i_file_name      text,
    in i_sql            text,
    in i_expect_root    boolean,
    out ret_code        int4,
    out ret_note        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.execute_start(4)
--
--      Start execution of DDL.  Should be called at the
--      start of the transaction that does the SQL execution.
--
-- Called-by:
--      Londiste setup tool on root, replay on branches/leafs.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_file_name     - Unique ID for SQL
--      i_sql           - Actual script (informative, not used here)
--      i_expect_root   - Is this on root?  Setup tool sets this to avoid
--                        execution on branches.
--
-- Returns:
--      200 - Proceed.
--      301 - Already applied
--      401 - Not root.
--      404 - No such queue
-- ----------------------------------------------------------------------
begin
    select f.ret_code, f.ret_note
      from londiste.execute_start(i_queue_name, i_file_name, i_sql, i_expect_root, null) f
      into ret_code, ret_note;
    return;
end;
$$ language plpgsql;



create or replace function londiste.execute_finish(
    in i_queue_name     text,
    in i_file_name      text,
    out ret_code        int4,
    out ret_note        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.execute_finish(2)
--
--      Finish execution of DDL.  Should be called at the
--      end of the transaction that does the SQL execution.
--
-- Called-by:
--      Londiste setup tool on root, replay on branches/leafs.
--
-- Returns:
--      200 - Proceed.
--      404 - Current entry not found, execute_start() was not called?
-- ----------------------------------------------------------------------
declare
    is_root boolean;
    sql text;
    attrs text;
begin
    is_root := pgq_node.is_root_node(i_queue_name);

    select execute_sql, execute_attrs
        into sql, attrs
        from londiste.applied_execute
        where execute_file = i_file_name;
    if not found then
        select 404, 'execute_file called without execute_start'
            into ret_code, ret_note;
        return;
    end if;

    if is_root then
        perform pgq.insert_event(i_queue_name, 'EXECUTE', sql, i_file_name, attrs, null, null);
    end if;

    select 200, 'Execute finished: ' || i_file_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;



-- Group: Internal functions


create or replace function londiste.root_check_seqs(
    in i_queue_name text, in i_buffer int8,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.root_check_seqs(1)
--
--      Check sequences, and publish values if needed.
--
-- Parameters:
--      i_queue_name    - set name
--      i_buffer        - safety room
--
-- Returns:
--      200 - OK
--      402 - Not a root node
--      404 - Queue not found
-- ----------------------------------------------------------------------
declare
    n record;
    seq record;
    real_value int8;
    pub_value int8;
    real_buffer int8;
begin
    if i_buffer is null or i_buffer < 10 then
        real_buffer := 10000;
    else
        real_buffer := i_buffer;
    end if;

    select node_type, node_name into n
        from pgq_node.node_info
        where queue_name = i_queue_name
        for update;
    if not found then
        select 404, 'Queue not found: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;
    if n.node_type <> 'root' then
        select 402, 'Not a root node' into ret_code, ret_note;
        return;
    end if;

    for seq in
        select seq_name, last_value,
               londiste.quote_fqname(seq_name) as fqname
            from londiste.seq_info
            where queue_name = i_queue_name
                and local
            order by nr
    loop
        execute 'select last_value from ' || seq.fqname into real_value;
        if real_value + real_buffer >= seq.last_value then
            pub_value := real_value + real_buffer * 3;
            perform pgq.insert_event(i_queue_name, 'londiste.update-seq',
                        pub_value::text, seq.seq_name, null, null, null);
            update londiste.seq_info set last_value = pub_value
                where queue_name = i_queue_name
                    and seq_name = seq.seq_name;
        end if;
    end loop;

    select 100, 'Sequences updated' into ret_code, ret_note;
    return;
end;
$$ language plpgsql;

create or replace function londiste.root_check_seqs(
    in i_queue_name text,
    out ret_code int4, out ret_note text)
as $$
begin
    select f.ret_code, f.ret_note
        into ret_code, ret_note
        from londiste.root_check_seqs(i_queue_name, 10000) f;
    return;
end;
$$ language plpgsql;




create or replace function londiste.root_notify_change(i_queue_name text, i_ev_type text, i_ev_data text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.root_notify_change(3)
--
--      Send event about change in root downstream.
-- ----------------------------------------------------------------------
declare
    que     text;
    ntype   text;
begin

    if not coalesce(pgq_node.is_root_node(i_queue_name), false) then
        raise exception 'only root node can send events';
    end if;
    perform pgq.insert_event(i_queue_name, i_ev_type, i_ev_data);

    return 1;
end;
$$ language plpgsql;




create or replace function londiste.local_set_table_state(
    in i_queue_name text,
    in i_table_name text,
    in i_snapshot text,
    in i_merge_state text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_set_table_state(4)
--
--      Change table state.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_table         - table name
--      i_snapshot      - optional remote snapshot info
--      i_merge_state   - merge state
-- ----------------------------------------------------------------------
declare
    _tbl text;
begin
    _tbl = londiste.make_fqname(i_table_name);

    update londiste.table_info
        set custom_snapshot = i_snapshot,
            merge_state = i_merge_state
      where queue_name = i_queue_name
        and table_name = _tbl
        and local;
    if not found then
        select 404, 'No such table: ' || _tbl
            into ret_code, ret_note;
        return;
    end if;

    select 200, 'Table ' || _tbl || ' state set to '
            || coalesce(quote_literal(i_merge_state), 'NULL')
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql;




create or replace function londiste.local_set_table_attrs(
    in i_queue_name text,
    in i_table_name text,
    in i_table_attrs text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_set_table_attrs(3)
--
--      Store urlencoded table attributes.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_table         - table name
--      i_table_attrs   - urlencoded attributes
-- ----------------------------------------------------------------------
begin
    update londiste.table_info
        set table_attrs = i_table_attrs
      where queue_name = i_queue_name
        and table_name = i_table_name
        and local;
    if found then
        select 200, i_table_name || ': Table attributes stored'
            into ret_code, ret_note;
    else
        select 404, 'no such local table: ' || i_table_name
            into ret_code, ret_note;
    end if;
    return;
end;
$$ language plpgsql;




create or replace function londiste.local_set_table_struct(
    in i_queue_name text,
    in i_table_name text,
    in i_dropped_ddl text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_set_table_struct(3)
--
--      Store dropped table struct temporarily.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_table         - table name
--      i_dropped_ddl   - merge state
-- ----------------------------------------------------------------------
begin
    update londiste.table_info
        set dropped_ddl = i_dropped_ddl
      where queue_name = i_queue_name
        and table_name = i_table_name
        and local;
    if found then
        select 200, 'Table struct stored'
            into ret_code, ret_note;
    else
        select 404, 'no such local table: '||i_table_name
            into ret_code, ret_note;

    end if;
    return;
end;
$$ language plpgsql;




create or replace function londiste.periodic_maintenance()
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.periodic_maintenance(0)
--
--      Clean random stuff.
-- ----------------------------------------------------------------------
begin

    -- clean old EXECUTE entries
    delete from londiste.applied_execute
        where execute_time < now() - '3 months'::interval;

    return 0;
end;
$$ language plpgsql; -- need admin access



-- Group: Utility functions

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




create or replace function londiste.find_table_fkeys(i_table_name text)
returns setof londiste.pending_fkeys as $$
-- ----------------------------------------------------------------------
-- Function: londiste.find_table_fkeys(1)
--
--      Return all active fkeys.
--
-- Parameters:
--      i_table_name    - fqname
--
-- Returns:
--      from_table      - fqname
--      to_table        - fqname
--      fkey_name       - name
--      fkey_def        - full def
-- ----------------------------------------------------------------------
declare
    fkey      record;
    tbl_oid   oid;
begin
    select londiste.find_table_oid(i_table_name) into tbl_oid;
        
    for fkey in
        select n1.nspname || '.' || t1.relname as from_table, n2.nspname || '.' || t2.relname as to_table,
            conname::text as fkey_name, 
            'alter table only ' || quote_ident(n1.nspname) || '.' || quote_ident(t1.relname)
            || ' add constraint ' || quote_ident(conname::text) || ' ' || pg_get_constraintdef(c.oid)
            as fkey_def
        from pg_constraint c, pg_namespace n1, pg_class t1, pg_namespace n2, pg_class t2
        where c.contype = 'f' and (c.conrelid = tbl_oid or c.confrelid = tbl_oid)
            and t1.oid = c.conrelid and n1.oid = t1.relnamespace
            and t2.oid = c.confrelid and n2.oid = t2.relnamespace
        order by 1,2,3
    loop
        return next fkey;
    end loop;
    
    return;
end;
$$ language plpgsql strict stable;





drop function if exists londiste.find_seq_oid(text);
drop function if exists londiste.find_table_oid(text);
drop function if exists londiste.find_rel_oid(text, text);

create or replace function londiste.find_rel_oid(i_fqname text, i_kind text)
returns oid as $$
-- ----------------------------------------------------------------------
-- Function: londiste.find_rel_oid(2)
--
--      Find pg_class row oid.
--
-- Parameters:
--      i_fqname    - fq object name
--      i_kind      - relkind value
--
-- Returns:
--      oid or exception of not found
-- ----------------------------------------------------------------------
declare
    res      oid;
    pos      integer;
    schema   text;
    name     text;
begin
    pos := position('.' in i_fqname);
    if pos > 0 then
        schema := substring(i_fqname for pos - 1);
        name := substring(i_fqname from pos + 1);
    else
        schema := 'public';
        name := i_fqname;
    end if;
    select c.oid into res
      from pg_namespace n, pg_class c
     where c.relnamespace = n.oid
       and c.relkind = i_kind
       and n.nspname = schema and c.relname = name;
    if not found then
        res := NULL;
    end if;

    return res;
end;
$$ language plpgsql strict stable;


create or replace function londiste.find_table_oid(tbl text)
returns oid as $$
-- ----------------------------------------------------------------------
-- Function: londiste.find_table_oid(1)
--
--      Find table oid based on fqname.
--
-- Parameters:
--      tbl - fqname
--
-- Returns:
--      oid
-- ----------------------------------------------------------------------
begin
    return londiste.find_rel_oid(tbl, 'r');
end;
$$ language plpgsql strict stable;


create or replace function londiste.find_seq_oid(seq text)
returns oid as $$
-- ----------------------------------------------------------------------
-- Function: londiste.find_seq_oid(1)
--
--      Find sequence oid based on fqname.
--
-- Parameters:
--      seq - fqname
--
-- Returns:
--      oid
-- ----------------------------------------------------------------------
begin
    return londiste.find_rel_oid(seq, 'S');
end;
$$ language plpgsql strict stable;




create or replace function londiste.quote_fqname(i_name text)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: londiste.quote_fqname(1)
--
--      Quete fully-qualified object name for SQL.
--
--      First dot is taken as schema separator.
--
--      If schema is missing, 'public' is assumed.
--
-- Parameters:
--      i_name  - fully qualified object name.
--
-- Returns:
--      Quoted name.
-- ----------------------------------------------------------------------
declare
    res     text;
    pos     integer;
    s       text;
    n       text;
begin
    pos := position('.' in i_name);
    if pos > 0 then
        s := substring(i_name for pos - 1);
        n := substring(i_name from pos + 1);
    else
        s := 'public';
        n := i_name;
    end if;
    return quote_ident(s) || '.' || quote_ident(n);
end;
$$ language plpgsql strict immutable;




create or replace function londiste.make_fqname(i_name text)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: londiste.make_fqname(1)
--
--      Make name to schema-qualified one.
--
--      First dot is taken as schema separator.
--
--      If schema is missing, 'public' is assumed.
--
-- Parameters:
--      i_name  - object name.
--
-- Returns:
--      Schema qualified name.
-- ----------------------------------------------------------------------
begin
    if position('.' in i_name) > 0 then
        return i_name;
    else
        return 'public.' || i_name;
    end if;
end;
$$ language plpgsql strict immutable;



create or replace function londiste.split_fqname(
    in i_fqname text,
    out schema_part text,
    out name_part text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.split_fqname(1)
--
--      Split fqname to schema and name parts.
--
--      First dot is taken as schema separator.
--
--      If schema is missing, 'public' is assumed.
--
-- Parameters:
--      i_fqname  - object name.
-- ----------------------------------------------------------------------
declare
    dot integer;
begin
    dot = position('.' in i_fqname);
    if dot > 0 then
        schema_part = substring(i_fqname for dot - 1);
        name_part = substring(i_fqname from dot + 1);
    else
        schema_part = 'public';
        name_part = i_fqname;
    end if;
    return;
end;
$$ language plpgsql strict immutable;




create or replace function londiste.table_info_trigger()
returns trigger as $$
-- ----------------------------------------------------------------------
-- Function: londiste.table_info_trigger(0)
--
--      Trigger on londiste.table_info.  Cleans triggers from tables
--      when table is removed from londiste.table_info.
-- ----------------------------------------------------------------------
begin
    if TG_OP = 'DELETE' then
        perform londiste.drop_table_triggers(OLD.queue_name, OLD.table_name);
    end if;
    return null;
end;
$$ language plpgsql;




create or replace function londiste.drop_table_triggers(
    in i_queue_name text, in i_table_name text)
returns void as $$
-- ----------------------------------------------------------------------
-- Function: londiste.drop_table_triggers(2)
--
--      Remove Londiste triggers from table.
--
-- Parameters:
--      i_queue_name      - set name
--      i_table_name      - table name
--
-- Returns:
--      200 - OK
--      404 - Table not found
-- ----------------------------------------------------------------------
declare
    logtrg_name     text;
    b_queue_name    bytea;
    _dest_table     text;
begin
    select coalesce(dest_table, table_name)
        from londiste.table_info t
        where t.queue_name = i_queue_name
          and t.table_name = i_table_name
        into _dest_table;
    if not found then
        return;
    end if;

    -- skip if no triggers found on that table
    perform 1 from pg_catalog.pg_trigger where tgrelid = londiste.find_table_oid(_dest_table);
    if not found then
        return;
    end if;

    -- cast to bytea
    b_queue_name := decode(replace(i_queue_name, E'\\', E'\\\\'), 'escape');

    -- drop all replication triggers that target our queue.
    -- by checking trigger func and queue name there is not
    -- dependency on naming standard or side-storage.
    for logtrg_name in
        select tgname from pg_catalog.pg_trigger
         where tgrelid = londiste.find_table_oid(_dest_table)
           and londiste.is_replica_func(tgfoid)
           and octet_length(tgargs) > 0
           and substring(tgargs for (position(E'\\000'::bytea in tgargs) - 1)) = b_queue_name
    loop
        execute 'drop trigger ' || quote_ident(logtrg_name)
                || ' on ' || londiste.quote_fqname(_dest_table);
    end loop;
end;
$$ language plpgsql strict;




create or replace function londiste.is_replica_func(func_oid oid)
returns boolean as $$
-- ----------------------------------------------------------------------
-- Function: londiste.is_replica_func(1)
--
--      Returns true if function is a PgQ-based replication functions.
--      This also means it takes queue name as first argument.
-- ----------------------------------------------------------------------
select count(1) > 0
  from pg_proc f join pg_namespace n on (n.oid = f.pronamespace)
  where f.oid = $1 and n.nspname = 'pgq' and f.proname in ('sqltriga', 'logutriga');
$$ language sql strict stable;




create or replace function londiste.version()
returns text as $$
-- ----------------------------------------------------------------------
-- Function: londiste.version(0)
--
--      Returns version string for londiste.  ATM it is based on SkyTools version
--      and only bumped when database code changes.
-- ----------------------------------------------------------------------
begin
    return '3.1.1';
end;
$$ language plpgsql;



-- Group: Utility functions for handlers


create or replace function londiste.create_partition(
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
--      Creates inherited child table if it does not exist by copying parent table's structure.
--      Locks parent table to avoid parallel creation.
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
    execute 'lock table ' || fq_table || ' in share update exclusive mode';
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
        select rw.rulename, rw.ev_enabled, pg_get_ruledef(rw.oid) as definition
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
        end if;
    end loop;

    return 1;
end;
$$ language plpgsql;






create trigger table_info_trigger_sync after delete on londiste.table_info
for each row execute procedure londiste.table_info_trigger();




grant usage on schema londiste to public;
grant select on londiste.table_info to public;
grant select on londiste.seq_info to public;
grant select on londiste.pending_fkeys to public;
grant select on londiste.applied_execute to public;



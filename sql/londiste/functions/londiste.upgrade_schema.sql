
create or replace function londiste.upgrade_schema()
returns int4 as $$
-- updates table structure if necessary
declare
    pgversion int;
    cnt int4 = 0;
begin
    show server_version_num into pgversion;

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

    -- table_info: change trigger timing
    if pgversion >= 90100 then
        perform 1 from information_schema.triggers
          where event_object_schema = 'londiste'
            and event_object_table = 'table_info'
            and trigger_name = 'table_info_trigger_sync'
            and action_timing = 'AFTER';
    else
        perform 1 from information_schema.triggers
          where event_object_schema = 'londiste'
            and event_object_table = 'table_info'
            and trigger_name = 'table_info_trigger_sync'
            and condition_timing = 'AFTER';
    end if;
    if found then
        drop trigger table_info_trigger_sync on londiste.table_info;
        create trigger table_info_trigger_sync before delete on londiste.table_info
            for each row execute procedure londiste.table_info_trigger();
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


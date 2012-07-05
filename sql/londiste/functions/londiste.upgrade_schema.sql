
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


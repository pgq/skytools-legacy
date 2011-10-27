
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

    return cnt;
end;
$$ language plpgsql;


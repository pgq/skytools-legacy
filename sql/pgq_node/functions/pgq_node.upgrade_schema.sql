
create or replace function pgq_node.upgrade_schema()
returns int4 as $$
-- updates table structure if necessary
declare
    cnt int4 = 0;
begin
    -- node_info.node_attrs
    perform 1 from information_schema.columns
      where table_schema = 'pgq_node'
        and table_name = 'node_info'
        and column_name = 'node_attrs';
    if not found then
        alter table pgq_node.node_info add column node_attrs text;
        cnt := cnt + 1;
    end if;

    return cnt;
end;
$$ language plpgsql;


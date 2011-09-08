
create or replace function pgq_node.upgrade_schema()
returns int4 as $$
-- updates table structure if necessary
declare
    cnt int4 = 0;
begin
    return cnt;
end;
$$ language plpgsql;



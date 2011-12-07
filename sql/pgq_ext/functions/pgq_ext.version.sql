
create or replace function pgq_ext.version()
returns text as $$
begin
    return '3.0.0.1';
end;
$$ language plpgsql;


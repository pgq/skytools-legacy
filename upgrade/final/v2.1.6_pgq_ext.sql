
begin;



create or replace function pgq_ext.version()
returns text as $$
begin
    return '2.1.6';
end;
$$ language plpgsql;



end;



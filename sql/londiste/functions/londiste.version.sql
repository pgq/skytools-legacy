
create or replace function londiste.version()
returns text as $$
begin
    return '2.1.7';
end;
$$ language plpgsql;


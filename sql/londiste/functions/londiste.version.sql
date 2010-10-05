
create or replace function londiste.version()
returns text as $$
begin
    return '2.1.12';
end;
$$ language plpgsql;


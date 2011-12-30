
create or replace function londiste.version()
returns text as $$
begin
    return '3.0.0.13';
end;
$$ language plpgsql;


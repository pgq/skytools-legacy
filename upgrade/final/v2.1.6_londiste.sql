
begin;



create or replace function londiste.version()
returns text as $$
begin
    return '2.1.6';
end;
$$ language plpgsql;



end;



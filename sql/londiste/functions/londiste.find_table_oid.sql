create or replace function londiste.find_rel_oid(tbl text, kind text)
returns oid as $$
declare
    res      oid;
    pos      integer;
    schema   text;
    name     text;
begin
    pos := position('.' in tbl);
    if pos > 0 then
        schema := substring(tbl for pos - 1);
        name := substring(tbl from pos + 1);
    else
        schema := 'public';
        name := tbl;
    end if;
    select c.oid into res
      from pg_namespace n, pg_class c
     where c.relnamespace = n.oid
       and c.relkind = kind
       and n.nspname = schema and c.relname = name;
    if not found then
        if kind = 'r' then
            raise exception 'table not found';
        elsif kind = 'S' then
            raise exception 'seq not found';
        else
            raise exception 'weird relkind';
        end if;
    end if;

    return res;
end;
$$ language plpgsql strict stable;

create or replace function londiste.find_table_oid(tbl text)
returns oid as $$
begin
    return londiste.find_rel_oid(tbl, 'r');
end;
$$ language plpgsql strict stable;

create or replace function londiste.find_seq_oid(tbl text)
returns oid as $$
begin
    return londiste.find_rel_oid(tbl, 'S');
end;
$$ language plpgsql strict stable;


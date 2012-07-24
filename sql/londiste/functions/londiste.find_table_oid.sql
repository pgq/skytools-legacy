
drop function if exists londiste.find_seq_oid(text);
drop function if exists londiste.find_table_oid(text);
drop function if exists londiste.find_rel_oid(text, text);

create or replace function londiste.find_rel_oid(i_fqname text, i_kind text)
returns oid as $$
-- ----------------------------------------------------------------------
-- Function: londiste.find_rel_oid(2)
--
--      Find pg_class row oid.
--
-- Parameters:
--      i_fqname    - fq object name
--      i_kind      - relkind value
--
-- Returns:
--      oid or exception of not found
-- ----------------------------------------------------------------------
declare
    res      oid;
    pos      integer;
    schema   text;
    name     text;
begin
    pos := position('.' in i_fqname);
    if pos > 0 then
        schema := substring(i_fqname for pos - 1);
        name := substring(i_fqname from pos + 1);
    else
        schema := 'public';
        name := i_fqname;
    end if;
    select c.oid into res
      from pg_namespace n, pg_class c
     where c.relnamespace = n.oid
       and c.relkind = i_kind
       and n.nspname = schema and c.relname = name;
    if not found then
        res := NULL;
    end if;

    return res;
end;
$$ language plpgsql strict stable;


create or replace function londiste.find_table_oid(tbl text)
returns oid as $$
-- ----------------------------------------------------------------------
-- Function: londiste.find_table_oid(1)
--
--      Find table oid based on fqname.
--
-- Parameters:
--      tbl - fqname
--
-- Returns:
--      oid
-- ----------------------------------------------------------------------
begin
    return londiste.find_rel_oid(tbl, 'r');
end;
$$ language plpgsql strict stable;


create or replace function londiste.find_seq_oid(seq text)
returns oid as $$
-- ----------------------------------------------------------------------
-- Function: londiste.find_seq_oid(1)
--
--      Find sequence oid based on fqname.
--
-- Parameters:
--      seq - fqname
--
-- Returns:
--      oid
-- ----------------------------------------------------------------------
begin
    return londiste.find_rel_oid(seq, 'S');
end;
$$ language plpgsql strict stable;



create or replace function londiste.make_fqname(i_name text)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: londiste.make_fqname(1)
--
--      Make name to schema-qualified one.
--
--      First dot is taken as schema separator.
--
--      If schema is missing, 'public' is assumed.
--
-- Parameters:
--      i_name  - object name.
--
-- Returns:
--      Schema qualified name.
-- ----------------------------------------------------------------------
begin
    if position('.' in i_name) > 0 then
        return i_name;
    else
        return 'public.' || i_name;
    end if;
end;
$$ language plpgsql strict immutable;


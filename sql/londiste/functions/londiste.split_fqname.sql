create or replace function londiste.split_fqname(
    in i_fqname text,
    out schema_part text,
    out name_part text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.split_fqname(1)
--
--      Split fqname to schema and name parts.
--
--      First dot is taken as schema separator.
--
--      If schema is missing, 'public' is assumed.
--
-- Parameters:
--      i_fqname  - object name.
-- ----------------------------------------------------------------------
declare
    dot integer;
begin
    dot = position('.' in i_fqname);
    if dot > 0 then
        schema_part = substring(i_fqname for dot - 1);
        name_part = substring(i_fqname from dot + 1);
    else
        schema_part = 'public';
        name_part = i_fqname;
    end if;
    return;
end;
$$ language plpgsql strict immutable;


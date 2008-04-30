
create or replace function pgq.seq_getval(i_seq_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.seq_getval(1)
--
--      read current last_val from seq, without offecting it.
--
-- Parameters:
--      i_seq_name     - Name of the sequence
--
-- Returns:
--      last value.
-- ----------------------------------------------------------------------
declare
    res  int8;
begin
    execute 'select last_value from ' || i_seq_name into res;
    return res;
end;
$$ language plpgsql;

create or replace function pgq.seq_setval(i_seq_name text, i_new_value int8)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.seq_setval(2)
--
--      Like setval() but does not allow going back.
--
-- Parameters:
--      i_seq_name      - Name of the sequence
--      i_new_value     - new value
--
-- Returns:
--      current last value.
-- ----------------------------------------------------------------------
declare
    res  int8;
begin
    res := pgq.seq_getval(i_seq_name);
    if res < i_new_value then
        perform setval(i_seq_name, i_new_value);
        return i_new_value;
    end if;
    return res;
end;
$$ language plpgsql;


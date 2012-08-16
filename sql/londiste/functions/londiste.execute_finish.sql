create or replace function londiste.execute_finish(
    in i_queue_name     text,
    in i_file_name      text,
    out ret_code        int4,
    out ret_note        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.execute_finish(2)
--
--      Finish execution of DDL.  Should be called at the
--      end of the transaction that does the SQL execution.
--
-- Called-by:
--      Londiste setup tool on root, replay on branches/leafs.
--
-- Returns:
--      200 - Proceed.
--      404 - Current entry not found, execute_start() was not called?
-- ----------------------------------------------------------------------
declare
    is_root boolean;
    sql text;
    attrs text;
begin
    is_root := pgq_node.is_root_node(i_queue_name);

    select execute_sql, execute_attrs
        into sql, attrs
        from londiste.applied_execute
        where execute_file = i_file_name;
    if not found then
        select 404, 'execute_file called without execute_start'
            into ret_code, ret_note;
        return;
    end if;

    if is_root then
        perform pgq.insert_event(i_queue_name, 'EXECUTE', sql, i_file_name, attrs, null, null);
    end if;

    select 200, 'Execute finished: ' || i_file_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;


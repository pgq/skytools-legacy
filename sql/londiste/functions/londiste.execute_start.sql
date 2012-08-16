create or replace function londiste.execute_start(
    in i_queue_name     text,
    in i_file_name      text,
    in i_sql            text,
    in i_expect_root    boolean,
    in i_attrs          text,
    out ret_code        int4,
    out ret_note        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.execute_start(5)
--
--      Start execution of DDL.  Should be called at the
--      start of the transaction that does the SQL execution.
--
-- Called-by:
--      Londiste setup tool on root, replay on branches/leafs.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_file_name     - Unique ID for SQL
--      i_sql           - Actual script (informative, not used here)
--      i_expect_root   - Is this on root?  Setup tool sets this to avoid
--                        execution on branches.
--      i_attrs         - urlencoded dict of extra attributes.
--                        The value will be put into ev_extra2
--                        field of outgoing event.
--
-- Returns:
--      200 - Proceed.
--      201 - Already applied
--      401 - Not root.
--      404 - No such queue
-- ----------------------------------------------------------------------
declare
    is_root boolean;
begin
    is_root := pgq_node.is_root_node(i_queue_name);
    if i_expect_root then
        if not is_root then
            select 401, 'Node is not root node: ' || i_queue_name
                into ret_code, ret_note;
            return;
        end if;
    end if;

    perform 1 from londiste.applied_execute
        where execute_file = i_file_name;
    if found then
        select 201, 'EXECUTE: "' || i_file_name || '" already applied, skipping'
            into ret_code, ret_note;
        return;
    end if;

    -- this also lock against potetial parallel execute
    insert into londiste.applied_execute (queue_name, execute_file, execute_sql, execute_attrs)
        values (i_queue_name, i_file_name, i_sql, i_attrs);

    select 200, 'Executing: ' || i_file_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql;

create or replace function londiste.execute_start(
    in i_queue_name     text,
    in i_file_name      text,
    in i_sql            text,
    in i_expect_root    boolean,
    out ret_code        int4,
    out ret_note        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.execute_start(4)
--
--      Start execution of DDL.  Should be called at the
--      start of the transaction that does the SQL execution.
--
-- Called-by:
--      Londiste setup tool on root, replay on branches/leafs.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_file_name     - Unique ID for SQL
--      i_sql           - Actual script (informative, not used here)
--      i_expect_root   - Is this on root?  Setup tool sets this to avoid
--                        execution on branches.
--
-- Returns:
--      200 - Proceed.
--      301 - Already applied
--      401 - Not root.
--      404 - No such queue
-- ----------------------------------------------------------------------
begin
    select f.ret_code, f.ret_note
      from londiste.execute_start(i_queue_name, i_file_name, i_sql, i_expect_root, null) f
      into ret_code, ret_note;
    return;
end;
$$ language plpgsql;


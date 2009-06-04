
create or replace function londiste.get_table_list(
    in i_queue_name text,
    out table_name text,
    out local boolean,
    out merge_state text,
    out custom_snapshot text,
    out table_attrs text,
    out dropped_ddl text,
    out copy_role text)
returns setof record as $$ 
-- ----------------------------------------------------------------------
-- Function: londiste.get_table_list(1)
--
--      Return info about registered tables.
--
-- Parameters:
--      i_queue_name - cascaded queue name
--
-- Returns:
--      table_name      - fully-quelified table name
--      local           - does events needs to be applied to local table
--      merge_state     - show phase of initial copy
--      custom_snapshot - remote snapshot of COPY transaction
--      table_attrs     - urlencoded dict of table attributes
--      dropped_ddl     - partition combining: temp place to put DDL
--      copy_role       - partition combining: how to handle copy
--
-- copy_role = lead:
--      on copy start, drop indexes and store in dropped_ddl
--      on copy finish wait, until copy_role turns to NULL
--      if dropped_ddl not NULL, restore them
--      tag as catching-up
-- copy_role = wait-copy:
--      on copy start wait, until role changes (to wait-replay)
-- copy_role = wait-replay:
--      on copy finish, tag as 'catching-up'
--      wait until copy_role is NULL, then proceed
--
declare
    q_part1     text;
    q_target    text;
    n_parts     int4;
    n_done      int4;
begin
    -- get first part queue, if exists
    select n.combined_queue into q_target
        from pgq_node.node_info n
        where n.queue_name = i_queue_name;
    if q_target is not null then
        select n.queue_name into q_part1
            from pgq_node.node_info n
            where n.combined_queue = q_target
            order by n.queue_name
            limit 1;
        select count(*) into n_parts
            from pgq_node.node_info n
            where n.combined_queue = q_target;
    end if;

    for table_name, local, merge_state, custom_snapshot, table_attrs, dropped_ddl in 
        select t.table_name, t.local, t.merge_state, t.custom_snapshot, t.table_attrs, t.dropped_ddl
            from londiste.table_info t
            where t.queue_name = i_queue_name
            order by t.nr, t.table_name
    loop
        -- if the table is in middle of copy from multiple partitions,
        -- the copy processes need coordination
        copy_role := null;
        if q_part1 is not null then
            select count(*) into n_done
                from londiste.table_info t, pgq_node.node_info n
                where n.combined_queue = q_target
                    and t.queue_name = n.queue_name
                    and t.table_name = table_name
                    and (t.merge_state is not null
                         and t.merge_state <> 'in-copy');
            if i_queue_name = q_part1 then
                -- lead
                if merge_state = 'in-copy' then
                    -- show copy_role only if need to wait for others
                    if n_done < n_parts - 1 then
                        copy_role := 'lead';
                    end if;
                end if;
            else
                -- follow
                if merge_state = 'in-copy' then
                    -- has lead already dropped ddl?
                    perform 1 from londiste.table_info t
                        where t.queue_name = q_part1
                            and t.table_name = table_name
                            and t.dropped_ddl is not null;
                    if found then
                        copy_role := 'wait-replay';
                    else
                        copy_role := 'wait-copy';
                    end if;
                elsif merge_state = 'catching-up' then
                    -- show copy_role only if need to wait for lead
                    if n_done < n_parts then
                        copy_role := 'wait-replay';
                    end if;
                end if;
            end if;
        end if;

        return next;
    end loop; 
    return;
end; 
$$ language plpgsql strict stable;


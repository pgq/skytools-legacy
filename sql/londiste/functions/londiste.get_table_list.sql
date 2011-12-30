
drop function if exists londiste.get_table_list(text);

create or replace function londiste.get_table_list(
    in i_queue_name text,
    out table_name text,
    out local boolean,
    out merge_state text,
    out custom_snapshot text,
    out table_attrs text,
    out dropped_ddl text,
    out copy_role text,
    out copy_pos int4,
    out dest_table text)
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
--      copy_pos        - position in parallel copy working order
--
-- copy_role = lead:
--      on copy start, drop indexes and store in dropped_ddl
--      on copy finish change state to catching-up, then wait until copy_role turns to NULL
--      catching-up: if dropped_ddl not NULL, restore them
-- copy_role = wait-copy:
--      on copy start wait, until role changes (to wait-replay)
-- copy_role = wait-replay:
--      on copy finish, tag as 'catching-up'
--      wait until copy_role is NULL, then proceed
-- ----------------------------------------------------------------------
begin
    for table_name, local, merge_state, custom_snapshot, table_attrs,
        dropped_ddl, dest_table
    in
        select t.table_name, t.local, t.merge_state, t.custom_snapshot, t.table_attrs,
               t.dropped_ddl, t.dest_table
            from londiste.table_info t
            where t.queue_name = i_queue_name
            order by t.nr, t.table_name
    loop
        copy_role := null;
        copy_pos := 0;

        if merge_state in ('in-copy', 'catching-up') then
            select f.copy_role, f.copy_pos
                from londiste._coordinate_copy(i_queue_name, table_name) f
                into copy_role, copy_pos;
        end if;

        return next;
    end loop;
    return;
end;
$$ language plpgsql strict stable;


create or replace function londiste._coordinate_copy(
    in i_queue_name text, in i_table_name text,
    out copy_role text, out copy_pos int4)
as $$
-- if the table is in middle of copy from multiple partitions,
-- the copy processes need coordination.
declare
    q_part1     text;
    q_part_ddl  text;
    n_parts     int4;
    n_done      int4;
    _table_name text;
    n_combined_queue text;
    merge_state text;
    dest_table  text;
    dropped_ddl text;
begin
    copy_pos := 0;
    copy_role := null;

    select t.merge_state, t.dest_table, t.dropped_ddl,
           min(case when t2.local then t2.queue_name else null end) as _queue1,
           min(case when t2.local and t2.dropped_ddl is not null then t2.queue_name else null end) as _queue1ddl,
           count(case when t2.local then t2.table_name else null end) as _total,
           count(case when t2.local then nullif(t2.merge_state, 'in-copy') else null end) as _done,
           min(n.combined_queue) as _combined_queue,
           count(nullif(t2.queue_name < i_queue_name and t.merge_state = 'in-copy' and t2.merge_state = 'in-copy', false)) as _copy_pos
        from londiste.table_info t
        join pgq_node.node_info n on (n.queue_name = t.queue_name)
        left join pgq_node.node_info n2 on (n2.combined_queue = n.combined_queue or
            (n2.combined_queue is null and n.combined_queue is null))
        left join londiste.table_info t2 on
           (coalesce(t2.dest_table, t2.table_name) = coalesce(t.dest_table, t.table_name) and
            t2.queue_name = n2.queue_name and
            (t2.merge_state is null or t2.merge_state != 'ok'))
        where t.queue_name = i_queue_name and t.table_name = i_table_name
        group by t.nr, t.table_name, t.local, t.merge_state, t.custom_snapshot, t.table_attrs, t.dropped_ddl, t.dest_table
        into merge_state, dest_table, dropped_ddl, q_part1, q_part_ddl, n_parts, n_done, n_combined_queue, copy_pos;

    -- q_part1, q_part_ddl, n_parts, n_done, n_combined_queue, copy_pos, dest_table

    -- be more robust against late joiners
    q_part1 := coalesce(q_part_ddl, q_part1);

    -- turn the logic off if no merge is happening
    if n_parts = 1 then
        q_part1 := null;
    end if;

    if q_part1 is not null then
        if i_queue_name = q_part1 then
            -- lead
            if merge_state = 'in-copy' then
                if dropped_ddl is null and n_done > 0 then
                    -- seems late addition, let it copy with indexes
                    copy_role := 'wait-replay';
                elsif n_done < n_parts then
                    -- show copy_role only if need to drop ddl or already did drop ddl
                    copy_role := 'lead';
                end if;

                -- make sure it cannot be made to wait
                copy_pos := 0;
            end if;
            if merge_state = 'catching-up' and dropped_ddl is not null then
                -- show copy_role only if need to wait for others
                if n_done < n_parts then
                    copy_role := 'wait-replay';
                end if;
            end if;
        else
            -- follow
            if merge_state = 'in-copy' then
                if q_part_ddl is not null then
                    -- can copy, wait in replay until lead has applied ddl
                    copy_role := 'wait-replay';
                elsif n_done > 0 then
                    -- ddl is not dropped, others are active, copy without touching ddl
                    copy_role := 'wait-replay';
                else
                    -- wait for lead to drop ddl
                    copy_role := 'wait-copy';
                end if;
            elsif merge_state = 'catching-up' then
                -- show copy_role only if need to wait for lead
                if q_part_ddl is not null then
                    copy_role := 'wait-replay';
                end if;
            end if;
        end if;
    end if;

    return;
end;
$$ language plpgsql strict stable;


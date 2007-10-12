create or replace function pgq.maint_rotate_tables_step1(i_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.maint_rotate_tables_step1(1)
--
--      Rotate tables for one queue.
--
-- Parameters:
--      i_queue_name        - Name of the queue
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    badcnt  integer;
    cf      record;
    nr      integer;
    tbl     text;
begin
    -- check if needed and load record
    select * from pgq.queue into cf
        where queue_name = i_queue_name
          and queue_rotation_period is not null
          and queue_switch_step2 is not null
          and queue_switch_time + queue_rotation_period < current_timestamp
        for update;
    if not found then
        return 0;
    end if;

    -- check if any consumer is on previous table
    select coalesce(count(*), 0) into badcnt
        from pgq.subscription, pgq.tick
        where txid_snapshot_xmin(tick_snapshot) < cf.queue_switch_step2
          and sub_queue = cf.queue_id
          and tick_queue = cf.queue_id
          and tick_id = (select tick_id from pgq.tick
                           where tick_id < sub_last_tick
                             and tick_queue = sub_queue
                           order by tick_queue desc, tick_id desc
                           limit 1);
    if badcnt > 0 then
        return 0;
    end if;

    -- all is fine, calc next table number
    nr := cf.queue_cur_table + 1;
    if nr = cf.queue_ntables then
        nr := 0;
    end if;
    tbl := cf.queue_data_pfx || '_' || nr;

    -- there may be long lock on the table from pg_dump,
    -- detect it and skip rotate then
    begin
        execute 'lock table ' || tbl || ' nowait';
        execute 'truncate ' || tbl;
    exception
        when lock_not_available then
            raise warning 'truncate of % failed, skipping rotate', tbl;
            return 0;
    end;

    -- remember the moment
    update pgq.queue
        set queue_cur_table = nr,
            queue_switch_time = current_timestamp,
            queue_switch_step1 = txid_current(),
            queue_switch_step2 = NULL
        where queue_id = cf.queue_id;

    -- clean ticks - avoid partial batches
    delete from pgq.tick
        where tick_queue = cf.queue_id
          and txid_snapshot_xmin(tick_snapshot) < cf.queue_switch_step2;

    return 1;
end;
$$ language plpgsql; -- need admin access

-- ----------------------------------------------------------------------
-- Function: pgq.maint_rotate_tables_step2(0)
--
--      It tag rotation as finished where needed.  It should be
--      called in separate transaction than pgq.maint_rotate_tables_step1()
-- ----------------------------------------------------------------------
create or replace function pgq.maint_rotate_tables_step2()
returns integer as $$
-- visibility tracking.  this should run in separate
-- tranaction than step1
begin
    update pgq.queue
       set queue_switch_step2 = txid_current()
     where queue_switch_step2 is null;
    return 1;
end;
$$ language plpgsql; -- need admin access


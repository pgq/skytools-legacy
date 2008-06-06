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
    badcnt      integer;
    cf          record;
    nr          integer;
    tbl         text;
    min_tick_id int8;
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

    -- load lowest tick for that queue
    select min(sub_last_tick) into min_tick_id
      from pgq.subscription
     where sub_queue = cf.queue_id;

    -- if some consumer exists
    if min_tick_id is not null then
        -- is the slowest one still on previous table?

        -- the '<=' is because at startup the tick and
        -- rotation happen in same tx
        perform 1 from pgq.tick
          where tick_queue = cf.queue_id
            and tick_id = min_tick_id
            and txid_snapshot_xmin(tick_snapshot) <= cf.queue_switch_step2;
        if found then
            return 0; -- skip rotation then
        end if;
    end if;

    -- all is fine, we can rotate
    
    -- calc next table number and name
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
            -- cannot truncate, skipping rotate
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





-- ----------------------------------------------------------------------
-- Section: Internal Tables
--
-- Overview:
--      pgq.queue                   - Queue configuration
--      pgq.consumer                - Consumer names
--      pgq.subscription            - Consumer registrations
--      pgq.tick                    - Per-queue snapshots (ticks)
--      pgq.event_*                 - Data tables
--      pgq.retry_queue             - Events to be retried later
--      pgq.failed_queue            - Events whose processing failed
--
-- Its basically generalized and simplified Slony-I structure:
--      sl_node                     - pgq.consumer
--      sl_set                      - pgq.queue
--      sl_subscriber + sl_confirm  - pgq.subscription
--      sl_event                    - pgq.tick
--      sl_setsync                  - pgq_ext.completed_*
--      sl_log_*                    - slony1 has per-cluster data tables,
--                                    pgq has per-queue data tables.
-- ----------------------------------------------------------------------

set client_min_messages = 'warning';
set default_with_oids = 'off';

-- drop schema if exists pgq cascade;
create schema pgq;

-- ----------------------------------------------------------------------
-- Table: pgq.consumer
--
--      Name to id lookup for consumers
--
-- Columns:
--      co_id       - consumer's id for internal usage
--      co_name     - consumer's id for external usage
-- ----------------------------------------------------------------------
create table pgq.consumer (
	co_id       serial,
	co_name     text        not null default 'fooz',

	constraint consumer_pkey primary key (co_id),
	constraint consumer_name_uq UNIQUE (co_name)
);


-- ----------------------------------------------------------------------
-- Table: pgq.queue
--
--     Information about available queues
--
-- Columns:
--      queue_id                    - queue id for internal usage
--      queue_name                  - queue name visible outside
--      queue_ntables               - how many data tables the queue has
--      queue_cur_table             - which data table is currently active
--      queue_rotation_period       - period for data table rotation
--      queue_switch_step1          - tx when rotation happened
--      queue_switch_step2          - tx after rotation was committed
--      queue_switch_time           - time when switch happened
--      queue_external_ticker       - ticks come from some external sources
--      queue_ticker_max_count      - batch should not contain more events
--      queue_ticker_max_lag        - events should not age more
--      queue_ticker_idle_period    - how often to tick when no events happen
--      queue_data_pfx              - prefix for data table names
--      queue_event_seq             - sequence for event id's
--      queue_tick_seq              - sequence for tick id's
-- ----------------------------------------------------------------------
create table pgq.queue (
	queue_id		    serial,
	queue_name		    text        not null,

        queue_ntables               integer     not null default 3,
        queue_cur_table             integer     not null default 0,
        queue_rotation_period       interval    not null default '2 hours',
        queue_switch_step1          bigint      not null default txid_current(),
        queue_switch_step2          bigint               default txid_current(),
        queue_switch_time           timestamptz not null default now(),

        queue_external_ticker       boolean     not null default false,
        queue_ticker_max_count      integer     not null default 500,
        queue_ticker_max_lag        interval    not null default '3 seconds',
        queue_ticker_idle_period    interval    not null default '1 minute',

        queue_data_pfx              text        not null,
        queue_event_seq             text        not null,
        queue_tick_seq              text        not null,

	constraint queue_pkey primary key (queue_id),
	constraint queue_name_uq unique (queue_name)
);

-- ----------------------------------------------------------------------
-- Table: pgq.tick
--
--      Snapshots for event batching
--
-- Columns:
--      tick_queue      - queue id whose tick it is
--      tick_id         - ticks id (per-queue)
--      tick_time       - time when tick happened
--      tick_snapshot   - transaction state
-- ----------------------------------------------------------------------
create table pgq.tick (
        tick_queue                  int4            not null,
        tick_id                     bigint          not null,
        tick_time                   timestamptz     not null default now(),
        tick_snapshot               txid_snapshot   not null default txid_current_snapshot(),

	constraint tick_pkey primary key (tick_queue, tick_id),
        constraint tick_queue_fkey foreign key (tick_queue)
                                   references pgq.queue (queue_id)
);

-- ----------------------------------------------------------------------
-- Sequence: pgq.batch_id_seq
--
--      Sequence for batch id's.
-- ----------------------------------------------------------------------
create sequence pgq.batch_id_seq;

-- ----------------------------------------------------------------------
-- Table: pgq.subscription
--
--      Consumer registration on a queue.
--
-- Columns:
--
--      sub_id          - subscription id for internal usage
--      sub_queue       - queue id
--      sub_consumer    - consumer's id
--      sub_last_tick   - last tick the consumer processed
--      sub_batch       - shortcut for queue_id/consumer_id/tick_id
--      sub_next_tick   - batch end pos
-- ----------------------------------------------------------------------
create table pgq.subscription (
	sub_id				serial      not null,
	sub_queue			int4        not null,
	sub_consumer			int4        not null,
	sub_last_tick                   bigint      not null,
        sub_active                      timestamptz not null default now(),
        sub_batch                       bigint,
        sub_next_tick                   bigint,

	constraint subscription_pkey primary key (sub_id),
	constraint subscription_ukey unique (sub_queue, sub_consumer),
        constraint sub_queue_fkey foreign key (sub_queue)
                                   references pgq.queue (queue_id),
        constraint sub_consumer_fkey foreign key (sub_consumer)
                                   references pgq.consumer (co_id)
);


-- ----------------------------------------------------------------------
-- Table: pgq.event_template
--
--      Parent table for all event tables
--
-- Columns:
--      ev_id               - event's id, supposed to be unique per queue
--      ev_time             - when the event was inserted
--      ev_txid             - transaction id which inserted the event
--      ev_owner            - subscription id that wanted to retry this
--      ev_retry            - how many times the event has been retried, NULL for new events
--      ev_type             - consumer/producer can specify what the data fields contain
--      ev_data             - data field
--      ev_extra1           - extra data field
--      ev_extra2           - extra data field
--      ev_extra3           - extra data field
--      ev_extra4           - extra data field
-- ----------------------------------------------------------------------
create table pgq.event_template (
	ev_id	            bigint          not null,
        ev_time             timestamptz     not null,

        ev_txid             bigint          not null default txid_current(),
        ev_owner            int4,
        ev_retry            int4,

        ev_type             text,
        ev_data             text,
        ev_extra1           text,
        ev_extra2           text,
        ev_extra3           text,
        ev_extra4           text
);

-- ----------------------------------------------------------------------
-- Table: pgq.retry_queue
--
--      Events to be retried.  When retry time reaches, they will
--      be put back into main queue.
--
-- Columns:
--      ev_retry_after          - time when it should be re-inserted to main queue
--      *                       - same as pgq.event_template
-- ----------------------------------------------------------------------
create table pgq.retry_queue (
    ev_retry_after          timestamptz     not null,

    like pgq.event_template,

    constraint rq_pkey primary key (ev_owner, ev_id),
    constraint rq_owner_fkey foreign key (ev_owner)
                             references pgq.subscription (sub_id)
);
alter table pgq.retry_queue alter column ev_owner set not null;
alter table pgq.retry_queue alter column ev_txid drop not null;
create index rq_retry_idx on pgq.retry_queue (ev_retry_after);
create index rq_retry_owner_idx on pgq.retry_queue (ev_owner, ev_id);

-- ----------------------------------------------------------------------
-- Table: pgq.failed_queue
--
--      Events whose processing failed.
--
-- Columns:
--      ev_failed_reason               - consumer's excuse for not processing
--      ev_failed_time                 - when it was tagged failed
--      *                              - same as pgq.event_template
-- ----------------------------------------------------------------------
create table pgq.failed_queue (
    ev_failed_reason                   text,
    ev_failed_time                     timestamptz not null,

    -- all event fields
    like pgq.event_template,

    constraint fq_pkey primary key (ev_owner, ev_id),
    constraint fq_owner_fkey foreign key (ev_owner)
                             references pgq.subscription (sub_id)
);
alter table pgq.failed_queue alter column ev_owner set not null;
alter table pgq.failed_queue alter column ev_txid drop not null;





grant usage on schema pgq to public;
grant select on table pgq.consumer to public;
grant select on table pgq.queue to public;
grant select on table pgq.tick to public;
grant select on table pgq.queue to public;
grant select on table pgq.subscription to public;
grant select on table pgq.event_template to public;
grant select on table pgq.retry_queue to public;
grant select on table pgq.failed_queue to public;



create type pgq.ret_queue_info as (
    queue_name                  text,
    queue_ntables               integer,
    queue_cur_table             integer,
    queue_rotation_period       interval,
    queue_switch_time           timestamptz,
    queue_external_ticker       boolean,
    queue_ticker_max_count      integer,
    queue_ticker_max_lag        interval,
    queue_ticker_idle_period    interval,
    ticker_lag                  interval
);

create type pgq.ret_consumer_info as (
    queue_name      text,
    consumer_name   text,
    lag             interval,
    last_seen       interval,
    last_tick       bigint,
    current_batch   bigint,
    next_tick       bigint
);

create type pgq.ret_batch_info as (
    queue_name      text,
    consumer_name   text,
    batch_start     timestamptz,
    batch_end       timestamptz,
    prev_tick_id    bigint,
    tick_id         bigint,
    lag             interval
);


create type pgq.ret_batch_event as (
    ev_id	    bigint,
    ev_time         timestamptz,

    ev_txid         bigint,
    ev_retry        int4,

    ev_type         text,
    ev_data         text,
    ev_extra1       text,
    ev_extra2       text,
    ev_extra3       text,
    ev_extra4       text
);



-- Section: Internal Functions

-- Group: Low-level event handling


create or replace function pgq.batch_event_sql(x_batch_id bigint)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.batch_event_sql(1)
--      Creates SELECT statement that fetches events for this batch.
--
-- Parameters:
--      x_batch_id    - ID of a active batch.
--
-- Returns:
--      SQL statement.
-- ----------------------------------------------------------------------

-- ----------------------------------------------------------------------
-- Algorithm description:
--      Given 2 snapshots, sn1 and sn2 with sn1 having xmin1, xmax1
--      and sn2 having xmin2, xmax2 create expression that filters
--      right txid's from event table.
--
--      Simplest solution would be
--      > WHERE ev_txid >= xmin1 AND ev_txid <= xmax2
--      >   AND NOT txid_visible_in_snapshot(ev_txid, sn1)
--      >   AND txid_visible_in_snapshot(ev_txid, sn2)
--
--      The simple solution has a problem with long transactions (xmin1 very low).
--      All the batches that happen when the long tx is active will need
--      to scan all events in that range.  Here is 2 optimizations used:
--
--      1)  Use [xmax1..xmax2] for range scan.  That limits the range to
--      txids that actually happened between two snapshots.  For txids
--      in the range [xmin1..xmax1] look which ones were actually
--      committed between snapshots and search for them using exact
--      values using IN (..) list.
--
--      2) As most TX are short, there could be lot of them that were
--      just below xmax1, but were committed before xmax2.  So look
--      if there are ID's near xmax1 and lower the range to include
--      them, thus decresing size of IN (..) list.
-- ----------------------------------------------------------------------
declare
    rec             record;
    sql             text;
    tbl             text;
    arr             text;
    part            text;
    select_fields   text;
    retry_expr      text;
    batch           record;
begin
    select s.sub_last_tick, s.sub_next_tick, s.sub_id, s.sub_queue,
           txid_snapshot_xmax(last.tick_snapshot) as tx_start,
           txid_snapshot_xmax(cur.tick_snapshot) as tx_end,
           last.tick_snapshot as last_snapshot,
           cur.tick_snapshot as cur_snapshot
        into batch
        from pgq.subscription s, pgq.tick last, pgq.tick cur
        where s.sub_batch = x_batch_id
          and last.tick_queue = s.sub_queue
          and last.tick_id = s.sub_last_tick
          and cur.tick_queue = s.sub_queue
          and cur.tick_id = s.sub_next_tick;
    if not found then
        raise exception 'batch not found';
    end if;

    -- load older transactions
    arr := '';
    for rec in
        -- active tx-es in prev_snapshot that were committed in cur_snapshot
        select id1 from
            txid_snapshot_xip(batch.last_snapshot) id1 left join
            txid_snapshot_xip(batch.cur_snapshot) id2 on (id1 = id2)
        where id2 is null
        order by 1 desc
    loop
        -- try to avoid big IN expression, so try to include nearby
        -- tx'es into range
        if batch.tx_start - 100 <= rec.id1 then
            batch.tx_start := rec.id1;
        else
            if arr = '' then
                arr := rec.id1;
            else
                arr := arr || ',' || rec.id1;
            end if;
        end if;
    end loop;

    -- must match pgq.event_template
    select_fields := 'select ev_id, ev_time, ev_txid, ev_retry, ev_type,'
        || ' ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4';
    retry_expr :=  ' and (ev_owner is null or ev_owner = '
        || batch.sub_id || ')';

    -- now generate query that goes over all potential tables
    sql := '';
    for rec in
        select xtbl from pgq.batch_event_tables(x_batch_id) xtbl
    loop
        tbl := rec.xtbl;
        -- this gets newer queries that definitely are not in prev_snapshot
        part := select_fields
            || ' from pgq.tick cur, pgq.tick last, ' || tbl || ' ev '
            || ' where cur.tick_id = ' || batch.sub_next_tick
            || ' and cur.tick_queue = ' || batch.sub_queue
            || ' and last.tick_id = ' || batch.sub_last_tick
            || ' and last.tick_queue = ' || batch.sub_queue
            || ' and ev.ev_txid >= ' || batch.tx_start
            || ' and ev.ev_txid <= ' || batch.tx_end
            || ' and txid_visible_in_snapshot(ev.ev_txid, cur.tick_snapshot)'
            || ' and not txid_visible_in_snapshot(ev.ev_txid, last.tick_snapshot)'
            || retry_expr;
        -- now include older tx-es, that were ongoing
        -- at the time of prev_snapshot
        if arr <> '' then
            part := part || ' union all '
                || select_fields || ' from ' || tbl || ' ev '
                || ' where ev.ev_txid in (' || arr || ')'
                || retry_expr;
        end if;
        if sql = '' then
            sql := part;
        else
            sql := sql || ' union all ' || part;
        end if;
    end loop;
    if sql = '' then
        raise exception 'could not construct sql for batch %', x_batch_id;
    end if;
    return sql || ' order by 1';
end;
$$ language plpgsql;  -- no perms needed



create or replace function pgq.batch_event_tables(x_batch_id bigint)
returns setof text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.batch_event_tables(1)
--
--     Returns set of table names where this batch events may reside.
--
-- Parameters:
--     x_batch_id    - ID of a active batch.
-- ----------------------------------------------------------------------
declare
    nr                    integer;
    tbl                   text;
    use_prev              integer;
    use_next              integer;
    batch                 record;
begin
    select
           txid_snapshot_xmin(last.tick_snapshot) as tx_min, -- absolute minimum
           txid_snapshot_xmax(cur.tick_snapshot) as tx_max, -- absolute maximum
           q.queue_data_pfx, q.queue_ntables,
           q.queue_cur_table, q.queue_switch_step1, q.queue_switch_step2
        into batch
        from pgq.tick last, pgq.tick cur, pgq.subscription s, pgq.queue q
        where cur.tick_id = s.sub_next_tick
          and cur.tick_queue = s.sub_queue
          and last.tick_id = s.sub_last_tick
          and last.tick_queue = s.sub_queue
          and s.sub_batch = x_batch_id
          and q.queue_id = s.sub_queue;
    if not found then
        raise exception 'Cannot find data for batch %', x_batch_id;
    end if;

    -- if its definitely not in one or other, look into both
    if batch.tx_max < batch.queue_switch_step1 then
        use_prev := 1;
        use_next := 0;
    elsif batch.queue_switch_step2 is not null
      and (batch.tx_min > batch.queue_switch_step2)
    then
        use_prev := 0;
        use_next := 1;
    else
        use_prev := 1;
        use_next := 1;
    end if;

    if use_prev then
        nr := batch.queue_cur_table - 1;
        if nr < 0 then
            nr := batch.queue_ntables - 1;
        end if;
        tbl := batch.queue_data_pfx || '_' || nr;
        return next tbl;
    end if;

    if use_next then
        tbl := batch.queue_data_pfx || '_' || batch.queue_cur_table;
        return next tbl;
    end if;

    return;
end;
$$ language plpgsql; -- no perms needed




create or replace function pgq.event_retry_raw(
    x_queue text,
    x_consumer text,
    x_retry_after timestamptz,
    x_ev_id bigint,
    x_ev_time timestamptz,
    x_ev_retry integer,
    x_ev_type text,
    x_ev_data text,
    x_ev_extra1 text,
    x_ev_extra2 text,
    x_ev_extra3 text,
    x_ev_extra4 text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.event_retry_raw(12)
--
--      Allows full control over what goes to retry queue.
--
-- Parameters:
--      x_queue         - name of the queue
--      x_consumer      - name of the consumer
--      x_retry_after   - when the event should be processed again
--      x_ev_id         - event id
--      x_ev_time       - creation time
--      x_ev_retry      - retry count
--      x_ev_type       - user data
--      x_ev_data       - user data
--      x_ev_extra1     - user data
--      x_ev_extra2     - user data
--      x_ev_extra3     - user data
--      x_ev_extra4     - user data
--
-- Returns:
--      Event ID.
-- ----------------------------------------------------------------------
declare
    q record;
    id bigint;
begin
    select sub_id, queue_event_seq into q
      from pgq.consumer, pgq.queue, pgq.subscription
     where queue_name = x_queue
       and co_name = x_consumer
       and sub_consumer = co_id
       and sub_queue = queue_id;
    if not found then
        raise exception 'consumer not registered';
    end if;

    id := x_ev_id;
    if id is null then
        id := nextval(q.queue_event_seq);
    end if;

    insert into pgq.retry_queue (ev_retry_after,
            ev_id, ev_time, ev_owner, ev_retry,
            ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    values (x_retry_after, x_ev_id, x_ev_time, q.sub_id, x_ev_retry,
            x_ev_type, x_ev_data, x_ev_extra1, x_ev_extra2,
            x_ev_extra3, x_ev_extra4);

    return id;
end;
$$ language plpgsql security definer;



-- \i functions/pgq.insert_event_raw.sql


-- ----------------------------------------------------------------------
-- Function: pgq.insert_event_raw(11)
--
--      Actual event insertion.  Used also by retry queue maintenance.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_id           - Event ID.  If NULL, will be taken from seq.
--      ev_time         - Event creation time.
--      ev_owner        - Subscription ID when retry event. If NULL, the event is for everybody.
--      ev_retry        - Retry count. NULL for first-time events.
--      ev_type         - user data
--      ev_data         - user data
--      ev_extra1       - user data
--      ev_extra2       - user data
--      ev_extra3       - user data
--      ev_extra4       - user data
--
-- Returns:
--      Event ID.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pgq.insert_event_raw(
    queue_name text, ev_id bigint, ev_time timestamptz,
    ev_owner integer, ev_retry integer, ev_type text, ev_data text,
    ev_extra1 text, ev_extra2 text, ev_extra3 text, ev_extra4 text)
RETURNS int8 AS '$libdir/pgq_lowlevel', 'pgq_insert_event_raw' LANGUAGE C;



-- Group: Ticker


create or replace function pgq.ticker(i_queue_name text, i_tick_id bigint)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.ticker(2)
--
--     Insert a tick with a particular tick_id.
--
--     For external tickers.
--
-- Parameters:
--     i_queue_name     - Name of the queue
--     i_tick_id        - Id of new tick.
--
-- Returns:
--     Tick id.
-- ----------------------------------------------------------------------
begin
    insert into pgq.tick (tick_queue, tick_id)
    select queue_id, i_tick_id
        from pgq.queue
        where queue_name = i_queue_name
          and queue_external_ticker;
    if not found then
        raise exception 'queue not found';
    end if;
    return i_tick_id;
end;
$$ language plpgsql security definer; -- unsure about access

create or replace function pgq.ticker(i_queue_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.ticker(1)
--
--     Insert a tick with a tick_id from sequence.
--
--     For pgqadm usage.
--
-- Parameters:
--     i_queue_name     - Name of the queue
--
-- Returns:
--     Tick id.
-- ----------------------------------------------------------------------
declare
    res bigint;
    ext boolean;
    seq text;
    q record;
begin
    select queue_id, queue_tick_seq, queue_external_ticker into q
        from pgq.queue where queue_name = i_queue_name;
    if not found then
        raise exception 'no such queue';
    end if;

    if q.queue_external_ticker then
        raise exception 'This queue has external tick source.';
    end if;

    insert into pgq.tick (tick_queue, tick_id)
        values (q.queue_id, nextval(q.queue_tick_seq));

    res = currval(q.queue_tick_seq);
    return res;
end;
$$ language plpgsql security definer; -- unsure about access

create or replace function pgq.ticker() returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.ticker(0)
--
--     Creates ticks for all queues which dont have external ticker.
--
-- Returns:
--     Number of queues that were processed.
-- ----------------------------------------------------------------------
declare
    res bigint;
begin
    select count(pgq.ticker(queue_name)) into res 
        from pgq.queue where not queue_external_ticker;
    return res;
end;
$$ language plpgsql security definer;



-- Group: Periodic maintenence


create or replace function pgq.maint_retry_events()
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.maint_retry_events(0)
--
--      Moves retry events back to main queue.
--
--      It moves small amount at a time.  It should be called
--      until it returns 0
--
-- Returns:
--      Number of events processed.
-- ----------------------------------------------------------------------
declare
    cnt    integer;
    rec    record;
begin
    cnt := 0;

    -- allow only single event mover at a time, without affecting inserts
    lock table pgq.retry_queue in share update exclusive mode;

    for rec in
        select queue_name,
               ev_id, ev_time, ev_owner, ev_retry, ev_type, ev_data,
               ev_extra1, ev_extra2, ev_extra3, ev_extra4
          from pgq.retry_queue, pgq.queue, pgq.subscription
         where ev_retry_after <= current_timestamp
           and sub_id = ev_owner
           and queue_id = sub_queue
         order by ev_retry_after
         limit 10
    loop
        cnt := cnt + 1;
        perform pgq.insert_event_raw(rec.queue_name,
                    rec.ev_id, rec.ev_time, rec.ev_owner, rec.ev_retry,
                    rec.ev_type, rec.ev_data, rec.ev_extra1, rec.ev_extra2,
                    rec.ev_extra3, rec.ev_extra4);
        delete from pgq.retry_queue
         where ev_owner = rec.ev_owner
           and ev_id = rec.ev_id;
    end loop;
    return cnt;
end;
$$ language plpgsql; -- need admin access



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
--      1 if rotation happened, otherwise 0.
-- ----------------------------------------------------------------------
declare
    badcnt          integer;
    cf              record;
    nr              integer;
    tbl             text;
    lowest_tick_id  int8;
    lowest_xmin     int8;
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

    -- find lowest tick for that queue
    select min(sub_last_tick) into lowest_tick_id
      from pgq.subscription
     where sub_queue = cf.queue_id;

    -- if some consumer exists
    if lowest_tick_id is not null then
        -- is the slowest one still on previous table?
        select txid_snapshot_xmin(tick_snapshot) into lowest_xmin
          from pgq.tick
         where tick_queue = cf.queue_id
           and tick_id = lowest_tick_id;
        if lowest_xmin <= cf.queue_switch_step2 then
            return 0; -- skip rotation then
        end if;
    end if;

    -- nobody on previous table, we can rotate
    
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

    -- Clean ticks by using step2 txid from previous rotation.
    -- That should keep all ticks for all batches that are completely
    -- in old table.  This keeps them for longer than needed, but:
    -- 1. we want the pgq.tick table to be big, to avoid Postgres
    --    accitentally switching to seqscans on that.
    -- 2. that way we guarantee to consumers that they an be moved
    --    back on the queue at least for one rotation_period.
    --    (may help in disaster recovery)
    delete from pgq.tick
        where tick_queue = cf.queue_id
          and txid_snapshot_xmin(tick_snapshot) < cf.queue_switch_step2;

    return 1;
end;
$$ language plpgsql; -- need admin access


create or replace function pgq.maint_rotate_tables_step2()
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.maint_rotate_tables_step2(0)
--
--      Stores the txid when the rotation was visible.  It should be
--      called in separate transaction than pgq.maint_rotate_tables_step1()
-- ----------------------------------------------------------------------
begin
    update pgq.queue
       set queue_switch_step2 = txid_current()
     where queue_switch_step2 is null;
    return 1;
end;
$$ language plpgsql; -- need admin access



create or replace function pgq.maint_tables_to_vacuum()
returns setof text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.maint_tables_to_vacuum(0)
--
--      Returns list of tablenames that need frequent vacuuming.
--
--      The goal is to avoid hardcoding them into maintenance process.
--
-- Returns:
--      List of table names.
-- ----------------------------------------------------------------------
declare
    row record;
begin
    return next 'pgq.subscription';
    return next 'pgq.consumer';
    return next 'pgq.queue';
    return next 'pgq.tick';
    return next 'pgq.retry_queue';

    -- include also txid, pgq_ext and londiste tables if they exist
    for row in
        select n.nspname as scm, t.relname as tbl
          from pg_class t, pg_namespace n
         where n.oid = t.relnamespace
           and n.nspname = 'txid' and t.relname = 'epoch'
        union all
        select n.nspname as scm, t.relname as tbl
          from pg_class t, pg_namespace n
         where n.oid = t.relnamespace
           and n.nspname = 'londiste' and t.relname = 'completed'
        union all
        select n.nspname as scm, t.relname as tbl
          from pg_class t, pg_namespace n
         where n.oid = t.relnamespace
           and n.nspname = 'pgq_ext'
           and t.relname in ('completed_tick', 'completed_batch', 'completed_event', 'partial_batch')
    loop
        return next row.scm || '.' || row.tbl;
    end loop;

    return;
end;
$$ language plpgsql;




-- Group: Random utility functions


create or replace function pgq.grant_perms(x_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.grant_perms(1)
--
--      Make event tables readable by public.
--
-- Parameters:
--      x_queue_name        - Name of the queue.
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    q           record;
    i           integer;
    tbl_perms   text;
    seq_perms   text;
begin
    select * from pgq.queue into q
        where queue_name = x_queue_name;
    if not found then
        raise exception 'Queue not found';
    end if;

    if true then
        -- safe, all access must go via functions
        seq_perms := 'select';
        tbl_perms := 'select';
    else
        -- allow ordinery users to directly insert
        -- to event tables.  dangerous.
        seq_perms := 'select, update';
        tbl_perms := 'select, insert';
    end if;

    -- tick seq, normal users don't need to modify it
    execute 'grant ' || seq_perms
        || ' on ' || q.queue_tick_seq || ' to public';

    -- event seq
    execute 'grant ' || seq_perms
        || ' on ' || q.queue_event_seq || ' to public';
    
    -- parent table for events
    execute 'grant select on ' || q.queue_data_pfx || ' to public';

    -- real event tables
    for i in 0 .. q.queue_ntables - 1 loop
        execute 'grant ' || tbl_perms
            || ' on ' || q.queue_data_pfx || '_' || i
            || ' to public';
    end loop;

    return 1;
end;
$$ language plpgsql security definer;





create or replace function pgq.force_tick(i_queue_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.force_tick(2)
--
--      Simulate lots of events happening to force ticker to tick.
--
--      Should be called in loop, with some delay until last tick
--      changes or too much time is passed.
--
--      Such function is needed because paraller calls of pgq.ticker() are
--      dangerous, and cannot be protected with locks as snapshot
--      is taken before locking.
--
-- Parameters:
--      i_queue_name     - Name of the queue
--
-- Returns:
--      Currently last tick id.
-- ----------------------------------------------------------------------
declare
    q  record;
    t  record;
begin
    -- bump seq and get queue id
    select queue_id,
           setval(queue_event_seq, nextval(queue_event_seq)
                                   + queue_ticker_max_count * 2) as tmp
      into q from pgq.queue
     where queue_name = i_queue_name
       and not queue_external_ticker;
    if not found then
        raise exception 'queue not found or ticks not allowed';
    end if;

    -- return last tick id
    select tick_id into t from pgq.tick
     where tick_queue = q.queue_id
     order by tick_queue desc, tick_id desc limit 1;

    return t.tick_id;
end;
$$ language plpgsql security definer;





-- Section: Public Functions

-- Group: Queue creation


create or replace function pgq.create_queue(i_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.create_queue(1)
--
--      Creates new queue with given name.
--
-- Returns:
--      0 - queue already exists
--      1 - queue created
-- ----------------------------------------------------------------------
declare
    tblpfx   text;
    tblname  text;
    idxpfx   text;
    idxname  text;
    sql      text;
    id       integer;
    tick_seq text;
    ev_seq text;
    n_tables integer;
begin
    if i_queue_name is null then
        raise exception 'Invalid NULL value';
    end if;

    -- check if exists
    perform 1 from pgq.queue where queue_name = i_queue_name;
    if found then
        return 0;
    end if;

    -- insert event
    id := nextval('pgq.queue_queue_id_seq');
    tblpfx := 'pgq.event_' || id;
    idxpfx := 'event_' || id;
    tick_seq := 'pgq.event_' || id || '_tick_seq';
    ev_seq := 'pgq.event_' || id || '_id_seq';
    insert into pgq.queue (queue_id, queue_name,
            queue_data_pfx, queue_event_seq, queue_tick_seq)
        values (id, i_queue_name, tblpfx, ev_seq, tick_seq);

    select queue_ntables into n_tables from pgq.queue
        where queue_id = id;

    -- create seqs
    execute 'CREATE SEQUENCE ' || tick_seq;
    execute 'CREATE SEQUENCE ' || ev_seq;

    -- create data tables
    execute 'CREATE TABLE ' || tblpfx || ' () '
            || ' INHERITS (pgq.event_template)';
    for i in 0 .. (n_tables - 1) loop
        tblname := tblpfx || '_' || i;
        idxname := idxpfx || '_' || i;
        execute 'CREATE TABLE ' || tblname || ' () '
                || ' INHERITS (' || tblpfx || ')';
        execute 'ALTER TABLE ' || tblname || ' ALTER COLUMN ev_id '
                || ' SET DEFAULT nextval(' || quote_literal(ev_seq) || ')';
        execute 'create index ' || idxname || '_txid_idx on '
                || tblname || ' (ev_txid)';
    end loop;

    perform pgq.grant_perms(i_queue_name);

    perform pgq.ticker(i_queue_name);

    return 1;
end;
$$ language plpgsql security definer;



create or replace function pgq.drop_queue(x_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.drop_queue(1)
--
--     Drop queue and all associated tables.
--     No consumers must be listening on the queue.
--
-- ----------------------------------------------------------------------
declare
    tblname  text;
    q record;
    num integer;
begin
    -- check ares
    if x_queue_name is null then
        raise exception 'Invalid NULL value';
    end if;

    -- check if exists
    select * into q from pgq.queue
        where queue_name = x_queue_name;
    if not found then
        raise exception 'No such event queue';
    end if;

    -- check if no consumers
    select count(*) into num from pgq.subscription
        where sub_queue = q.queue_id;
    if num > 0 then
        raise exception 'cannot drop queue, consumers still attached';
    end if;

    -- drop data tables
    for i in 0 .. (q.queue_ntables - 1) loop
        tblname := q.queue_data_pfx || '_' || i;
        execute 'DROP TABLE ' || tblname;
    end loop;
    execute 'DROP TABLE ' || q.queue_data_pfx;

    -- delete ticks
    delete from pgq.tick where tick_queue = q.queue_id;

    -- drop seqs
    -- FIXME: any checks needed here?
    execute 'DROP SEQUENCE ' || q.queue_tick_seq;
    execute 'DROP SEQUENCE ' || q.queue_event_seq;

    -- delete event
    delete from pgq.queue
        where queue_name = x_queue_name;

    return 1;
end;
$$ language plpgsql security definer;



-- Group: Event publishing


create or replace function pgq.insert_event(queue_name text, ev_type text, ev_data text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.insert_event(3)
--
--      Insert a event into queue.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_type         - User-specified type for the event
--      ev_data         - User data for the event
--
-- Returns:
--      Event ID
-- ----------------------------------------------------------------------
begin
    return pgq.insert_event(queue_name, ev_type, ev_data, null, null, null, null);
end;
$$ language plpgsql security definer;



create or replace function pgq.insert_event(
    queue_name text, ev_type text, ev_data text,
    ev_extra1 text, ev_extra2 text, ev_extra3 text, ev_extra4 text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.insert_event(7)
--
--      Insert a event into queue with all the extra fields.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_type         - User-specified type for the event
--      ev_data         - User data for the event
--      ev_extra1       - Extra data field for the event
--      ev_extra2       - Extra data field for the event
--      ev_extra3       - Extra data field for the event
--      ev_extra4       - Extra data field for the event
--
-- Returns:
--      Event ID
-- ----------------------------------------------------------------------
begin
    return pgq.insert_event_raw(queue_name, null, now(), null, null,
            ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4);
end;
$$ language plpgsql security definer;



create or replace function pgq.current_event_table(x_queue_name text)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.current_event_table(1)
--
--      Return active event table for particular queue.
--      Event can be added to it without going via functions,
--      e.g. by COPY.
--
-- Note:
--      The result is valid only during current transaction.
--
-- Permissions:
--      Actual insertion requires superuser access.
--
-- Parameters:
--      x_queue_name    - Queue name.
-- ----------------------------------------------------------------------
declare
    res text;
begin
    select queue_data_pfx || '_' || queue_cur_table into res
        from pgq.queue where queue_name = x_queue_name;
    if not found then
        raise exception 'Event queue not found';
    end if;
    return res;
end;
$$ language plpgsql; -- no perms needed



-- Group: Subscribing to queue


create or replace function pgq.register_consumer(
    x_queue_name text,
    x_consumer_id text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.register_consumer(2)
--
--      Subscribe consumer on a queue.
--
--      From this moment forward, consumer will see all events in the queue.
--
-- Parameters:
--      x_queue_name        - Name of queue
--      x_consumer_name     - Name of consumer
--
-- Returns:
--      0  - if already registered
--      1  - if new registration
-- ----------------------------------------------------------------------
begin
    return pgq.register_consumer(x_queue_name, x_consumer_id, NULL);
end;
$$ language plpgsql security definer;


create or replace function pgq.register_consumer(
    x_queue_name text,
    x_consumer_name text,
    x_tick_pos bigint)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.register_consumer(3)
--
--      Extended registration, allows to specify tick_id.
--
-- Note:
--      For usage in special situations.
--
-- Parameters:
--      x_queue_name        - Name of a queue
--      x_consumer_name     - Name of consumer
--      x_tick_pos          - Tick ID
--
-- Returns:
--      0/1 whether consumer has already registered.
-- ----------------------------------------------------------------------
declare
    tmp         text;
    last_tick   bigint;
    x_queue_id          integer;
    x_consumer_id integer;
    queue integer;
    sub record;
begin
    select queue_id into x_queue_id from pgq.queue
        where queue_name = x_queue_name;
    if not found then
        raise exception 'Event queue not created yet';
    end if;

    -- get consumer and create if new
    select co_id into x_consumer_id from pgq.consumer
        where co_name = x_consumer_name;
    if not found then
        insert into pgq.consumer (co_name) values (x_consumer_name);
        x_consumer_id := currval('pgq.consumer_co_id_seq');
    end if;

    -- if particular tick was requested, check if it exists
    if x_tick_pos is not null then
        perform 1 from pgq.tick
            where tick_queue = x_queue_id
              and tick_id = x_tick_pos;
        if not found then
            raise exception 'cannot reposition, tick not found: %', x_tick_pos;
        end if;
    end if;

    -- check if already registered
    select sub_last_tick, sub_batch into sub
        from pgq.subscription
        where sub_consumer = x_consumer_id
          and sub_queue  = x_queue_id;
    if found then
        if x_tick_pos is not null then
            if sub.sub_batch is not null then
                raise exception 'reposition while active not allowed';
            end if;
            -- update tick pos if requested
            update pgq.subscription
                set sub_last_tick = x_tick_pos
                where sub_consumer = x_consumer_id
                  and sub_queue = x_queue_id;
        end if;
        -- already registered
        return 0;
    end if;

    --  new registration
    if x_tick_pos is null then
        -- start from current tick
        select tick_id into last_tick from pgq.tick
            where tick_queue = x_queue_id
            order by tick_queue desc, tick_id desc
            limit 1;
        if not found then
            raise exception 'No ticks for this queue.  Please run ticker on database.';
        end if;
    else
        last_tick := x_tick_pos;
    end if;

    -- register
    insert into pgq.subscription (sub_queue, sub_consumer, sub_last_tick)
        values (x_queue_id, x_consumer_id, last_tick);
    return 1;
end;
$$ language plpgsql security definer;





create or replace function pgq.unregister_consumer(
    x_queue_name text,
    x_consumer_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.unregister_consumer(2)
--
--      Unsubscriber consumer from the queue.  Also consumer's failed
--      and retry events are deleted.
--
-- Parameters:
--      x_queue_name        - Name of the queue
--      x_consumer_name     - Name of the consumer
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    x_sub_id integer;
begin
    select sub_id into x_sub_id
        from pgq.subscription, pgq.consumer, pgq.queue
        where sub_queue = queue_id
          and sub_consumer = co_id
          and queue_name = x_queue_name
          and co_name = x_consumer_name;
    if not found then
        raise exception 'consumer not registered on queue';
    end if;

    delete from pgq.retry_queue
        where ev_owner = x_sub_id;

    delete from pgq.failed_queue
        where ev_owner = x_sub_id;

    delete from pgq.subscription
        where sub_id = x_sub_id;

    return 1;
end;
$$ language plpgsql security definer;



-- Group: Batch processing


create or replace function pgq.next_batch(x_queue_name text, x_consumer_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.next_batch(2)
--
--      Makes next block of events active.
--
--      If it returns NULL, there is no events available in queue.
--      Consumer should sleep a bith then.
--
-- Parameters:
--      x_queue_name        - Name of the queue
--      x_consumer_name     - Name of the consumer
--
-- Returns:
--      Batch ID or NULL if there are no more events available.
-- ----------------------------------------------------------------------
declare
    next_tick       bigint;
    batch_id        bigint;
    errmsg          text;
    sub             record;
begin
    select sub_queue, sub_consumer, sub_id, sub_last_tick, sub_batch into sub
        from pgq.queue q, pgq.consumer c, pgq.subscription s
        where q.queue_name = x_queue_name
          and c.co_name = x_consumer_name
          and s.sub_queue = q.queue_id
          and s.sub_consumer = c.co_id;
    if not found then
        errmsg := 'Not subscriber to queue: '
            || coalesce(x_queue_name, 'NULL')
            || '/'
            || coalesce(x_consumer_name, 'NULL');
        raise exception '%', errmsg;
    end if;

    -- has already active batch
    if sub.sub_batch is not null then
        return sub.sub_batch;
    end if;

    -- find next tick
    select tick_id into next_tick
        from pgq.tick
        where tick_id > sub.sub_last_tick
          and tick_queue = sub.sub_queue
        order by tick_queue asc, tick_id asc
        limit 1;
    if not found then
        -- nothing to do
        return null;
    end if;

    -- get next batch
    batch_id := nextval('pgq.batch_id_seq');
    update pgq.subscription
        set sub_batch = batch_id,
            sub_next_tick = next_tick,
            sub_active = now()
        where sub_queue = sub.sub_queue
          and sub_consumer = sub.sub_consumer;
    return batch_id;
end;
$$ language plpgsql security definer;




create or replace function pgq.get_batch_events(x_batch_id bigint)
returns setof pgq.ret_batch_event as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.get_batch_events(1)
--
--      Get all events in batch.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--
-- Returns:
--      List of events.
-- ----------------------------------------------------------------------
declare 
    rec pgq.ret_batch_event%rowtype; 
    sql text; 
begin 
    sql := pgq.batch_event_sql(x_batch_id); 
    for rec in execute sql loop
        return next rec; 
    end loop; 
    return;
end; 
$$ language plpgsql; -- no perms needed




create or replace function pgq.event_failed(
    x_batch_id bigint,
    x_event_id bigint,
    x_reason text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.event_failed(3)
--
--      Copies the event to failed queue so it can be looked at later.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_event_id      - Event id
--      x_reason        - Text to associate with event.
--
-- Returns:
--     0 if event was already in queue, 1 otherwise.
-- ----------------------------------------------------------------------
begin
    insert into pgq.failed_queue (ev_failed_reason, ev_failed_time,
        ev_id, ev_time, ev_txid, ev_owner, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    select x_reason, now(),
           ev_id, ev_time, NULL, sub_id, coalesce(ev_retry, 0),
           ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4
      from pgq.get_batch_events(x_batch_id),
           pgq.subscription
     where sub_batch = x_batch_id
       and ev_id = x_event_id;
    if not found then
        raise exception 'event not found';
    end if;
    return 1;

-- dont worry if the event is already in queue
exception
    when unique_violation then
        return 0;
end;
$$ language plpgsql security definer;



create or replace function pgq.event_retry(
    x_batch_id bigint,
    x_event_id bigint,
    x_retry_time timestamptz)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.event_retry(3)
--
--     Put the event into retry queue, to be processed again later.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_event_id      - event id
--      x_retry_time    - Time when the event should be put back into queue
--
-- Returns:
--     nothing
-- ----------------------------------------------------------------------
begin
    insert into pgq.retry_queue (ev_retry_after,
        ev_id, ev_time, ev_txid, ev_owner, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    select x_retry_time,
           ev_id, ev_time, NULL, sub_id, coalesce(ev_retry, 0) + 1,
           ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4
      from pgq.get_batch_events(x_batch_id),
           pgq.subscription
     where sub_batch = x_batch_id
       and ev_id = x_event_id;
    if not found then
        raise exception 'event not found';
    end if;
    return 1;

-- dont worry if the event is already in queue
exception
    when unique_violation then
        return 0;
end;
$$ language plpgsql security definer;


create or replace function pgq.event_retry(
    x_batch_id bigint,
    x_event_id bigint,
    x_retry_seconds integer)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.event_retry(3)
--
--     Put the event into retry queue, to be processed later again.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_event_id      - event id
--      x_retry_seconds - Time when the event should be put back into queue
--
-- Returns:
--     nothing
-- ----------------------------------------------------------------------
declare
    new_retry  timestamptz;
begin
    new_retry := current_timestamp + ((x_retry_seconds || ' seconds')::interval);
    return pgq.event_retry(x_batch_id, x_event_id, new_retry);
end;
$$ language plpgsql security definer;




create or replace function pgq.finish_batch(
    x_batch_id bigint)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.finish_batch(1)
--
--      Closes a batch.  No more operations can be done with events
--      of this batch.
--
-- Parameters:
--      x_batch_id      - id of batch.
--
-- Returns:
--      If batch 1 if batch was found, 0 otherwise.
-- ----------------------------------------------------------------------
begin
    update pgq.subscription
        set sub_active = now(),
            sub_last_tick = sub_next_tick,
            sub_next_tick = null,
            sub_batch = null
        where sub_batch = x_batch_id;
    if not found then
        raise warning 'finish_batch: batch % not found', x_batch_id;
        return 0;
    end if;

    return 1;
end;
$$ language plpgsql security definer;



-- Group: General info functions


create or replace function pgq.get_queue_info()
returns setof pgq.ret_queue_info as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_queue_info(0)
--
--      Get info about all queues.
--
-- Returns:
--      List of pgq.ret_queue_info records.
-- ----------------------------------------------------------------------
declare
    q     record;
    ret   pgq.ret_queue_info%rowtype;
begin
    for q in
        select queue_name from pgq.queue order by 1
    loop
        select * into ret from pgq.get_queue_info(q.queue_name);
        return next ret;
    end loop;
    return;
end;
$$ language plpgsql security definer;

create or replace function pgq.get_queue_info(qname text)
returns pgq.ret_queue_info as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_queue_info(1)
--
--      Get info about particular queue.
--
-- Returns:
--      One pgq.ret_queue_info record.
-- ----------------------------------------------------------------------
declare
    ret   pgq.ret_queue_info%rowtype;
begin
    select queue_name, queue_ntables, queue_cur_table,
           queue_rotation_period, queue_switch_time,
           queue_external_ticker,
           queue_ticker_max_count, queue_ticker_max_lag,
           queue_ticker_idle_period,
           (select current_timestamp - tick_time
              from pgq.tick where tick_queue = queue_id
             order by tick_queue desc, tick_id desc limit 1
            ) as ticker_lag
      into ret from pgq.queue where queue_name = qname;
    return ret;
end;
$$ language plpgsql security definer;




-------------------------------------------------------------------------
create or replace function pgq.get_consumer_info()
returns setof pgq.ret_consumer_info as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_consumer_info(0)
--
--      Returns info about all consumers on all queues.
--
-- Returns:
--      See pgq.get_consumer_info(2)
-- ----------------------------------------------------------------------
declare
    ret  pgq.ret_consumer_info%rowtype;
    i    record;
begin
    for i in select queue_name from pgq.queue order by 1
    loop
        for ret in
            select * from pgq.get_consumer_info(i.queue_name)
        loop
            return next ret;
        end loop;
    end loop;
    return;
end;
$$ language plpgsql security definer;


-------------------------------------------------------------------------
create or replace function pgq.get_consumer_info(x_queue_name text)
returns setof pgq.ret_consumer_info as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_consumer_info(1)
--
--      Returns info about consumers on one particular queue.
--
-- Parameters:
--      x_queue_name    - Queue name
--
-- Returns:
--      See pgq.get_consumer_info(2)
-- ----------------------------------------------------------------------
declare
    ret  pgq.ret_consumer_info%rowtype;
    tmp record;
begin
    for tmp in
        select queue_name, co_name
          from pgq.queue, pgq.consumer, pgq.subscription
         where queue_id = sub_queue
           and co_id = sub_consumer
           and queue_name = x_queue_name
         order by 1, 2
    loop
        for ret in
            select * from pgq.get_consumer_info(tmp.queue_name, tmp.co_name)
        loop
            return next ret;
        end loop;
    end loop;
    return;
end;
$$ language plpgsql security definer;


------------------------------------------------------------------------
create or replace function pgq.get_consumer_info(
    x_queue_name text,
    x_consumer_name text)
returns setof pgq.ret_consumer_info as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_consumer_info(2)
--
--      Get info about particular consumer on particular queue.
--
-- Parameters:
--      x_queue_name        - name of a queue.
--      x_consumer_name     - name of a consumer
--
-- Returns:
--      queue_name          - Queue name
--      consumer_name       - Consumer name
--      lag                 - How old are events the consumer is processing
--      last_seen           - When the consumer seen by pgq
--      last_tick           - Tick ID of last processed tick
--      current_batch       - Current batch ID, if one is active or NULL
--      next_tick           - If batch is active, then its final tick.
-- ----------------------------------------------------------------------
declare
    ret  pgq.ret_consumer_info%rowtype;
begin
    for ret in 
        select queue_name, co_name,
               current_timestamp - tick_time as lag,
               current_timestamp - sub_active as last_seen,
               sub_last_tick as last_tick,
               sub_batch as current_batch,
               sub_next_tick as next_tick
          from pgq.subscription, pgq.tick, pgq.queue, pgq.consumer
         where tick_id = sub_last_tick
           and queue_id = sub_queue
           and tick_queue = sub_queue
           and co_id = sub_consumer
           and queue_name = x_queue_name
           and co_name = x_consumer_name
         order by 1,2
    loop
        return next ret;
    end loop;
    return;
end;
$$ language plpgsql security definer;



create or replace function pgq.version()
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.version(0)
--
--      Returns verison string for pgq.  ATM its SkyTools version
--      that is only bumped when PGQ database code changes.
-- ----------------------------------------------------------------------
begin
    return '2.1.8';
end;
$$ language plpgsql;




create or replace function pgq.get_batch_info(x_batch_id bigint)
returns pgq.ret_batch_info as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_batch_info(1)
--
--      Returns detailed info about a batch.
--
-- Parameters:
--      x_batch_id      - id of a active batch.
--
-- Returns:
--      Info
-- ----------------------------------------------------------------------
declare
    ret  pgq.ret_batch_info%rowtype;
begin
    select queue_name, co_name,
           prev.tick_time as batch_start,
           cur.tick_time as batch_end,
           sub_last_tick, sub_next_tick,
           current_timestamp - cur.tick_time as lag
        into ret
        from pgq.subscription, pgq.tick cur, pgq.tick prev,
             pgq.queue, pgq.consumer
        where sub_batch = x_batch_id
          and prev.tick_id = sub_last_tick
          and prev.tick_queue = sub_queue
          and cur.tick_id = sub_next_tick
          and cur.tick_queue = sub_queue
          and queue_id = sub_queue
          and co_id = sub_consumer;
    return ret;
end;
$$ language plpgsql security definer;



-- Group: Failed queue browsing



create or replace function pgq.failed_event_list(
    x_queue_name text,
    x_consumer_name text)
returns setof pgq.failed_queue as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.failed_event_list(2)
--
--      Get list of all failed events for one consumer.
--
-- Parameters:
--      x_queue_name        - Queue name
--      x_consumer_name     - Consumer name
--
-- Returns:
--      List of failed events.
-- ----------------------------------------------------------------------
declare 
    rec pgq.failed_queue%rowtype; 
begin 
    for rec in
        select fq.*
          from pgq.failed_queue fq, pgq.consumer,
               pgq.queue, pgq.subscription
         where queue_name = x_queue_name
           and co_name = x_consumer_name
           and sub_consumer = co_id
           and sub_queue = queue_id
           and ev_owner = sub_id
        order by ev_id
    loop
        return next rec; 
    end loop; 
    return;
end; 
$$ language plpgsql security definer;

create or replace function pgq.failed_event_list(
    x_queue_name text,
    x_consumer_name text,
    x_count integer,
    x_offset integer)
returns setof pgq.failed_queue as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.failed_event_list(4)
--
--      Get list of failed events, from offset and specific count.
--
-- Parameters:
--      x_queue_name        - Queue name
--      x_consumer_name     - Consumer name
--      x_count             - Max amount of events to fetch
--      x_offset            - From this offset
--
-- Returns:
--      List of failed events.
-- ----------------------------------------------------------------------
declare 
    rec pgq.failed_queue%rowtype; 
begin 
    for rec in
        select fq.*
          from pgq.failed_queue fq, pgq.consumer,
               pgq.queue, pgq.subscription
         where queue_name = x_queue_name
           and co_name = x_consumer_name
           and sub_consumer = co_id
           and sub_queue = queue_id
           and ev_owner = sub_id
        order by ev_id
        limit x_count
        offset x_offset
    loop
        return next rec; 
    end loop; 
    return;
end; 
$$ language plpgsql security definer;

create or replace function pgq.failed_event_count(
    x_queue_name text,
    x_consumer_name text)
returns integer as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.failed_event_count(2)
--
--      Get size of failed event queue.
--
-- Parameters:
--      x_queue_name        - Queue name
--      x_consumer_name     - Consumer name
--
-- Returns:
--      Number of failed events in failed event queue.
-- ----------------------------------------------------------------------
declare 
    ret integer;
begin 
    select count(1) into ret
      from pgq.failed_queue, pgq.consumer, pgq.queue, pgq.subscription
     where queue_name = x_queue_name
       and co_name = x_consumer_name
       and sub_queue = queue_id
       and sub_consumer = co_id
       and ev_owner = sub_id;
    return ret;
end; 
$$ language plpgsql security definer;

create or replace function pgq.failed_event_delete(
    x_queue_name text,
    x_consumer_name text,
    x_event_id bigint)
returns integer as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.failed_event_delete(3)
--
--      Delete specific event from failed event queue.
--
-- Parameters:
--      x_queue_name        - Queue name
--      x_consumer_name     - Consumer name
--      x_event_id          - Event ID
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare 
    x_sub_id integer;
begin 
    select sub_id into x_sub_id
      from pgq.subscription, pgq.consumer, pgq.queue
     where queue_name = x_queue_name
       and co_name = x_consumer_name
       and sub_consumer = co_id
       and sub_queue = queue_id;
    if not found then
        raise exception 'no such queue/consumer';
    end if;

    delete from pgq.failed_queue
     where ev_owner = x_sub_id
       and ev_id = x_event_id;
    if not found then
        raise exception 'event not found';
    end if;

    return 1;
end; 
$$ language plpgsql security definer;

create or replace function pgq.failed_event_retry(
    x_queue_name text,
    x_consumer_name text,
    x_event_id bigint)
returns bigint as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.failed_event_retry(3)
--
--      Insert specific event from failed queue to main queue.
--
-- Parameters:
--      x_queue_name        - Queue name
--      x_consumer_name     - Consumer name
--      x_event_id          - Event ID
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare 
    ret         bigint;
    x_sub_id    integer;
begin 
    select sub_id into x_sub_id
      from pgq.subscription, pgq.consumer, pgq.queue
     where queue_name = x_queue_name
       and co_name = x_consumer_name
       and sub_consumer = co_id
       and sub_queue = queue_id;
    if not found then
        raise exception 'no such queue/consumer';
    end if;

    select pgq.insert_event_raw(x_queue_name, ev_id, ev_time,
            ev_owner, ev_retry, ev_type, ev_data,
            ev_extra1, ev_extra2, ev_extra3, ev_extra4)
      into ret
      from pgq.failed_queue, pgq.consumer, pgq.queue
     where ev_owner = x_sub_id
       and ev_id = x_event_id;
    if not found then
        raise exception 'event not found';
    end if;

    perform pgq.failed_event_delete(x_queue_name, x_consumer_name, x_event_id);

    return ret;
end; 
$$ language plpgsql security definer;







-- Section: Public Triggers

-- Group: Trigger Functions

-- \i triggers/pgq.logutriga.sql


-- ----------------------------------------------------------------------
-- Function: pgq.logtriga()
--
--      Deprecated - non-automatic SQL trigger.  It puts row data in partial
--      SQL form into queue.  It does not auto-detect table structure,
--      it needs to be passed as trigger arg.
--
-- Purpose:
--      Used by Londiste to generate replication events.  The "partial SQL"
--      format is more compact than the urlencoded format but cannot be
--      parsed, only applied.  Which is fine for Londiste.
--
-- Parameters:
--      arg1 - queue name
--      arg2 - column type spec string where each column corresponds to one char (k/v/i).
--              if spec string is shorter than column list, rest of columns default to 'i'.
--
-- Column types:
--      k   - pkey column
--      v   - normal data column
--      i   - ignore column
--
-- Queue event fields:
--    ev_type     - I/U/D
--    ev_data     - partial SQL statement
--    ev_extra1   - table name
--
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pgq.logtriga() RETURNS trigger
AS '$libdir/pgq_triggers', 'pgq_logtriga' LANGUAGE C;

-- ----------------------------------------------------------------------
-- Function: pgq.sqltriga()
--
--      Automatic SQL trigger.  It puts row data in partial SQL form into
--      queue.  It autodetects table structure.
--
-- Purpose:
--      Written as more flexible version of logtriga to handle exceptional cases
--      where there is no primary key index on table etc.
--
-- Parameters:
--      arg1 - queue name
--      argX - any number of optional arg, in any order
--
-- Optinal arguments:
--      SKIP                - The actual operation should be skipped
--      ignore=col1[,col2]  - don't look at the specified arguments
--      pkey=col1[,col2]    - Set pkey fields for the table, autodetection will be skipped
--      backup              - Put urlencoded contents of old row to ev_extra2
--
-- Queue event fields:
--    ev_type     - I/U/D
--    ev_data     - partial SQL statement
--    ev_extra1   - table name
--    ev_extra2   - optional urlencoded backup
--
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pgq.sqltriga() RETURNS trigger
AS '$libdir/pgq_triggers', 'pgq_sqltriga' LANGUAGE C;

-- ----------------------------------------------------------------------
-- Function: pgq.logutriga()
--
--      Trigger function that puts row data in urlencoded form into queue.
--
-- Purpose:
--	Used as producer for several PgQ standard consumers (cube_dispatcher, 
--      queue_mover, table_dispatcher).  Basically for cases where the
--      consumer wants to parse the event and look at the actual column values.
--
-- Trigger parameters:
--      arg1 - queue name
--      argX - any number of optional arg, in any order
--
-- Optinal arguments:
--      SKIP                - The actual operation should be skipped
--      ignore=col1[,col2]  - don't look at the specified arguments
--      pkey=col1[,col2]    - Set pkey fields for the table, autodetection will be skipped
--      backup              - Put urlencoded contents of old row to ev_extra2
--
-- Queue event fields:
--      ev_type      - I/U/D ':' pkey_column_list
--      ev_data      - column values urlencoded
--      ev_extra1    - table name
--      ev_extra2    - optional urlencoded backup
--
-- Regular listen trigger example:
-- >   CREATE TRIGGER triga_nimi AFTER INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgq.logutriga('qname');
--
-- Redirect trigger example:
-- >   CREATE TRIGGER triga_nimi BEFORE INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgq.logutriga('qname', 'SKIP');
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION pgq.logutriga() RETURNS TRIGGER
AS '$libdir/pgq_triggers', 'pgq_logutriga' LANGUAGE C;







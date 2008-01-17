


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

-- drop schema if exists pgq cascade;
create schema pgq;
grant usage on schema pgq to public;

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
--      queue_data                  - parent table for actual data tables
--      queue_switch_step1          - tx when rotation happened
--      queue_switch_step2          - tx after rotation was committed
--      queue_switch_time           - time when switch happened
--      queue_ticker_max_count      - batch should not contain more events
--      queue_ticker_max_lag        - events should not age more
--      queue_ticker_idle_period    - how often to tick when no events happen
-- ----------------------------------------------------------------------
create table pgq.queue (
	queue_id		    serial,
	queue_name		    text        not null,

        queue_ntables               integer     not null default 3,
        queue_cur_table             integer     not null default 0,
        queue_rotation_period       interval    not null default '2 hours',
	queue_switch_step1          bigint      not null default get_current_txid(),
	queue_switch_step2          bigint               default get_current_txid(),
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
--      tick_snapshot
-- ----------------------------------------------------------------------
create table pgq.tick (
        tick_queue                  int4            not null,
        tick_id                     bigint          not null,
        tick_time                   timestamptz     not null default now(),
        tick_snapshot               txid_snapshot   not null default get_current_snapshot(),

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
--      Consumer registration on a queue
--
-- Columns:
--
--      sub_id          - subscription id for internal usage
--      sub_queue       - queue id
--      sub_consumer    - consumer's id
--      sub_tick        - last tick the consumer processed
--      sub_batch       - shortcut for queue_id/consumer_id/tick_id
--      sub_next_tick   - 
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

        ev_txid             bigint          not null default get_current_txid(),
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
--      Events to be retried
--
-- Columns:
--      ev_retry_after          - time when it should be re-inserted to main queue
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

-- ----------------------------------------------------------------------
-- Table: pgq.failed_queue
--
--      Events whose processing failed
--
-- Columns:
--      ev_failed_reason               - consumer's excuse for not processing
--      ev_failed_time                 - when it was tagged failed
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
--      >   AND NOT txid_in_snapshot(ev_txid, sn1)
--      >   AND txid_in_snapshot(ev_txid, sn2)
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
           get_snapshot_xmax(last.tick_snapshot) as tx_start,
           get_snapshot_xmax(cur.tick_snapshot) as tx_end,
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
            get_snapshot_active(batch.last_snapshot) id1 left join
            get_snapshot_active(batch.cur_snapshot) id2 on (id1 = id2)
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
            || ' and txid_in_snapshot(ev.ev_txid, cur.tick_snapshot)'
            || ' and not txid_in_snapshot(ev.ev_txid, last.tick_snapshot)'
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
           get_snapshot_xmin(last.tick_snapshot) as tx_min, -- absolute minimum
           get_snapshot_xmax(cur.tick_snapshot) as tx_max, -- absolute maximum
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



create or replace function pgq.insert_event_raw(
        queue_name text, ev_id bigint, ev_time timestamptz,
        ev_owner integer, ev_retry integer, ev_type text, ev_data text,
        ev_extra1 text, ev_extra2 text, ev_extra3 text, ev_extra4 text)
returns bigint as $$
# -- ----------------------------------------------------------------------
# -- Function: pgq.insert_event_raw(11)
# --
# --      Actual event insertion.  Used also by retry queue maintenance.
# --
# -- Parameters:
# --      queue_name      - Name of the queue
# --      ev_id           - Event ID.  If NULL, will be taken from seq.
# --      ev_time         - Event creation time.
# --      ev_type         - user data
# --      ev_data         - user data
# --      ev_extra1       - user data
# --      ev_extra2       - user data
# --      ev_extra3       - user data
# --      ev_extra4       - user data
# --
# -- Returns:
# --      Event ID.
# -- ----------------------------------------------------------------------

    # load args
    queue_name = args[0]
    ev_id = args[1]
    ev_time = args[2]
    ev_owner = args[3]
    ev_retry = args[4]
    ev_type = args[5]
    ev_data = args[6]
    ev_extra1 = args[7]
    ev_extra2 = args[8]
    ev_extra3 = args[9]
    ev_extra4 = args[10]

    if not "cf_plan" in SD:
        # get current event table
        q = "select queue_data_pfx, queue_cur_table, queue_event_seq "\
            " from pgq.queue where queue_name = $1"
        SD["cf_plan"] = plpy.prepare(q, ["text"])

        # get next id
        q = "select nextval($1) as id"
        SD["seq_plan"] = plpy.prepare(q, ["text"])

    # get queue config
    res = plpy.execute(SD["cf_plan"], [queue_name])
    if len(res) != 1:
        plpy.error("Unknown event queue: %s" % (queue_name))
    tbl_prefix = res[0]["queue_data_pfx"]
    cur_nr = res[0]["queue_cur_table"]
    id_seq = res[0]["queue_event_seq"]

    # get id - bump seq even if id is given
    res = plpy.execute(SD['seq_plan'], [id_seq])
    if ev_id is None:
        ev_id = res[0]["id"]

    # create plan for insertion
    ins_plan = None
    ins_key = "ins.%s" % (queue_name)
    if ins_key in SD:
        nr, ins_plan = SD[ins_key]
        if nr != cur_nr:
            ins_plan = None
    if ins_plan == None:
        q = "insert into %s_%s (ev_id, ev_time, ev_owner, ev_retry,"\
            " ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4)"\
            " values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)" % (
            tbl_prefix, cur_nr)
        types = ["int8", "timestamptz", "int4", "int4", "text",
                 "text", "text", "text", "text", "text"]
        ins_plan = plpy.prepare(q, types)
        SD[ins_key] = (cur_nr, ins_plan)

    # insert the event
    plpy.execute(ins_plan, [ev_id, ev_time, ev_owner, ev_retry, ev_type, ev_data,
                            ev_extra1, ev_extra2, ev_extra3, ev_extra4])

    # done
    return ev_id

$$ language plpythonu;  -- event inserting needs no special perms



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
-- Parameters:
--      arg - desc
--
-- Returns:
--      Number of events processed.
-- ----------------------------------------------------------------------
declare
    cnt    integer;
    rec    record;
begin
    cnt := 0;
    for rec in
        select pgq.insert_event_raw(queue_name,
                    ev_id, ev_time, ev_owner, ev_retry, ev_type, ev_data,
                    ev_extra1, ev_extra2, ev_extra3, ev_extra4),
               ev_owner, ev_id
          from pgq.retry_queue, pgq.queue, pgq.subscription
         where ev_retry_after <= current_timestamp
           and sub_id = ev_owner
           and queue_id = sub_queue
         order by ev_retry_after
         limit 10
    loop
        cnt := cnt + 1;
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
        where get_snapshot_xmin(tick_snapshot) < cf.queue_switch_step2
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
            queue_switch_step1 = get_current_txid(),
            queue_switch_step2 = NULL
        where queue_id = cf.queue_id;

    -- clean ticks - avoid partial batches
    delete from pgq.tick
        where tick_queue = cf.queue_id
          and get_snapshot_xmin(tick_snapshot) < cf.queue_switch_step2;

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
       set queue_switch_step2 = get_current_txid()
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
begin
    return next 'pgq.subscription';
    return next 'pgq.consumer';
    return next 'pgq.queue';
    return next 'pgq.tick';
    return next 'pgq.retry_queue';

    -- vacuum also txid.epoch, if exists
    perform 1 from pg_class t, pg_namespace n
        where t.relname = 'epoch'
          and n.nspname = 'txid'
          and n.oid = t.relnamespace;
    if found then
        return next 'txid.epoch';
    end if;

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
    q     record;
    i     integer;
begin
    select * from pgq.queue into q
        where queue_name = x_queue_name;
    if not found then
        raise exception 'Queue not found';
    end if;
    execute 'grant select, update on '
        || q.queue_event_seq || ',' || q.queue_tick_seq
        || ' to public';
    execute 'grant select on '
        || q.queue_data_pfx
        || ' to public';
    for i in 0 .. q.queue_ntables - 1 loop
        execute 'grant select, insert on '
            || q.queue_data_pfx || '_' || i
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
--      Such function is needed because paraller calls o ticker() are
--      dangerous, and cannot be protected with locks as snapshot
--      is taken before.
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
$$ language plpgsql;  -- event inserting needs no special perms



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
$$ language plpgsql;  -- event inserting needs no special perms



create or replace function pgq.current_event_table(x_queue_name text)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.current_event_table(1)
--
--     Return active event table for particular queue.
--
-- Note:
--     The result is valid only during current transaction.
--
-- Parameters:
--     x_queue_name    - Queue name.
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
$$ language plpgsql; -- no perms needed


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
    next_batch      bigint;
    errmsg          text;
    sub             record;
begin
    select sub_queue, sub_id, sub_last_tick, sub_batch into sub
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
    next_batch := nextval('pgq.batch_id_seq');
    update pgq.subscription
        set sub_batch = next_batch,
            sub_next_tick = next_tick,
            sub_active = now()
        where sub_id = sub.sub_id;
    return next_batch;
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
--      Copies the event to failed queue.  Can be looked later.
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
--     Put the event into retry queue, to be processed later again.
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
    return '2.1.4';
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



create or replace function pgq.logutriga()
returns trigger as $$
# -- ----------------------------------------------------------------------
# -- Function: pgq.logutriga()
# --
# --      Trigger function that puts row data urlencoded into queue.
# --
# -- Trigger parameters:
# --      arg1 - queue name
# --      arg2 - optionally 'SKIP'
# --
# -- Queue event fields:
# --   ev_type      - I/U/D
# --   ev_data      - column values urlencoded
# --   ev_extra1    - table name
# --   ev_extra2    - primary key columns
# --
# -- Regular listen trigger example:
# -- >  CREATE TRIGGER triga_nimi AFTER INSERT OR UPDATE ON customer
# -- >  FOR EACH ROW EXECUTE PROCEDURE pgq.logutriga('qname');
# --
# -- Redirect trigger example:
# -- >   CREATE TRIGGER triga_nimi AFTER INSERT OR UPDATE ON customer
# -- >   FOR EACH ROW EXECUTE PROCEDURE pgq.logutriga('qname', 'SKIP');
# -- ----------------------------------------------------------------------

# this triger takes 1 or 2 args:
#   queue_name - destination queue
#   option return code (OK, SKIP) SKIP means op won't happen
# copy-paste of db_urlencode from skytools.quoting
from urllib import quote_plus
def db_urlencode(dict):
    elem_list = []
    for k, v in dict.items():
        if v is None:
            elem = quote_plus(str(k))
        else:
            elem = quote_plus(str(k)) + '=' + quote_plus(str(v))
        elem_list.append(elem)
    return '&'.join(elem_list)

# load args
queue_name = TD['args'][0]
if len(TD['args']) > 1:
    ret_code = TD['args'][1]
else:
    ret_code = 'OK'
table_oid = TD['relid']

# on first call init plans
if not 'init_done' in SD:
    # find table name
    q = "SELECT n.nspname || '.' || c.relname AS table_name"\
        " FROM pg_namespace n, pg_class c"\
        " WHERE n.oid = c.relnamespace AND c.oid = $1"
    SD['name_plan'] = plpy.prepare(q, ['oid'])

    # find key columns
    q = "SELECT k.attname FROM pg_index i, pg_attribute k"\
        " WHERE i.indrelid = $1 AND k.attrelid = i.indexrelid"\
        "   AND i.indisprimary AND k.attnum > 0 AND NOT k.attisdropped"\
        " ORDER BY k.attnum"
    SD['key_plan'] = plpy.prepare(q, ['oid'])

    # insert data
    q = "SELECT pgq.insert_event($1, $2, $3, $4, $5, null, null)"
    SD['ins_plan'] = plpy.prepare(q, ['text', 'text', 'text', 'text', 'text'])

    # shorter tags
    SD['op_map'] = {'INSERT': 'I', 'UPDATE': 'U', 'DELETE': 'D'}

    # remember init
    SD['init_done'] = 1

# load & cache table data
if table_oid in SD:
    tbl_name, tbl_keys = SD[table_oid]
else:
    res = plpy.execute(SD['name_plan'], [table_oid])
    tbl_name = res[0]['table_name']
    res = plpy.execute(SD['key_plan'], [table_oid])
    tbl_keys = ",".join(map(lambda x: x['attname'], res))

    SD[table_oid] = (tbl_name, tbl_keys)

# prepare args
if TD['event'] == 'DELETE':
    data = db_urlencode(TD['old'])
else:
    data = db_urlencode(TD['new'])

# insert event
plpy.execute(SD['ins_plan'], [
    queue_name,
    SD['op_map'][TD['event']],
    data, tbl_name, tbl_keys])

# done
return ret_code

$$ language plpythonu;




-- listen trigger:
-- create trigger triga_nimi after insert or update on customer
-- for each row execute procedure pgq.sqltriga('qname');

-- redirect trigger:
-- create trigger triga_nimi after insert or update on customer
-- for each row execute procedure pgq.sqltriga('qname', 'ret=SKIP');

create or replace function pgq.sqltriga()
returns trigger as $$
# -- ----------------------------------------------------------------------
# -- Function: pgq.sqltriga()
# --
# --      Trigger function that puts row data in partial SQL form into queue.
# --
# -- Parameters:
# --    arg1 - queue name
# --    arg2 - optional urlencoded options
# --
# -- Extra options:
# --
# --    ret     - return value for function OK/SKIP
# --    pkey    - override pkey fields, can be functions
# --    ignore  - comma separated field names to ignore
# --
# -- Queue event fields:
# --    ev_type     - I/U/D
# --    ev_data     - partial SQL statement
# --    ev_extra1   - table name
# --
# -- ----------------------------------------------------------------------
# this triger takes 1 or 2 args:
#   queue_name - destination queue
#   args - urlencoded dict of options:
#       ret - return value: OK/SKIP
#       pkey - comma-separated col names or funcs on cols
#              simple:  pkey=user,orderno
#              hashed:  pkey=user,hashtext(user)
#       ignore - comma-separated col names to ignore

# on first call init stuff
if not 'init_done' in SD:
    # find table name plan
    q = "SELECT n.nspname || '.' || c.relname AS table_name"\
        " FROM pg_namespace n, pg_class c"\
        " WHERE n.oid = c.relnamespace AND c.oid = $1"
    SD['name_plan'] = plpy.prepare(q, ['oid'])

    # find key columns plan
    q = "SELECT k.attname FROM pg_index i, pg_attribute k"\
        " WHERE i.indrelid = $1 AND k.attrelid = i.indexrelid"\
        "   AND i.indisprimary AND k.attnum > 0 AND NOT k.attisdropped"\
        " ORDER BY k.attnum"
    SD['key_plan'] = plpy.prepare(q, ['oid'])

    # data insertion
    q = "SELECT pgq.insert_event($1, $2, $3, $4, null, null, null)"
    SD['ins_plan'] = plpy.prepare(q, ['text', 'text', 'text', 'text'])

    # shorter tags
    SD['op_map'] = {'INSERT': 'I', 'UPDATE': 'U', 'DELETE': 'D'}

    # quoting
    from psycopg import QuotedString
    def quote(s):
        if s is None:
            return "null"
        s = str(s)
        return str(QuotedString(s))
        s = s.replace('\\', '\\\\')
        s = s.replace("'", "''")
        return "'%s'" % s

    # TableInfo class
    import re, urllib
    class TableInfo:
        func_rc = re.compile("([^(]+) [(] ([^)]+) [)]", re.I | re.X)
        def __init__(self, table_oid, options_txt):
            res = plpy.execute(SD['name_plan'], [table_oid])
            self.name = res[0]['table_name']

            self.parse_options(options_txt)
            self.load_pkey()

        def recheck(self, options_txt):
            if self.options_txt == options_txt:
                return
            self.parse_options(options_txt)
            self.load_pkey()

        def parse_options(self, options_txt):
            self.options = {'ret': 'OK'}
            if options_txt:
                for s in options_txt.split('&'):
                    k, v = s.split('=', 1)
                    self.options[k] = urllib.unquote_plus(v)
            self.options_txt = options_txt

        def load_pkey(self):
            self.pkey_list = []
            if not 'pkey' in self.options:
                res = plpy.execute(SD['key_plan'], [table_oid])
                for krow in res:
                    col = krow['attname']
                    expr = col + "=%s"
                    self.pkey_list.append( (col, expr) )
            else:
                for a_pk in self.options['pkey'].split(','):
                    m = self.func_rc.match(a_pk)
                    if m:
                        col = m.group(2)
                        fn = m.group(1)
                        expr = "%s(%s) = %s(%%s)" % (fn, col, fn)
                    else:
                        # normal case
                        col = a_pk
                        expr = col + "=%s"
                    self.pkey_list.append( (col, expr) )
            if len(self.pkey_list) == 0:
                plpy.error('sqltriga needs primary key on table')
        
        def get_insert_stmt(self, new):
            col_list = []
            val_list = []
            for k, v in new.items():
                col_list.append(k)
                val_list.append(quote(v))
            return "(%s) values (%s)" % (",".join(col_list), ",".join(val_list))

        def get_update_stmt(self, old, new):
            chg_list = []
            for k, v in new.items():
                ov = old[k]
                if v == ov:
                    continue
                chg_list.append("%s=%s" % (k, quote(v)))
            if len(chg_list) == 0:
                pk = self.pkey_list[0][0]
                chg_list.append("%s=%s" % (pk, quote(new[pk])))
            return "%s where %s" % (",".join(chg_list), self.get_pkey_expr(new))

        def get_pkey_expr(self, data):
            exp_list = []
            for col, exp in self.pkey_list:
                exp_list.append(exp % quote(data[col]))
            return " and ".join(exp_list)

    SD['TableInfo'] = TableInfo

    # cache some functions
    def proc_insert(tbl):
        return tbl.get_insert_stmt(TD['new'])
    def proc_update(tbl):
        return tbl.get_update_stmt(TD['old'], TD['new'])
    def proc_delete(tbl):
        return tbl.get_pkey_expr(TD['old'])
    SD['event_func'] = {
        'I': proc_insert,
        'U': proc_update,
        'D': proc_delete,
    }

    # remember init
    SD['init_done'] = 1


# load args
table_oid = TD['relid']
queue_name = TD['args'][0]
if len(TD['args']) > 1:
    options_str = TD['args'][1]
else:
    options_str = ''

# load & cache table data
if table_oid in SD:
    tbl = SD[table_oid]
    tbl.recheck(options_str)
else:
    tbl = SD['TableInfo'](table_oid, options_str)
    SD[table_oid] = tbl

# generate payload
op = SD['op_map'][TD['event']]
data = SD['event_func'][op](tbl)

# insert event
plpy.execute(SD['ins_plan'], [queue_name, op, data, tbl.name])

# done
return tbl.options['ret']

$$ language plpythonu;







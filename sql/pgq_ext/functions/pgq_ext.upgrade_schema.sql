
create or replace function pgq_ext.upgrade_schema()
returns int4 as $$
-- updates table structure if necessary
-- ----------------------------------------------------------------------
-- Function: pgq_ext.upgrade_schema()
--
--	    Upgrades tables to have column subconsumer_id 
--
-- Parameters:
--      None
--
-- Returns:
--	    number of tables updated 
-- Calls:
--      None
-- Tables directly manipulated:
--      alter - pgq_ext.completed_batch
--      alter - pgq_ext.completed_tick
--      alter - pgq_ext.partial_batch
--      alter - pgq_ext.completed_event
-- ----------------------------------------------------------------------
declare
    cnt int4 = 0;
    tbl text;
    sql text;
begin
    -- pgq_ext.completed_batch: subconsumer_id
    perform 1 from information_schema.columns
      where table_schema = 'pgq_ext'
        and table_name = 'completed_batch'
        and column_name = 'subconsumer_id';
    if not found then
        alter table pgq_ext.completed_batch
            add column subconsumer_id text;
        update pgq_ext.completed_batch
            set subconsumer_id = '';
        alter table pgq_ext.completed_batch
            alter column subconsumer_id set not null;
        alter table pgq_ext.completed_batch
            drop constraint completed_batch_pkey;
        alter table pgq_ext.completed_batch
            add constraint completed_batch_pkey
            primary key (consumer_id, subconsumer_id);
        cnt := cnt + 1;
    end if;

    -- pgq_ext.completed_tick: subconsumer_id
    perform 1 from information_schema.columns
      where table_schema = 'pgq_ext'
        and table_name = 'completed_tick'
        and column_name = 'subconsumer_id';
    if not found then
        alter table pgq_ext.completed_tick
            add column subconsumer_id text;
        update pgq_ext.completed_tick
            set subconsumer_id = '';
        alter table pgq_ext.completed_tick
            alter column subconsumer_id set not null;
        alter table pgq_ext.completed_tick
            drop constraint completed_tick_pkey;
        alter table pgq_ext.completed_tick
            add constraint completed_tick_pkey
            primary key (consumer_id, subconsumer_id);
        cnt := cnt + 1;
    end if;

    -- pgq_ext.partial_batch: subconsumer_id
    perform 1 from information_schema.columns
      where table_schema = 'pgq_ext'
        and table_name = 'partial_batch'
        and column_name = 'subconsumer_id';
    if not found then
        alter table pgq_ext.partial_batch
            add column subconsumer_id text;
        update pgq_ext.partial_batch
            set subconsumer_id = '';
        alter table pgq_ext.partial_batch
            alter column subconsumer_id set not null;
        alter table pgq_ext.partial_batch
            drop constraint partial_batch_pkey;
        alter table pgq_ext.partial_batch
            add constraint partial_batch_pkey
            primary key (consumer_id, subconsumer_id);
        cnt := cnt + 1;
    end if;

    -- pgq_ext.completed_event: subconsumer_id
    perform 1 from information_schema.columns
      where table_schema = 'pgq_ext'
        and table_name = 'completed_event'
        and column_name = 'subconsumer_id';
    if not found then
        alter table pgq_ext.completed_event
            add column subconsumer_id text;
        update pgq_ext.completed_event
            set subconsumer_id = '';
        alter table pgq_ext.completed_event
            alter column subconsumer_id set not null;
        alter table pgq_ext.completed_event
            drop constraint completed_event_pkey;
        alter table pgq_ext.completed_event
            add constraint completed_event_pkey
            primary key (consumer_id, subconsumer_id, batch_id, event_id);
        cnt := cnt + 1;
    end if;

    -- add default value to subconsumer_id column
    for tbl in
        select table_name
           from information_schema.columns
           where table_schema = 'pgq_ext'
             and table_name in ('completed_tick', 'completed_event', 'partial_batch', 'completed_batch')
             and column_name = 'subconsumer_id'
             and column_default is null
    loop
        sql := 'alter table pgq_ext.' || tbl
            || ' alter column subconsumer_id set default ' || quote_literal('');
        execute sql;
        cnt := cnt + 1;
    end loop;

    return cnt;
end;
$$ language plpgsql;



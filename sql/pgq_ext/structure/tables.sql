-- ----------------------------------------------------------------------
-- Section: Tables
--
--      The pgq_ext schema exists to help in making sure that allenents get
--      processed and they get processed only once
--
-- Simple quidelines for avoiding duplicate events:
-- 
--      It is pretty burdensome to check if event is already processed,
--      especially on bulk data moving.  Here's a way how checking
--      individual event checks can be avoided by tracking processing of batches.
-- 
--      First, consumer must guarantee that it processes all events in one tx.
-- 
--      Consumer itself can tag events for retry, but then
--      it must be able to handle them later.
-- 
-- Simple case: Only one db:
-- 
--      If the PgQ queue and event data handling happen in same database,
--      the consumer must simply call pgq.finish_batch() inside
--      the event-processing transaction.
-- 
-- Several databases:
-- 
--      If the event processing happens in different database, the consumer
--      must store the batch_id into destination database, inside the same
--      transaction as the event processing happens.
-- 
--      * Only after committing it, consumer can call pgq.finish_batch()
--        in queue database and commit that.
-- 
--      * As the batches come in sequence, there's no need to remember
--        full log of batch_id's, it's enough to keep the latest batch_id.
-- 
--      * Then at the start of every batch, consumer can check if the batch_id already
--        exists in destination database, and if it does, then just tag batch done,
--        without processing.
-- 
--      With this, there's no need for consumer to check for already processed
--      events.
-- 
-- Note:
-- 
--      This assumes the event processing is transactional and failures
--      will be rollbacked.  If event processing includes communication with
--      world outside database, eg. sending email, such handling won't work.
-- 
-- ----------------------------------------------------------------------

set client_min_messages = 'warning';
set default_with_oids = 'off';

create schema pgq_ext;


--
-- Table: pgq_ext.completed_tick
--
--      Used for tracking last completed batch tracking
--      via tick_id.
--
create table pgq_ext.completed_tick (
    consumer_id     text not null,
    subconsumer_id  text not null,
    last_tick_id    bigint not null,

    primary key (consumer_id, subconsumer_id)
);

--
-- Table: pgq_ext.completed_batch
--
--      Used for tracking last completed batch tracking
--
create table pgq_ext.completed_batch (
    consumer_id     text not null,
    subconsumer_id  text not null,
    last_batch_id   bigint not null,

    primary key (consumer_id, subconsumer_id)
);


--
-- Table: pgq_ext.completed_event
--
--      Stored completed event in current partial batch.
--
create table pgq_ext.completed_event (
    consumer_id     text not null,
    subconsumer_id  text not null,
    batch_id        bigint not null,
    event_id        bigint not null,

    primary key (consumer_id, subconsumer_id, batch_id, event_id)
);

--
-- Table: pgq_ext.partial_batch
--
--      Stored current in-progress batch
--
create table pgq_ext.partial_batch (
    consumer_id     text not null,
    subconsumer_id  text not null,
    cur_batch_id    bigint not null,

    primary key (consumer_id, subconsumer_id)
);


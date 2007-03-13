
set client_min_messages = 'warning';
set default_with_oids = 'off';

create schema pgq_ext;
grant usage on schema pgq_ext to public;


--
-- batch tracking
--
create table pgq_ext.completed_batch (
    consumer_id   text not null,
    last_batch_id bigint not null,

    primary key (consumer_id)
);


--
-- event tracking
--
create table pgq_ext.completed_event (
    consumer_id   text not null,
    batch_id      bigint not null,
    event_id      bigint not null,

    primary key (consumer_id, batch_id, event_id)
);

create table pgq_ext.partial_batch (
    consumer_id   text not null,
    cur_batch_id  bigint not null,

    primary key (consumer_id)
);

--
-- tick tracking for SerialConsumer()
-- no access functions provided here
--
create table pgq_ext.completed_tick (
    consumer_id   text not null,
    last_tick_id  bigint not null,

    primary key (consumer_id)
);


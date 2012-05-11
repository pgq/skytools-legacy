-- ----------------------------------------------------------------------
-- File: Tables
--
--      Schema 'pgq_node', contains tables for cascaded pgq.
--
-- Event types for cascaded queue:
--      pgq.location-info       - ev_data: node_name, extra1: queue_name, extra2: location, extra3: dead
--                                It contains updated node connect string.
--
--      pgq.global-watermark    - ev_data: tick_id,  extra1: queue_name
--                                Root node sends minimal tick_id that must be kept.
--
--      pgq.tick-id             - ev_data: tick_id,  extra1: queue_name
--                                Partition node inserts it's tick-id into combined queue.
--
-- ----------------------------------------------------------------------

create schema pgq_node;

-- ----------------------------------------------------------------------
-- Table: pgq_node.location
--
--      Static table that just lists all members in set.
--
-- Columns:
--      queue_name      - cascaded queue name
--      node_name       - node name
--      node_location   - libpq connect string for connecting to node
--      dead            - whether the node is offline
-- ----------------------------------------------------------------------
create table pgq_node.node_location (
    queue_name      text not null,
    node_name       text not null,
    node_location   text not null,
    dead            boolean not null default false,

    primary key (queue_name, node_name)
);

-- ----------------------------------------------------------------------
-- Table: pgq_node.node_info
--
--      Local node info.
--
-- Columns:
--      queue_name          - cascaded queue name
--      node_type           - local node type
--      node_name           - local node name
--      provider_node       - provider node name
--      worker_name         - consumer name that maintains this node
--      combined_queue      - on 'leaf' the target combined set name
--      node_attrs          - urlencoded fields for worker
--
-- Node types:
--      root            - data + batches is generated here
--      branch          - replicates full queue contents and maybe contains some tables
--      leaf            - does not replicate queue / or uses combined queue for that
-- ----------------------------------------------------------------------
create table pgq_node.node_info (
    queue_name      text not null primary key,
    node_type       text not null,
    node_name       text not null,
    worker_name     text,
    combined_queue  text,
    node_attrs      text,

    foreign key (queue_name, node_name) references pgq_node.node_location,
    check (node_type in ('root', 'branch', 'leaf')),
    check (case when node_type = 'root'   then  (worker_name is not null and combined_queue is null)
                when node_type = 'branch' then  (worker_name is not null and combined_queue is null)
                when node_type = 'leaf'   then  (worker_name is not null)
                else false end)
);

-- ----------------------------------------------------------------------
-- Table: pgq_node.local_state
--
--      All cascaded consumers (both worker and non-worker)
--      keep their state here.
--
-- Columns:
--      queue_name      - cascaded queue name
--      consumer_name   - cascaded consumer name
--      provider_node   - node name the consumer reads from
--      last_tick_id    - last committed tick id on this node
--      cur_error       - reason why current batch failed
--      paused          - whether consumer should wait
--      uptodate        - if consumer has seen new state
-- ----------------------------------------------------------------------
create table pgq_node.local_state (
    queue_name      text not null,
    consumer_name   text not null,
    provider_node   text not null,
    last_tick_id    bigint not null,
    cur_error       text,

    paused          boolean not null default false,
    uptodate        boolean not null default false,

    primary key (queue_name, consumer_name),
    foreign key (queue_name) references pgq_node.node_info,
    foreign key (queue_name, provider_node) references pgq_node.node_location
);

-- ----------------------------------------------------------------------
-- Table: pgq_node.subscriber
--
--      List of nodes that subscribe to local node.
--
-- Columns:
--      queue_name      - cascaded queue name
--      subscriber_node - node name that uses this node as provider.
--      worker_name     - consumer name that maintains remote node
-- ----------------------------------------------------------------------
create table pgq_node.subscriber_info (
    queue_name          text not null,
    subscriber_node     text not null,
    worker_name         text not null,
    watermark_name      text not null,

    primary key (queue_name, subscriber_node),
    foreign key (queue_name, subscriber_node) references pgq_node.node_location,
    foreign key (worker_name) references pgq.consumer (co_name),
    foreign key (watermark_name) references pgq.consumer (co_name)
);


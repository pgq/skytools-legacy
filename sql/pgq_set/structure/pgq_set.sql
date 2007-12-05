
create schema pgq_set;
grant usage on schema pgq_set to public;

-- ----------------------------------------------------------------------
-- Table: pgq_set.member_info
--
--      Static table that just lists all members in set.
--
-- Columns:
--      set_name        - set name
--      node_name       - node name
--      node_location   - libpq connect string for connecting to node
--      online          - whether the node is available
-- ----------------------------------------------------------------------
create table pgq_set.member_info (
    set_name        text not null,
    node_name       text not null,
    node_location   text not null,
    dead            boolean not null default false,

    primary key (set_name, node_name)
);

-- ----------------------------------------------------------------------
-- Table: pgq_set.local_node
--
--      Local node info.
--
-- Columns:
--      set_name            - set name
--      node_type           - local node type
--      node_name           - local node name
--      queue_name          - local queue name for set, NULL on leaf
--      provider_node       - provider node name
--      combined_set        - on 'merge-leaf' the target combined set name
--      global_watermark    - set's global watermark, set by root node
--      paused              - true if worker for this node should sleep
--      resync              - true if worker for this node needs to re-register itself on provider queue
--      up_to_date          - true if worker for this node has seen table changes
--
-- Node types:
--      root            - data + batches is generated here
--      branch          - replicates full queue contents and maybe contains some tables
--      leaf            - does not replicate queue
--      combined-root   - data from several partitions is merged here
--      combined-branch - can take over the role of combined-root
--      merge-leaf      - this node in part set is linked to combined-root/branch
-- ----------------------------------------------------------------------
create table pgq_set.set_info (
    set_name        text not null primary key,
    node_type       text not null,
    node_name       text not null,
    queue_name      text,
    provider_node   text,
    combined_set    text,

    global_watermark bigint not null,

    paused          boolean not null default false,
    resync          boolean not null default false,
    up_to_date      boolean not null default false,

    foreign key (set_name, node_name) references pgq_set.member_info,
    foreign key (set_name, provider_node) references pgq_set.member_info,
    check (node_type in ('root', 'branch', 'leaf', 'combined-root', 'combined-branch', 'merge-leaf')),
    check (case when node_type = 'root'              then (queue_name is not null and provider_node is null     and combined_set is null)
                when node_type = 'branch'            then (queue_name is not null and provider_node is not null and combined_set is null)
                when node_type = 'leaf'              then (queue_name is null     and provider_node is not null and combined_set is null)
                when node_type = 'combined-root'     then (queue_name is not null and provider_node is null     and combined_set is null)
                when node_type = 'combined-branch'   then (queue_name is not null and provider_node is not null and combined_set is null)
                when node_type = 'merge-leaf'        then (queue_name is null     and provider_node is not null and combined_set is not null)
                else false end)
);

-- ----------------------------------------------------------------------
-- Table: pgq_set.subscriber_info
--
--      Contains subscribers for a set.
--
-- Columns:
--      set_name        - set's name
--      node_name       - node name
--      worker_name     - consumer_name for node
--      local_watermark - watermark for node and it's subscribers
-- ----------------------------------------------------------------------
create table pgq_set.subscriber_info (
    set_name        text not null,
    node_name       text not null,
    worker_name     text not null,
    local_watermark bigint not null,

    primary key (set_name, node_name),
    foreign key (set_name, node_name) references pgq_set.member_info
);

-- ----------------------------------------------------------------------
-- Table: pgq_set.completed_tick
--
--      Contains completed tick_id from provider.
--
-- Columns:
--      set_name - set's name
--      tick_id  - last committed tick id
-- ----------------------------------------------------------------------
create table pgq_set.completed_tick (
    set_name        text not null primary key,
    tick_id         bigint not null,

    foreign key (set_name) references pgq_set.set_info
);


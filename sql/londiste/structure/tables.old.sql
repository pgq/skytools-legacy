-- ----------------------------------------------------------------------
-- Section: Londiste internals
--
--      Londiste storage: tables/seqs/fkeys/triggers/events.
--
-- Londiste event types:
--      I/U/D                   - ev_data: table update in partial-sql format, ev_extra1: fq table name
--      I:/U:/D: <pk>           - ev_data: table update in urlencoded format, ev_extra1: fq table name
--      londiste.add-table      - ev_data: table name that was added on root
--      londiste.remove-table   - ev_data: table name that was removed on root
--      londiste.update-seq     - ev_data: new seq value from root, ev_extra1: seq name
--      lodniste.remove-seq     - ev_data: seq name that was removed on root
-- ----------------------------------------------------------------------
create schema londiste;

set default_with_oids = 'off';


-- ----------------------------------------------------------------------
-- Table: londiste.set_table
--
--      Tables available on root, meaning that events for only
--      tables specified here can appear in queue.
--
-- Columns:
--      nr          - just to have stable order
--      set_name    - which set the table belongs to
--      table_name  - fq table name
-- ----------------------------------------------------------------------
create table londiste.set_table (
    nr                  serial not null,
    set_name            text not null,
    table_name          text not null,
    foreign key (set_name) references pgq_node.node_info (queue_name),
    primary key (set_name, table_name)
);

-- ----------------------------------------------------------------------
-- Table: londiste.set_seq
--
--      Sequences available on root, meaning that events for only
--      sequences specified here can appear in queue.
--
-- Columns:
--      nr          - just to have stable order
--      set_name    - which set the table belongs to
--      seq_name    - fq seq name
--      local       - there is actual seq on local node
--      last_value  - last published value from root
-- ----------------------------------------------------------------------
create table londiste.seq_state (
    nr                  serial not null,
    set_name            text not null,
    seq_name            text not null,
    local               boolean not null default false,
    last_value          int8 not null,
    foreign key (set_name) references pgq_node.node_info (queue_name),
    primary key (set_name, seq_name)
);


-- ----------------------------------------------------------------------
-- Table: londiste.node_table
--
--      Info about attached tables.
--
-- Columns:
--      nr              - Dummy number for visual ordering
--      set_name        - Set name
--      table_name      - fully-qualified table name
--      merge_state     - State for tables
--      trigger_type    - trigger type
--      trigger_name    - londiste trigger name
--      copy_snapshot   - remote snapshot for COPY command
--      custom_tg_args  - user-specified 
--      skip_truncate   - if 'in-copy' should not do TRUNCATE
--
-- Tables merge states:
--      master          - master: all in sync
--      ok              - slave: all in sync
--      in-copy         -
--      catching-up     -
--      wanna-sync:%    -
--      do-sync:%       -
--      unsynced        -
--
-- Trigger type:
--      notrigger       - no trigger applied
--      pgq.logtriga    - Partial SQL trigger with fixed column list
--      pgq.sqltriga    - Partial SQL trigger with autodetection
--      pgq.logutriga   - urlenc trigger with autodetection
--      pgq.denytrigger - deny trigger
-- ----------------------------------------------------------------------
create table londiste.node_table (
    nr                  serial not null,
    set_name            text not null,
    table_name          text not null,
    merge_state         text,
    custom_snapshot     text,
    skip_truncate       bool,

    foreign key (set_name, table_name) references londiste.set_table,
    primary key (set_name, table_name)
);


-- ----------------------------------------------------------------------
-- Table: londiste.applied_execute
--
--      Info about EXECUTE commands that are ran.
--
-- Columns:
--      set_name        - which set it belongs to
--      execute_file    - filename / unique id
--      execute_time    - the time execute happened
--      execute_sql     - contains SQL for EXECUTE event (informative)
-- ----------------------------------------------------------------------
create table londiste.applied_execute (
    set_name            text not null,
    execute_file        text not null,
    execute_time        timestamptz not null default now(),
    execute_sql         text not null,
    primary key (set_name, execute_file)
);


-- ----------------------------------------------------------------------
-- Table: londiste.pending_fkeys
--
--      Details on dropped fkeys.  Global, not specific to any set.
--
-- Columns:
--      from_table      - fully-qualified table name
--      to_table        - fully-qualified table name
--      fkey_name       - name of constraint
--      fkey_def        - full fkey definition
-- ----------------------------------------------------------------------
create table londiste.pending_fkeys (
    from_table          text not null,
    to_table            text not null,
    fkey_name           text not null,
    fkey_def            text not null,
    
    primary key (from_table, fkey_name)
);



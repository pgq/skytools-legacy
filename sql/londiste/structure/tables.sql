-- ----------------------------------------------------------------------
-- Section: Londiste internals
--
--      Londiste storage: tables/seqs/fkeys/triggers/events.
--
-- Londiste event types:
--      I/U/D       - ev_data: table update in partial-sql format, ev_extra1: fq table name
--      I:/U:/D:    - ev_data: table update in urlencoded format, ev_extra1: fq table name
--      add-seq     - ev_data: seq name that was added on root
--      del-seq     - ev_data: seq name that was removed on root
--      add-tbl     - ev_data: table name that was added on root
--      del-tbl     - ev_data: table name that was removed on root
--      seq-values  - ev_data: urlencoded fqname:value pairs
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
    foreign key (set_name) references pgq_set.set_info,
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
-- ----------------------------------------------------------------------
create table londiste.set_seq (
    nr                  serial not null,
    set_name            text not null,
    seq_name            text not null,
    foreign key (set_name) references pgq_set.set_info,
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
-- Table: londiste.node_trigger
--
--      Node-specific triggers.  When node type changes,
--      Londiste will make sure unnecessary triggers are
--      dropped and new triggers created.
--
-- Columns:
--      set_name        - set it belongs to
--      table_name      - table name
--      tg_type         - any / root / non-root
--      tg_name         - name for the trigger
--      tg_def          - full statement for trigger creation
-- ----------------------------------------------------------------------
create table londiste.node_trigger (
    set_name            text not null,
    table_name          text not null,
    tg_name             text not null,
    tg_type             text not null,
    tg_def              text not null,
    foreign key (set_name, table_name) references londiste.node_table,
    primary key (set_name, table_name, tg_name)
);

-- ----------------------------------------------------------------------
-- Table: londiste.node_seq
--
--      Info about attached sequences.
--
-- Columns:
--      nr              - dummy number for ordering
--      set_name        - which set it belongs to
--      seq_name        - fully-qualified seq name
-- ----------------------------------------------------------------------
create table londiste.node_seq (
    nr                  serial not null,
    set_name            text not null,
    seq_name            text not null,
    foreign key (set_name, seq_name) references londiste.set_seq,
    primary key (set_name, seq_name)
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


-- ----------------------------------------------------------------------
-- Table: londiste.pending_triggers
--
--      Details on dropped triggers.  Global, not specific to any set.
--
-- Columns:
--      table_name      - fully-qualified table name
--      trigger_name    - trigger name
--      trigger_def     - full trigger definition
-- ----------------------------------------------------------------------
create table londiste.pending_triggers (
    table_name          text not null,
    trigger_name        text not null,
    trigger_def         text not null,
    
    primary key (table_name, trigger_name)
);


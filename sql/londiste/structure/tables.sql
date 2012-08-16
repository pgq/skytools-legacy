-- ----------------------------------------------------------------------
-- Section: Londiste internals
--
--      Londiste storage: tables/seqs/fkeys/triggers/events.
--
-- Londiste event types:
--      I/U/D                   - partial SQL event from pgq.sqltriga()
--      I:/U:/D: <pk>           - urlencoded event from pgq.logutriga()
--      EXECUTE                 - SQL script execution
--      TRUNCATE                - table truncation
--      londiste.add-table      - global table addition
--      londiste.remove-table   - global table removal
--      londiste.update-seq     - sequence update
--      londiste.remove-seq     - global sequence removal
--
-- pgq.sqltriga() event:
--      ev_type     - I/U/D which means insert/update/delete
--      ev_data     - partial SQL
--      ev_extra1   - table name
--
--      Insert: ev_type = "I", ev_data = "(col1, col2) values (2, 'foo')", ev_extra1 = "public.tblname"
--
--      Update: ev_type = "U", ev_data = "col2 = null where col1 = 2", ev_extra1 = "public.tblname"
--
--      Delete: ev_type = "D", ev_data = "col1 = 2", ev_extra1 = "public.tblname"
--
-- pgq.logutriga() event:
--      ev_type     - I:/U:/D: plus comma separated list of pkey columns
--      ev_data     - urlencoded row columns
--      ev_extra1   - table name
--
--      Insert: ev_type = "I:col1", ev_data = ""
--
-- Truncate trigger event:
--      ev_type     - TRUNCATE
--      ev_extra1   - table name
--
-- Execute SQL event:
--      ev_type     - EXECUTE
--      ev_data     - SQL script
--      ev_extra1   - Script ID
--
-- Global table addition:
--      ev_type     - londiste.add-table
--      ev_data     - table name
--
-- Global table removal:
--      ev_type     - londiste.remove-table
--      ev_data     - table name
--
-- Global sequence update:
--      ev_type     - londiste.update-seq
--      ev_data     - seq value
--      ev_extra1   - seq name
--5)
-- Global sequence removal:
--      ev_type     - londiste.remove-seq
--      ev_data     - seq name
-- ----------------------------------------------------------------------
create schema londiste;

set default_with_oids = 'off';


-- ----------------------------------------------------------------------
-- Table: londiste.table_info
--
--      Info about registered tables.
--
-- Columns:
--      nr              - number for visual ordering
--      queue_name      - Cascaded queue name
--      table_name      - fully-qualified table name
--      local           - Is used locally
--      merge_state     - State for tables
--      custom_snapshot - remote snapshot for COPY command
--      dropped_ddl     - temp place to store ddl
--      table_attrs     - urlencoded dict of extra attributes
--
-- Tables merge states:
--      NULL            - copy has not yet happened
--      in-copy         - ongoing bulk copy
--      catching-up     - copy process applies events that happened during copy
--      wanna-sync:%    - copy process caught up, wants to hand table over to replay
--      do-sync:%       - replay process is ready to accept the table
--      ok              - in sync, replay applies events
-- ----------------------------------------------------------------------
create table londiste.table_info (
    nr                  serial not null,
    queue_name          text not null,
    table_name          text not null,
    local               boolean not null default false,
    merge_state         text,
    custom_snapshot     text,
    dropped_ddl         text,
    table_attrs         text,
    dest_table          text,

    primary key (queue_name, table_name),
    foreign key (queue_name)
      references pgq_node.node_info (queue_name)
      on delete cascade,
    check (dropped_ddl is null or merge_state in ('in-copy', 'catching-up'))
);


-- ----------------------------------------------------------------------
-- Table: londiste.seq_info
--
--      Sequences available on this queue.
--
-- Columns:
--      nr          - number for visual ordering
--      queue_name  - cascaded queue name
--      seq_name    - fully-qualified seq name
--      local       - there is actual seq on local node
--      last_value  - last published value from root
-- ----------------------------------------------------------------------
create table londiste.seq_info (
    nr                  serial not null,
    queue_name          text not null,
    seq_name            text not null,
    local               boolean not null default false,
    last_value          int8 not null,

    primary key (queue_name, seq_name),
    foreign key (queue_name)
      references pgq_node.node_info (queue_name)
      on delete cascade
);


-- ----------------------------------------------------------------------
-- Table: londiste.applied_execute
--
--      Info about EXECUTE commands that are ran.
--
-- Columns:
--      queue_name      - cascaded queue name
--      execute_file    - filename / unique id
--      execute_time    - the time execute happened
--      execute_sql     - contains SQL for EXECUTE event (informative)
-- ----------------------------------------------------------------------
create table londiste.applied_execute (
    queue_name          text not null,
    execute_file        text not null,
    execute_time        timestamptz not null default now(),
    execute_sql         text not null,
    execute_attrs       text,
    primary key (execute_file)
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



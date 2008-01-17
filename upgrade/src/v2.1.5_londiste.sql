begin;

create table londiste.subscriber_pending_fkeys(
    from_table          text not null,
    to_table            text not null,
    fkey_name           text not null,
    fkey_def            text not null,
    
    primary key (from_table, fkey_name)
);

create table londiste.subscriber_pending_triggers (
    table_name          text not null,
    trigger_name        text not null,
    trigger_def         text not null,

    primary key (table_name, trigger_name)
);

-- drop function londiste.denytrigger();

\i ../sql/londiste/functions/londiste.find_table_fkeys.sql
\i ../sql/londiste/functions/londiste.find_table_triggers.sql
\i ../sql/londiste/functions/londiste.find_column_types.sql
\i ../sql/londiste/functions/londiste.subscriber_fkeys_funcs.sql
\i ../sql/londiste/functions/londiste.subscriber_trigger_funcs.sql
\i ../sql/londiste/functions/londiste.quote_fqname.sql

\i ../sql/londiste/functions/londiste.find_table_oid.sql
\i ../sql/londiste/functions/londiste.get_last_tick.sql
\i ../sql/londiste/functions/londiste.provider_add_table.sql
\i ../sql/londiste/functions/londiste.provider_create_trigger.sql
\i ../sql/londiste/functions/londiste.provider_notify_change.sql
\i ../sql/londiste/functions/londiste.provider_remove_table.sql
\i ../sql/londiste/functions/londiste.set_last_tick.sql
\i ../sql/londiste/functions/londiste.subscriber_remove_table.sql

\i ../sql/londiste/structure/grants.sql

end;



create type londiste.ret_provider_table_list as (
    table_name text,
    trigger_name text
);

create type londiste.ret_subscriber_table as (
    table_name text,
    merge_state text,
    snapshot text,
    trigger_name text,
    skip_truncate bool
);


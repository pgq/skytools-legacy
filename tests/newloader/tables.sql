
set client_min_messages = 'warning';

create schema data;

create table data.simple_tbl (
    username text not null,
    contactname text not null,
    data text,
    primary key (username, contactname)
);

create table data.bulk_tbl (
    id serial primary key,
    data text
);

create table data.keep_all_tbl (
    id serial primary key,
    username text not null,
    tstamp timestamptz not null default now(),
    data text
);

create table data.keep_latest_tbl (
    id serial primary key,
    username text not null,
    tstamp timestamptz not null default now(),
    data text
);

create table data.random_tbl (
    id serial primary key,
    username text not null,
    tstamp timestamptz not null default now(),
    data text
);



set client_min_messages = 'warning';

create table data1 (
    id serial primary key,
    data text
);

create unique index idx_data1_uq on data1 (data);

create index idx_data1_rand on data1 (id, data);

create table data2 (
    id serial primary key,
    data text,
    -- old_id integer references data1,
    constraint uq_data2 unique (data)
);

create index idx_data2_rand on data2 (id, data);


create sequence test_seq;
select setval('test_seq', 50);



create table expect_test (
    dbname text primary key
);
insert into expect_test values (current_database());

create table skip_test (
    id serial not null,
    dbname text not null,
    primary key (id, dbname)
);
insert into skip_test (dbname) values (current_database());


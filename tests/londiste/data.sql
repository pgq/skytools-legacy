
set client_min_messages = 'warning';

create table data1 (
    id serial primary key,
    data text
);
cluster data1 using data1_pkey;

create or replace function test_triga() returns trigger
as $$ begin return new; end; $$ language plpgsql;
create trigger xtriga after  insert on data1
for each row execute procedure test_triga();


create unique index idx_data1_uq on data1 (data);

create index idx_data1_rand on data1 (id, data);


create table data2 (
    id serial primary key,
    data text,
    ref1 integer references data1,
    constraint uq_data2 unique (data)
);

create index idx_data2_rand on data2 (id, data);
cluster data2 using idx_data2_rand;

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

create table "Table" (
    "I D" serial primary key,
    "table" text,
    "d1.ref" int4 references data1,
    constraint "Woof" unique ("table")
);

create index "idx Table" on "Table" ("table", "I D");


create table inh_parent1 (
    id serial primary key,
    data text,
    ref1 integer,
    constraint p1_uq_data unique (data)
);

create table inh_parent2 (
    id serial primary key,
    data text,
    ref1 integer,
    constraint p2_uq_data unique (data)
);

create table inh_mid (
    id serial primary key,
    data text,
    ref1 integer,
    constraint m_uq_data unique (data)
) inherits (inh_parent1, inh_parent2);

create table inh_child (
    id serial primary key,
    data text,
    ref1 integer,
    constraint c_uq_data unique (data)
) inherits (inh_mid);





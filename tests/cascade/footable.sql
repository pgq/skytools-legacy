
create table footable (
    id serial primary key,
    username text not null,
    utype int4 not null check (utype in (1,2,3))
);
create index uindex on footable (username);


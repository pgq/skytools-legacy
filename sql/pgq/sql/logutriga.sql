
select 1 from (select set_config(name, 'escape', false) as ignore
          from pg_settings where name = 'bytea_output') x
          where x.ignore = 'foo';

drop function pgq.insert_event(text, text, text,  text, text, text, text);
create or replace function pgq.insert_event(que text, ev_type text, ev_data text, x1 text, x2 text, x3 text, x4 text)
returns bigint as $$
begin
    raise notice 'insert_event(%, %, %, %)', que, ev_type, ev_data, x1;
    return 1;
end;
$$ language plpgsql;

create table udata (
    id serial primary key,
    txt text,
    bin bytea
);

create trigger utest AFTER insert or update or delete ON udata
for each row execute procedure pgq.logutriga('udata_que');

insert into udata (txt) values ('text1');
insert into udata (bin) values (E'bi\tn\\000bin');

-- test ignore
drop trigger utest on udata;
truncate udata;
create trigger utest after insert or update or delete on udata
for each row execute procedure pgq.logutriga('udata_que', 'ignore=bin');

insert into udata values (1, 'txt', 'bin');
update udata set txt = 'txt';
update udata set txt = 'txt2', bin = 'bin2';
update udata set bin = 'bin3';
delete from udata;

-- test missing pkey
create table nopkey2 (dat text);
create trigger nopkey_triga2 after insert or update or delete on nopkey2
for each row execute procedure pgq.logutriga('que3');

insert into nopkey2 values ('foo');
update nopkey2 set dat = 'bat';
delete from nopkey2;

-- test custom pkey
create table ucustom_pkey (dat1 text not null, dat2 int2 not null, dat3 text);
create trigger ucustom_triga after insert or update or delete on ucustom_pkey
--for each row execute procedure pgq.logutriga('que3', 'pkey=dat1,dat2');
for each row execute procedure pgq.logutriga('que3');

insert into ucustom_pkey values ('foo', '2');
update ucustom_pkey set dat3 = 'bat';
delete from ucustom_pkey;

-- test custom fields
create table custom_fields2 (
    dat1 text not null primary key,
    dat2 int2 not null,
    dat3 text,
    _pgq_ev_type text default 'my_type',
    _pgq_ev_extra1 text default 'e1',
    _pgq_ev_extra2 text default 'e2',
    _pgq_ev_extra3 text default 'e3',
    _pgq_ev_extra4 text default 'e4'
);
create trigger customf2_triga after insert or update or delete on custom_fields2
for each row execute procedure pgq.logutriga('que3');

insert into custom_fields2 values ('foo', '2');
update custom_fields2 set dat3 = 'bat';
delete from custom_fields2;


-- test custom expression
create table custom_expr2 (
    dat1 text not null primary key,
    dat2 int2 not null,
    dat3 text
);
create trigger customex2_triga after insert or update or delete on custom_expr2
for each row execute procedure pgq.logutriga('que3', 'ev_extra1=''test='' || dat1', 'ev_type=dat3');

insert into custom_expr2 values ('foo', '2');
update custom_expr2 set dat3 = 'bat';
delete from custom_expr2;




drop function pgq.insert_event(text, text, text, text, text, text, text);
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


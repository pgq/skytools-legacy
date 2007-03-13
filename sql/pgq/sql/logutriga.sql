
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



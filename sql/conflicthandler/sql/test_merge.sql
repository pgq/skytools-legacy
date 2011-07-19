
\set ECHO none
\i merge_on_time.sql
\set ECHO all

set DateStyle='ISO';

create table mergetest (
    intcol int4,
    txtcol text,
    timecol timestamp
);

-- insert to empty
select merge_on_time('timefield=timecol', null, null, null, null, null, 'I:intcol', 'intcol=5&txtcol=v1&timecol=2010-09-09+12:12', 'mergetest', null, null, null);
select * from mergetest;

-- insert to with time earlier
select merge_on_time('timefield=timecol', null, null, null, null, null, 'I:intcol', 'intcol=5&txtcol=v2&timecol=2010-09-08+12:12', 'mergetest', null, null, null);
select * from mergetest;

-- insert to with time later
select merge_on_time('timefield=timecol', null, null, null, null, null, 'I:intcol', 'intcol=5&txtcol=v3&timecol=2010-09-10+12:12', 'mergetest', null, null, null);
select * from mergetest;


-- ----------
-- txid.sql
--
--	SQL script for loading the transaction ID compatible datatype 
--
--	Copyright (c) 2003-2004, PostgreSQL Global Development Group
--	Author: Jan Wieck, Afilias USA INC.
--
-- ----------

--
-- now the epoch storage
--

CREATE SCHEMA txid;

-- remember txid settings
-- use bigint so we can do arithmetic with it
create table txid.epoch (
	epoch bigint,
	last_value bigint
);

-- make sure there exist exactly one row
insert into txid.epoch values (0, 1);


-- then protect it
create function txid.epoch_guard()
returns trigger as $$
begin
    if TG_OP = 'UPDATE' then
	-- epoch: allow only small increase
	if NEW.epoch > OLD.epoch and NEW.epoch < (OLD.epoch + 3) then
	    return NEW;
	end if;
	-- last_value: allow only increase
	if NEW.epoch = OLD.epoch and NEW.last_value > OLD.last_value then
	    return NEW;
	end if;
    end if;
    raise exception 'bad operation on txid.epoch';
end;
$$ language plpgsql;

-- the trigger
create trigger epoch_guard_trigger
before insert or update or delete on txid.epoch
for each row execute procedure txid.epoch_guard();


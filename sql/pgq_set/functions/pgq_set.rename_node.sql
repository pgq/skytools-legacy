
create or replace function pgq_set.rename_node_step1(
    in i_set_name text,
    in i_node_name_old text,
    in i_node_name_new text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.rename_node_step1(3)
--
--      Rename a node - step1.
--
-- Parameters:
--      i_set_name - set name
--      i_node_name_old - node name
--      i_node_name_new - node connect string
--
-- Returns:
--      ret_code - error code
--      ret_note - error description
--
-- Return Codes:
--      200 - Ok
--      404 - No such set
-- ----------------------------------------------------------------------
declare
    n  record;
    reg record;
begin
    select s.node_name, s.node_type, s.paused, s.uptodate, s.queue_name
      into n from pgq_set.set_info s
     where s.set_name = i_set_name for update;
    if not found then
        select 404, 'Unknown set: ' || i_set_name into ret_code, ret_note;
        return;
    end if;

    -- make copy of member info
    perform 1 from pgq_set.member_info
      where set_name = i_set_name
        and node_name = i_node_name_new;
    if not found then
        insert into pgq_set.member_info
              (set_name, node_name, node_location, dead)
        select set_name, i_node_name_new, node_location, dead
          from pgq_set.member_info
         where set_name = i_set_name
           and node_name = i_node_name_old;
    end if;

    -- make copy of subscriber info
    perform 1 from pgq_set.subscriber_info
      where set_name = i_set_name
        and node_name = i_node_name_new;
    if not found then
        insert into pgq_set.subscriber_info
              (set_name, node_name, local_watermark)
        select set_name, i_node_name_new, local_watermark
          from pgq_set.subscriber_info
         where set_name = i_set_name
           and node_name = i_node_name_old;
    end if;

    if n.queue_name is not null then
        select f.last_tick into reg
          from pgq.get_consumer_info(n.queue_name, i_node_name_old) f;
        if found then
            perform 1 from pgq.get_consumer_info(n.queue_name, i_node_name_new);
            if not found then
                perform pgq.register_consumer_at(n.queue_name, i_node_name_new, reg.last_tick);
            end if;
        end if;
    end if;

    -- FIXME: on root insert event about new node

    select 200, 'Ok' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;


create or replace function pgq_set.rename_node_step2(
    in i_set_name text,
    in i_node_name_old text,
    in i_node_name_new text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.rename_node_step2(3)
--
--      Rename a node - step2.
--
-- Parameters:
--      i_set_name - set name
--      i_node_name_old - node name
--      i_node_name_new - node connect string
--
-- Returns:
--      ret_code - error code
--      ret_note - error description
--
-- Return Codes:
--      200 - Ok
--      404 - No such set
-- ----------------------------------------------------------------------
declare
    n  record;
    det record;
    reg record;
begin
    select s.node_name, s.node_type, s.paused, s.uptodate, s.queue_name, s.provider_node
      into n from pgq_set.set_info s
     where s.set_name = i_set_name for update;
    if not found then
        select 404, 'Unknown set: ' || i_set_name into ret_code, ret_note;
        return;
    end if;

    if n.node_name = i_node_name_old then
        if not n.paused or not n.uptodate then
            select 401, 'Bad node state during rename' into ret_code, ret_note;
            return;
        end if;
        update pgq_set.set_info
           set node_name = i_node_name_new,
               uptodate = false
         where set_name = i_set_name;
    elsif n.provider_node = i_node_name_old then
        update pgq_set.set_into
           set provider_node = i_node_name_new,
               uptodate = false
         where set_name = i_set_name;
    end if;

    -- delete old copy of subscriber info
    select into det
      (select count(1) from pgq_set.subscriber_info
        where set_name = i_set_name
          and node_name = i_node_name_old) as got_old,
      (select count(1) from pgq_set.subscriber_info
        where set_name = i_set_name
          and node_name = i_node_name_new) as got_new;
    if det.got_old > 0 and det.got_new > 0 then
        delete from pgq_set.subscriber_info
         where set_name = i_set_name
           and node_name = i_node_name_old;
    elsif det.got_old > 0 then
        select 401, 'got old subscriber but not new' into ret_code, ret_note;
        return;
    end if;
    
    -- delete old copy of subscriber info
    select into det
      (select count(1) from pgq_set.member_info
        where set_name = i_set_name
          and node_name = i_node_name_old) > 0 as got_old,
      (select count(1) from pgq_set.member_info
        where set_name = i_set_name
          and node_name = i_node_name_new) > 0 as got_new;
    if det.got_old and det.got_new then
        delete from pgq_set.member_info
         where set_name = i_set_name
           and node_name = i_node_name_old;
    elsif det.got_old then
        select 401, 'got old member but not new' into ret_code, ret_note;
        return;
    end if;
    
    if n.queue_name is not null then
        select f.last_tick into reg
          from pgq.get_consumer_info(n.queue_name, i_node_name_old) f;
        if found then
            perform 1 from pgq.get_consumer_info(n.queue_name, i_node_name_new);
            if not found then
                perform pgq.register_consumer_at(n.queue_name, i_node_name_new, reg.last_tick);
            end if;
        end if;
    end if;

    -- FIXME: on parent remove old registration
    -- FIXME: on root insert event about old node delete

    select 200, 'Ok' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;


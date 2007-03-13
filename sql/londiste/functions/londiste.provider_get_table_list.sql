
create or replace function londiste.provider_get_table_list(i_queue text)
returns setof londiste.ret_provider_table_list as $$ 
declare 
    rec   londiste.ret_provider_table_list%rowtype; 
begin 
    for rec in 
        select table_name, trigger_name
            from londiste.provider_table
            where queue_name = i_queue
            order by nr
    loop
        return next rec;
    end loop; 
    return;
end; 
$$ language plpgsql security definer;


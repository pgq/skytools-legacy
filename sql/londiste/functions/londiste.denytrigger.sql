
create or replace function londiste.deny_trigger()
returns trigger as $$
    if 'undeny' in GD:
        return 'OK'
    plpy.error('Changes no allowed on this table')
$$ language plpythonu;

create or replace function londiste.disable_deny_trigger(i_allow boolean)
returns boolean as $$
    if args[0]:
        GD['undeny'] = 1
        return True
    else:
        if 'undeny' in GD:
            del GD['undeny']
        return False
$$ language plpythonu;


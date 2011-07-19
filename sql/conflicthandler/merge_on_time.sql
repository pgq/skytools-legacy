create or replace function merge_on_time(
    fn_conf text,
    cur_tick text,
    ev_id text,
    ev_time text,
    ev_txid text,
    ev_retry text,
    ev_type text,
    ev_data text,
    ev_extra1 text,
    ev_extra2 text,
    ev_extra3 text,
    ev_extra4 text)
returns text as $$
# callback function for londiste applyfn handler
try:
    import pkgloader
    pkgloader.require('skytools', '3.0')
    from skytools.plpy_applyrow import ts_conflict_handler
    args = [fn_conf, ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4]
    return ts_conflict_handler(SD, args)
except:
    import traceback
    for ln in traceback.format_exc().split('\n'):
        if ln:
            plpy.warning(ln)
    raise

$$ language plpythonu;

-- select merge_on_time('timefield=modified_date', 'I:id_ccard', 'key_user=foo&id_ccard=1&modified_date=2005-01-01', 'ccdb.ccard', '', '', '');